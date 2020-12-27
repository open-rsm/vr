package vr

import (
	"log"
	"sync"
	"errors"
	"github.com/open-rsm/spec/proto"
)

var ErrNotReached = errors.New("access entry at op-number is not reached")
var ErrArchived = errors.New("access op-number is not reached due to archive")
var ErrOutOfBounds = errors.New("out of bounds")
var ErrUnavailable = errors.New("requested entry at op-number is unavailable")

// storage models of view stamped replication
type Store struct {
	sync.Mutex
	hardState    proto.HardState     //
	appliedState proto.AppliedState  //
	entries      []proto.Entry       //
}

func NewStore() *Store {
	return &Store{
		entries: make([]proto.Entry, 1),
	}
}

func (s *Store) SetHardState(hs proto.HardState) error {
	s.hardState = hs
	return nil
}

func (s *Store) SetAppliedState(as proto.AppliedState) error {
	s.Lock()
	defer s.Unlock()
	s.appliedState = as
	s.entries = []proto.Entry{{ViewNum: as.Applied.ViewNum, OpNum: as.Applied.OpNum}}
	return nil
}

func (s *Store) LoadState() (proto.HardState, proto.ConfigurationState, error) {
	return s.hardState, proto.ConfigurationState{}, nil
}

func (s *Store) GetAppliedState() (proto.AppliedState, error) {
	s.Lock()
	defer s.Unlock()
	return s.appliedState, nil
}

func (s *Store) Seek(low, up uint64) ([]proto.Entry, error) {
	s.Lock()
	defer s.Unlock()
	offset := s.entries[0].OpNum
	if low <= offset {
		return nil, ErrArchived
	}
	if up > s.lastOpNum()+1 {
		log.Panicf("entries up(%d) is out of bound last-op-number(%d)", up, s.lastOpNum())
	}
	if len(s.entries) == 1 {
		return nil, ErrNotReached
	}
	return s.entries[low-offset:up-offset], nil
}

func (s *Store) ViewNum(num uint64) (uint64, error) {
	s.Lock()
	defer s.Unlock()
	offset := s.entries[0].OpNum
	if num < offset {
		return 0, ErrArchived
	}
	if int(num-offset) >= len(s.entries) {
		return 0, ErrUnavailable
	}
	return s.entries[num-offset].ViewNum, nil
}

func (s *Store) CommitNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.hardState.CommitNum, nil
}

func (s *Store) StartOpNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.startOpNum() + 1, nil
}

func (s *Store) startOpNum() uint64 {
	return s.entries[0].OpNum
}

func (s *Store) LastOpNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.lastOpNum() - 1, nil
}

func (s *Store) lastOpNum() uint64 {
	return s.entries[0].OpNum + uint64(len(s.entries))
}

func (s *Store) CreateAppliedState(num uint64, data []byte, rs *proto.ConfigurationState) (proto.AppliedState, error) {
	s.Lock()
	defer s.Unlock()
	if num < s.appliedState.Applied.OpNum {
		return proto.AppliedState{}, ErrOutOfBounds
	}
	if num > s.lastOpNum() {
		log.Panicf("vr.stores: appliedNum state %d is out of bound last op-number(%d)", num, s.lastOpNum())
	}
	s.appliedState.Applied.OpNum = num
	s.appliedState.Applied.ViewNum = s.entries[num-s.startOpNum()].ViewNum
	s.appliedState.Data = data
	if rs != nil {
		s.appliedState.Applied.ConfigurationState = *rs
	}
	return s.appliedState, nil
}

func (s *Store) Archive(archiveNum uint64) error {
	s.Lock()
	defer s.Unlock()
	offset := s.startOpNum()
	if archiveNum <= offset {
		return ErrArchived
	}
	if archiveNum >= s.lastOpNum() {
		log.Panicf("vr.stores: archive %d is out of bound last op-number(%d)", archiveNum, offset+uint64(len(s.entries))-1)
	}
	num := archiveNum - offset
	entries := make([]proto.Entry, 1, 1+uint64(len(s.entries))-num)
	entries[0].OpNum = s.entries[num].OpNum
	entries[0].ViewNum = s.entries[num].ViewNum
	entries = append(entries, s.entries[num+1:]...)
	s.entries = entries
	return nil
}

func (s *Store) Append(entries []proto.Entry) error {
	s.Lock()
	defer s.Unlock()
	if len(entries) == 0 {
		return nil
	}
	start := s.startOpNum()
	last := s.lastOpNum()
	if last < start {
		return nil
	}
	if start > entries[0].OpNum {
		entries = entries[start-entries[0].OpNum:]
	}
	offset := entries[0].OpNum - s.startOpNum()
	if uint64(len(s.entries)) > offset {
		s.entries = append([]proto.Entry{}, s.entries[:offset]...)
		s.entries = append(s.entries, entries...)

	} else if uint64(len(s.entries)) == offset {
		s.entries = append(s.entries, entries...)
	} else {
		log.Panicf("vr.stores: missing oplog entry [last: %d, append at: %d]",
			s.appliedState.Applied.OpNum+uint64(len(s.entries)), entries[0].OpNum)
	}
	return nil
}
