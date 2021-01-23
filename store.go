package vr

import (
	"log"
	"sync"
	"errors"
	"github.com/open-rsm/vr/proto"
)

var ErrNotReached = errors.New("vr.store: access entry at op-number is not reached")
var ErrArchived = errors.New("vr.store: access op-number is not reached due to archive")
var ErrOverflow = errors.New("vr.store: overflow")
var ErrUnavailable = errors.New("vr.store: requested entry at op-number is unavailable")

// storage models of view stamped replication
type Store struct {
	sync.Mutex
	hardState    proto.HardState     // persistent state that has been stored to disk
	appliedState proto.AppliedState  // the state that has been used by the application layer
	entries      []proto.Entry       // used to manage newly added log entries
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
	s.entries = []proto.Entry{{ViewStamp: as.Applied.ViewStamp}}
	return nil
}

func (s *Store) LoadConfigurationState() (proto.ConfigurationState, error) {
	return proto.ConfigurationState{}, nil
}

func (s *Store) LoadHardState() (proto.HardState, error) {
	return s.hardState, nil
}

func (s *Store) GetAppliedState() (proto.AppliedState, error) {
	s.Lock()
	defer s.Unlock()
	return s.appliedState, nil
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
	if start > entries[0].ViewStamp.OpNum {
		entries = entries[start-entries[0].ViewStamp.OpNum:]
	}
	offset := entries[0].ViewStamp.OpNum - s.startOpNum()
	if uint64(len(s.entries)) > offset {
		s.entries = append([]proto.Entry{}, s.entries[:offset]...)
		s.entries = append(s.entries, entries...)
	} else if uint64(len(s.entries)) == offset {
		s.entries = append(s.entries, entries...)
	} else {
		log.Panicf("vr.store: not found oplog entry [last: %d, append at: %d]",
			s.appliedState.Applied.ViewStamp.OpNum+uint64(len(s.entries)), entries[0].ViewStamp.OpNum)
	}
	return nil
}

func (s *Store) Subset(low, up uint64) ([]proto.Entry, error) {
	s.Lock()
	defer s.Unlock()
	offset := s.entries[0].ViewStamp.OpNum
	if low <= offset {
		return nil, ErrArchived
	}
	if up > s.lastOpNum()+1 {
		log.Panicf("vr.store: entries up(%d) is overflow last-op-number(%d)", up, s.lastOpNum())
	}
	if len(s.entries) == 1 {
		return nil, ErrNotReached
	}
	return s.entries[low-offset:up-offset], nil
}

func (s *Store) ViewNum(num uint64) (uint64, error) {
	s.Lock()
	defer s.Unlock()
	vo := s.entries[0].ViewStamp.OpNum
	if num < vo {
		return 0, ErrArchived
	}
	if int(num-vo) >= len(s.entries) {
		return 0, ErrUnavailable
	}
	return s.entries[num-vo].ViewStamp.ViewNum, nil
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
	return s.entries[0].ViewStamp.OpNum
}

func (s *Store) LastOpNum() (uint64, error) {
	s.Lock()
	defer s.Unlock()
	return s.lastOpNum() - 1, nil
}

func (s *Store) lastOpNum() uint64 {
	return s.entries[0].ViewStamp.OpNum + uint64(len(s.entries))
}

func (s *Store) CreateAppliedState(num uint64, data []byte, rs *proto.ConfigurationState) (proto.AppliedState, error) {
	s.Lock()
	defer s.Unlock()
	if num < s.appliedState.Applied.ViewStamp.OpNum {
		return proto.AppliedState{}, ErrOverflow
	}
	if num > s.lastOpNum() {
		log.Panicf("vr.store: applied-number state %d is overflow last op-number(%d)", num, s.lastOpNum())
	}
	s.appliedState.Applied.ViewStamp.OpNum = num
	s.appliedState.Applied.ViewStamp.ViewNum = s.entries[num-s.startOpNum()].ViewStamp.ViewNum
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
		log.Panicf("vr.store: archive %d is overflow last op-number(%d)", archiveNum, offset+uint64(len(s.entries))-1)
	}
	num := archiveNum - offset
	entries := make([]proto.Entry, 1, 1+uint64(len(s.entries))-num)
	entries[0].ViewStamp.OpNum = s.entries[num].ViewStamp.OpNum
	entries[0].ViewStamp.ViewNum = s.entries[num].ViewStamp.ViewNum
	entries = append(entries, s.entries[num+1:]...)
	s.entries = entries
	return nil
}