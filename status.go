package vr

import (
	"log"
	"encoding/json"
	"github.com/open-rsm/spec/proto"
)

type Status struct {
	SoftState
	proto.HardState

	ID      uint64
	Applied uint64
	Windows map[uint64]Window
}

func getStatus(vr *VR) Status {
	s := Status{ID: vr.replicaNum}
	s.HardState = vr.HardState
	s.SoftState = *vr.softState()
	s.Applied = vr.opLog.appliedNum
	if s.VRRole == Primary {
		s.Windows = make(map[uint64]Window)
		for id, window := range vr.windows {
			s.Windows[id] = *window
		}
	}
	return s
}

func (s Status) String() string {
	b, err := json.Marshal(&s)
	if err != nil {
		log.Panicf("unexpected error: %v", err)
	}
	return string(b)
}
