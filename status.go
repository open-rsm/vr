package vr

import (
	"log"
	"encoding/json"
	"github.com/open-rsm/vr/proto"
)

// provide the current status of the replication group
type Status struct {
	// temporary status syncing
	SoftState
	// persistent state
	proto.HardState

	Num        uint64            // current replica number
	CommitNum  uint64            // logs that have been committed
	AppliedNum uint64            // track the status that has been applied
	//Windows    map[uint64]window // peer's window control information, subject to primary
}

// build and package status
func getStatus(vr *VR) Status {
	s := Status{Num: vr.replicaNum}
	s.HardState = vr.HardState
	s.SoftState = *vr.softState()
	s.AppliedNum = vr.opLog.appliedNum
	if s.Role == Primary {
		/*
		s.Windows = make(map[uint64]window)
		for num, window := range vr.replicas {
			s.Windows[num] = *window
		}
		*/
	}
	return s
}

// serialized status data
func (s Status) String() string {
	b, err := json.Marshal(&s)
	if err != nil {
		log.Panicf("unexpected error: %v", err)
	}
	return string(b)
}
