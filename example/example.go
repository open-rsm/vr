package example

import (
	"github.com/open-rsm/vr"
	"github.com/open-rsm/spec/proto"
)

func applyToStore([]proto.Entry)      {}
func sendMessages([]proto.Message)     {}
func saveStateToDisk(proto.HardState)  {}
func saveEntriesToDisk([]proto.Entry) {}

func ExampleReplicator() {
	replica := vr.StartReplica(&vr.Config{
		Num:               1,
		Peers:             nil,
		TransitionTimeout: 0,
		HeartbeatTimeout:  0,
		Store:             nil,
		AppliedNum:        0,
	},
		nil,
	)
	var prev proto.HardState
	for {
		rd := <-replica.Ready()
		if !vr.IsHardStateEqual(prev, rd.HardState) {
			saveStateToDisk(rd.HardState)
			prev = rd.HardState
		}
		saveEntriesToDisk(rd.PersistentEntries)
		go applyToStore(rd.ApplicableEntries)
		sendMessages(rd.Messages)
	}
}
