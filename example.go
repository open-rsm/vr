package vr

import "github.com/open-rsm/vr/proto"

func applyToStore([]proto.Entry)      {}
func sendMessages([]proto.Message)     {}
func saveStateToDisk(proto.HardState)  {}
func saveEntriesToDisk([]proto.Entry) {}

func ExampleReplicator() {
	replicator := StartReplica(&Config{
		Num:               1,
		Peers:             nil,
		TransitionTimeout: 0,
		HeartbeatTimeout:  0,
		Store:             nil,
		AppliedNum:        0,
	})
	var prev proto.HardState
	for {
		tp := <-replicator.Tuple()
		if !IsHardStateEqual(prev, tp.HardState) {
			saveStateToDisk(tp.HardState)
			prev = tp.HardState
		}
		saveEntriesToDisk(tp.PersistentEntries)
		go applyToStore(tp.ApplicableEntries)
		sendMessages(tp.Messages)
	}
}
