package vr

import (
	"context"
	"testing"
	"github.com/open-rsm/spec/proto"
)

func BenchmarkSingleReplica(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newReplica()
	s := NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             s,
		AppliedNum:        0,
	})
	go r.run(vr)
	defer r.Stop()
	r.Change(ctx)
	for i := 0; i < b.N; i++ {
		f := <-r.Ready()
		s.Append(f.PersistentEntries)
		r.Advance()
		r.Step(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Data: []byte("testdata")}}})
	}
	f := <-r.Ready()
	if f.HardState.CommitNum != uint64(b.N+1) {
		b.Errorf("commit-number = %d, expected %d", f.HardState.CommitNum, b.N+1)
	}
}
