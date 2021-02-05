package vr

import (
	"context"
	"testing"
	"github.com/open-rsm/vr/proto"
)

func Benchmark(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newBus()
	s := NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             s,
		AppliedNum:        0,
	})
	go r.cycle(vr)
	defer r.Stop()
	r.Change(ctx)
	for i := 0; i < b.N; i++ {
		f := <-r.Tuple()
		s.Append(f.PersistentEntries)
		r.Advance()
		r.Call(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Data: []byte("testdata")}}})
	}
	f := <-r.Tuple()
	if f.HardState.CommitNum != uint64(b.N+1) {
		b.Errorf("commit-number = %d, expected %d", f.HardState.CommitNum, b.N+1)
	}
}
