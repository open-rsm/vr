package vr

import (
	"context"
	"testing"
	"github.com/open-rsm/vr/proto"
)

func Benchmark(tb *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := newBus()
	s := NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             s,
		AppliedNum:        0,
	})
	go b.cycle(vr)
	defer b.Stop()
	b.Change(ctx)
	for i := 0; i < tb.N; i++ {
		f := <-b.Tuple()
		s.Append(f.PersistentEntries)
		b.Advance()
		b.Call(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Data: []byte("testdata")}}})
	}
	f := <-b.Tuple()
	if f.HardState.CommitNum != uint64(tb.N+1) {
		tb.Errorf("commit-number = %d, expected %d", f.HardState.CommitNum, tb.N+1)
	}
}
