package vr

import (
	"time"
	"context"
	"reflect"
	"testing"

	"github.com/open-rsm/vr/proto"
)

func TestBusCall(t *testing.T) {
	for i, mtn := range proto.MessageType_name {
		b := &bus{
			requestC: make(chan proto.Message, 1),
			receiveC: make(chan proto.Message, 1),
		}
		mt := proto.MessageType(i)
		b.Call(context.TODO(), proto.Message{Type: mt})
		if mt == proto.Request {
			select {
			case <-b.requestC:
			default:
				t.Errorf("%d: cannot receive %s on request chan", mt, mtn)
			}
		} else {
			if mt == proto.Heartbeat || mt == proto.Change {
				select {
				case <-b.receiveC:
					t.Errorf("%d: step should ignore %s", mt, mtn)
				default:
				}
			} else {
				select {
				case <-b.receiveC:
				default:
					t.Errorf("%d: cannot receive %s on receive chan", mt, mtn)
				}
			}
		}
	}
}

func TestBusCallNonblocking(t *testing.T) {
	b := &bus{
		requestC: make(chan proto.Message),
		doneC:    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopFunc := func() {
		close(b.doneC)
	}
	cases := []struct {
		unblock func()
		err     error
	}{
		{stopFunc, ErrStopped},
		{cancel, context.Canceled},
	}
	for i, test := range cases {
		errC := make(chan error, 1)
		go func() {
			err := b.Call(ctx, proto.Message{Type: proto.Request})
			errC <- err
		}()
		test.unblock()
		select {
		case err := <-errC:
			if err != test.err {
				t.Errorf("#%d: err = %v, expected %v", i, err, test.err)
			}
			if ctx.Err() != nil {
				ctx = context.TODO()
			}
			select {
			case <-b.doneC:
				b.doneC = make(chan struct{})
			default:
			}
		case <-time.After(time.Millisecond * 100):
			t.Errorf("#%d: failed to unblock call", i)
		}
	}
}

func TestBusRequest(t *testing.T) {
	msgs := []proto.Message{}
	appendCall := func(r *VR, m proto.Message) {
		msgs = append(msgs, m)
	}
	b := newBus()
	store := NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             store,
		AppliedNum:        0,
	})
	go b.cycle(vr)
	b.Change(context.TODO())
	for {
		f := <-b.Tuple()
		store.Append(f.PersistentEntries)
		if f.SoftState.Prim == vr.replicaNum {
			vr.call = appendCall
			b.Advance()
			break
		}
		b.Advance()
	}
	b.Call(context.TODO(), proto.Message{
		Type:    proto.Request,
		Entries: []proto.Entry{{Data: []byte("testdata")}}},
	)
	b.Stop()
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d, expected %d", len(msgs), 1)
	}
	if msgs[0].Type != proto.Request {
		t.Errorf("msg type = %d, expected %d", msgs[0].Type, proto.Request)
	}
	if !reflect.DeepEqual(msgs[0].Entries[0].Data, []byte("testdata")) {
		t.Errorf("data = %v, expected %v", msgs[0].Entries[0].Data, []byte("testdata"))
	}
}

func TestBusClock(t *testing.T) {
	b := newBus()
	bs := NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             bs,
		AppliedNum:        0,
	})
	go b.cycle(vr)
	pulse := vr.pulse
	b.Clock()
	b.Stop()
	if vr.pulse != pulse+1 {
		t.Errorf("pulse = %d, expected %d", vr.pulse, pulse+1)
	}
}

func TestBusStop(t *testing.T) {
	b := newBus()
	bs := NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             bs,
		AppliedNum:        0,
	})
	doneC := make(chan struct{})

	go func() {
		b.cycle(vr)
		close(doneC)
	}()

	pulse := vr.pulse
	b.Clock()
	b.Stop()

	select {
	case <-doneC:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for node to stop!")
	}
	if vr.pulse != pulse+1 {
		t.Errorf("pulse = %d, expected %d", vr.pulse, pulse+1)
	}
	b.Clock()
	if vr.pulse != pulse+1 {
		t.Errorf("pulse = %d, expected %d", vr.pulse, pulse+1)
	}
	b.Stop()
}

func TestReadyPreCheck(t *testing.T) {
	cases := []struct {
		f     Tuple
		check bool
	}{
		{Tuple{}, false},
		{Tuple{SoftState: &SoftState{Prim: 1}}, true},
		{Tuple{PersistentEntries: make([]proto.Entry, 1, 1)}, true},
		{Tuple{ApplicableEntries: make([]proto.Entry, 1, 1)}, true},
		{Tuple{Messages: make([]proto.Message, 1, 1)}, true},
	}
	for i, test := range cases {
		if rv := test.f.PreCheck(); rv != test.check {
			t.Errorf("#%d: precheck = %v, expected %v", i, rv, test.check)
		}
	}
}

func TestBusRestart(t *testing.T) {
	entries := []proto.Entry{
		{ViewStamp: proto.ViewStamp{ViewNum: 1, OpNum: 1}},
		{ViewStamp: proto.ViewStamp{ViewNum: 1, OpNum: 2}, Data: []byte("foo")},
	}
	hs := proto.HardState{ViewStamp: proto.ViewStamp{ViewNum: 1}, CommitNum: 1}
	f := Tuple{
		HardState:         hs,
		ApplicableEntries: entries[:hs.CommitNum],
	}
	s := NewStore()
	s.SetHardState(hs)
	s.Append(entries)
	n := Restart(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             s,
		AppliedNum:        0,
	})
	if g := <-n.Tuple(); !reflect.DeepEqual(g, f) {
		t.Errorf("g = %+v,\n f %+v", g, f)
	}
	n.Advance()
	select {
	case f := <-n.Tuple():
		t.Errorf("unexpecteded Tuple: %+v", f)
	case <-time.After(time.Millisecond):
	}
}

func TestBusAdvance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewStore()
	r := Start(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             s,
		AppliedNum:        0,
	})
	r.Change(ctx)
	<-r.Tuple()
	r.Call(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Data: []byte("foo")}}})
	var f Tuple
	select {
	case f = <-r.Tuple():
		t.Fatalf("unexpecteded ready before advance: %+v", f)
	case <-time.After(time.Millisecond):
	}
	s.Append(f.PersistentEntries)
	r.Advance()
	select {
	case <-r.Tuple():
	case <-time.After(time.Millisecond):
		t.Errorf("expected ready after advance, but there is no ready available")
	}
}

func TestSoftStateEqual(t *testing.T) {
	cases := []struct {
		ss       *SoftState
		expEqual bool
	}{
		{&SoftState{}, true},
		{&SoftState{Prim: 1}, false},
		{&SoftState{Role: Primary}, false},
	}
	for i, test := range cases {
		if rv := test.ss.equal(&SoftState{}); rv != test.expEqual {
			t.Errorf("#%d, equal = %v, expected %v", i, rv, test.expEqual)
		}
	}
}

func TestIsHardStateEqual(t *testing.T) {
	cases := []struct {
		hs       proto.HardState
		expEqual bool
	}{
		{nilHardState, true},
		{proto.HardState{CommitNum: 1}, false},
		{proto.HardState{ViewStamp: proto.ViewStamp{ViewNum: 1}}, false},
	}
	for i, test := range cases {
		if IsHardStateEqual(test.hs, nilHardState) != test.expEqual {
			t.Errorf("#%d, equal = %v, expected %v", i, IsHardStateEqual(test.hs, nilHardState), test.expEqual)
		}
	}
}
