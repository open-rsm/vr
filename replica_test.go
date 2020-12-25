package vr

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/open-rsm/spec/proto"
)

func TestReplicaCall(t *testing.T) {
	for i, mtn := range proto.MessageType_name {
		r := &replica{
			requestC: make(chan proto.Message, 1),
			receiveC: make(chan proto.Message, 1),
		}
		mt := proto.MessageType(i)
		r.Step(context.TODO(), proto.Message{Type: mt})
		if mt == proto.Request {
			select {
			case <-r.requestC:
			default:
				t.Errorf("%d: cannot receive %s on request chan", mt, mtn)
			}
		} else {
			if mt == proto.Heartbeat || mt == proto.Change {
				select {
				case <-r.receiveC:
					t.Errorf("%d: step should ignore %s", mt, mtn)
				default:
				}
			} else {
				select {
				case <-r.receiveC:
				default:
					t.Errorf("%d: cannot receive %s on receive chan", mt, mtn)
				}
			}
		}
	}
}

func TestReplicaCallNonblocking(t *testing.T) {
	r := &replica{
		requestC: make(chan proto.Message),
		doneC:    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopFunc := func() {
		close(r.doneC)
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
			err := r.Step(ctx, proto.Message{Type: proto.Request})
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
			case <-r.doneC:
				r.doneC = make(chan struct{})
			default:
			}
		case <-time.After(time.Millisecond * 100):
			t.Errorf("#%d: failed to unblock call", i)
		}
	}
}

func TestReplicaRequest(t *testing.T) {
	msgs := []proto.Message{}
	appendCall := func(r *VR, m proto.Message) {
		msgs = append(msgs, m)
	}
	r := newReplica()
	store := NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             store,
		AppliedNum:        0,
	})
	go r.run(vr)
	r.Change(context.TODO())
	for {
		f := <-r.Ready()
		store.Append(f.PersistentEntries)
		if f.SoftState.Prim == vr.num {
			vr.callFn = appendCall
			r.Advance()
			break
		}
		r.Advance()
	}
	r.Step(context.TODO(), proto.Message{
		Type:    proto.Request,
		Entries: []proto.Entry{{Data: []byte("testdata")}}},
	)
	r.Stop()
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

func TestReplicaClock(t *testing.T) {
	r := newReplica()
	bs := NewStore()
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             bs,
		AppliedNum:        0,
	})
	go r.run(vr)
	pulse := vr.pulse
	r.Clock()
	r.Stop()
	if vr.pulse != pulse+1 {
		t.Errorf("pulse = %d, expected %d", vr.pulse, pulse+1)
	}
}

func TestReplicaStop(t *testing.T) {
	r := newReplica()
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
		r.run(vr)
		close(doneC)
	}()

	pulse := vr.pulse
	r.Clock()
	r.Stop()

	select {
	case <-doneC:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for node to stop!")
	}
	if vr.pulse != pulse+1 {
		t.Errorf("pulse = %d, expected %d", vr.pulse, pulse+1)
	}
	r.Clock()
	if vr.pulse != pulse+1 {
		t.Errorf("pulse = %d, expected %d", vr.pulse, pulse+1)
	}
	r.Stop()
}

func TestReadyPreCheck(t *testing.T) {
	cases := []struct {
		f     Ready
		check bool
	}{
		{Ready{}, false},
		{Ready{SoftState: &SoftState{Prim: 1}}, true},
		{Ready{PersistentEntries: make([]proto.Entry, 1, 1)}, true},
		{Ready{ApplicableEntries: make([]proto.Entry, 1, 1)}, true},
		{Ready{Messages: make([]proto.Message, 1, 1)}, true},
	}
	for i, test := range cases {
		if rv := test.f.PreCheck(); rv != test.check {
			t.Errorf("#%d: precheck = %v, expected %v", i, rv, test.check)
		}
	}
}

func TestReplicaRestart(t *testing.T) {
	entries := []proto.Entry{
		{ViewNum: 1, OpNum: 1},
		{ViewNum: 1, OpNum: 2, Data: []byte("foo")},
	}
	hs := proto.HardState{ViewNum: 1, CommitNum: 1}
	f := Ready{
		HardState:         hs,
		ApplicableEntries: entries[:hs.CommitNum],
	}

	bs := NewStore()
	bs.SetHardState(hs)
	bs.Append(entries)
	n := RestartReplica(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             bs,
		AppliedNum:        0,
	})
	if g := <-n.Ready(); !reflect.DeepEqual(g, f) {
		t.Errorf("g = %+v,\n f %+v", g, f)
	}
	n.Advance()
	select {
	case f := <-n.Ready():
		t.Errorf("unexpecteded Ready: %+v", f)
	case <-time.After(time.Millisecond):
	}
}

func TestReplicaAdvance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bs := NewStore()
	r := StartReplica(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             bs,
		AppliedNum:        0,
	},
		[]Peer{{ID: 1}},
	)
	r.Change(ctx)
	<-r.Ready()
	r.Step(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Data: []byte("foo")}}})
	var f Ready
	select {
	case f = <-r.Ready():
		t.Fatalf("unexpecteded ready before advance: %+v", f)
	case <-time.After(time.Millisecond):
	}
	bs.Append(f.PersistentEntries)
	r.Advance()
	select {
	case <-r.Ready():
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
		{&SoftState{VRRole: Primary}, false},
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
		{proto.HardState{ViewNum: 1}, false},
	}
	for i, test := range cases {
		if IsHardStateEqual(test.hs, nilHardState) != test.expEqual {
			t.Errorf("#%d, equal = %v, expected %v", i, IsHardStateEqual(test.hs, nilHardState), test.expEqual)
		}
	}
}
