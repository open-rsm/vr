package vr

import (
	"math"
	"bytes"
	"reflect"
	"testing"
	"github.com/open-rsm/vr/proto"
)

const (
	replicaA = iota + 1
	replicaB
	replicaC
	replicaD
	replicaE
)

var routes = [][]uint64{
	{replicaA, replicaC}, {replicaA, replicaD}, {replicaA, replicaE},
}

func changeMessage(from, to uint64) proto.Message {
	return proto.Message{From: from, To: to, Type: proto.Change}
}

func requestMessage(from, to uint64) proto.Message {
	return proto.Message{From: from, To: to, Type: proto.Request, Entries: []proto.Entry{{Data: []byte("testdata")}}}
}

func heartbeatMessage(from, to uint64) proto.Message {
	return proto.Message{From: from, To: to, Type: proto.Heartbeat}
}

func requestMessageEmptyEntries(from, to uint64) proto.Message {
	return proto.Message{From: from, To: to, Type: proto.Request, Entries: []proto.Entry{{}}}
}

var (
	v0o1,v1o1,v1o2,v1o3,v1o4,v1o5,v1o6,v1o7,v2o0,v2o2,v2o4,v2o5,v2o6,v2o7,v2o8,v3o2,v2o1,v2o3,v3o3,
	v3o4,v4o3,v4o4,v4o5,v4o6,v4o1,v4o2 proto.ViewStamp
)

func initViewStampCase()  {
	v0o1 = proto.ViewStamp{ViewNum: 0, OpNum: 1}
	v1o1 = proto.ViewStamp{ViewNum: 1, OpNum: 1}
	v1o2 = proto.ViewStamp{ViewNum: 1, OpNum: 2}
	v1o3 = proto.ViewStamp{ViewNum: 1, OpNum: 3}
	v1o4 = proto.ViewStamp{ViewNum: 1, OpNum: 4}
	v1o5 = proto.ViewStamp{ViewNum: 1, OpNum: 5}
	v1o6 = proto.ViewStamp{ViewNum: 1, OpNum: 6}
	v1o7 = proto.ViewStamp{ViewNum: 1, OpNum: 7}
	v2o0 = proto.ViewStamp{ViewNum: 2, OpNum: 0}
	v2o2 = proto.ViewStamp{ViewNum: 2, OpNum: 2}
	v2o4 = proto.ViewStamp{ViewNum: 2, OpNum: 4}
	v2o5 = proto.ViewStamp{ViewNum: 2, OpNum: 5}
	v2o6 = proto.ViewStamp{ViewNum: 2, OpNum: 6}
	v2o7 = proto.ViewStamp{ViewNum: 2, OpNum: 7}
	v2o8 = proto.ViewStamp{ViewNum: 2, OpNum: 8}
	v3o2 = proto.ViewStamp{ViewNum: 3, OpNum: 2}
	v3o4 = proto.ViewStamp{ViewNum: 3, OpNum: 4}
	v2o1 = proto.ViewStamp{ViewNum: 2, OpNum: 1}
	v2o3 = proto.ViewStamp{ViewNum: 2, OpNum: 3}
	v3o3 = proto.ViewStamp{ViewNum: 3, OpNum: 3}
	v4o3 = proto.ViewStamp{ViewNum: 4, OpNum: 3}
	v4o4 = proto.ViewStamp{ViewNum: 4, OpNum: 4}
	v4o5 = proto.ViewStamp{ViewNum: 4, OpNum: 5}
	v4o6 = proto.ViewStamp{ViewNum: 4, OpNum: 6}
	v4o1 = proto.ViewStamp{ViewNum: 4, OpNum: 1}
	v4o2 = proto.ViewStamp{ViewNum: 4, OpNum: 2}
}

func TestPrimaryChange(t *testing.T) {
	cases := []struct {
		*mock
		role role
	}{
		{newMock(node, node, node), Primary},
		{newMock(node, hole, hole), Replica},
		{newMock(node, hole, hole, node), Replica},
		{newMock(node, node, hole), Primary},
		{newMock(node, hole, hole, node, node), Primary},
		{newMock(entries(1), node, entries(2), entries(1), node), Primary},
	}
	for i, test := range cases {
		test.trigger(changeMessage(replicaA, replicaA))
		peer := test.mock.peers(replicaA)
		if peer.role != test.role {
			t.Errorf("#%d: role = %s, expected %s", i, roleName[peer.role], roleName[test.role])
		}
		if vn := peer.ViewStamp.ViewNum; vn != 1 {
			t.Errorf("#%d: view-number = %d, expected %d", i, vn, 1)
		}
	}
}

func TestSingleReplica(t *testing.T) {
	m := newMock(node)
	m.trigger(changeMessage(replicaA, replicaA))
	peer := m.peers(1)
	if peer.role != Primary {
		t.Errorf("role = %d, expected %d", peer.role, Primary)
	}
}

func TestSingleReplicaCommit(t *testing.T) {
	m := newMock(node)
	m.trigger(changeMessage(replicaA, replicaA))
	m.trigger(requestMessage(replicaA, replicaA))
	m.trigger(requestMessage(replicaA, replicaA))
	vr := m.peers(1)
	if vr.opLog.commitNum != 3 {
		t.Errorf("commit-number = %d, expected %d", vr.opLog.commitNum, 3)
	}
}

func TestLogReplication(t *testing.T) {
	cases := []struct {
		*mock
		messages     []proto.Message
		expCommitNum uint64
	}{
		{
			newMock(node, node, node),
			[]proto.Message{ requestMessage(replicaA, replicaA) },
			2,
		},
		{
			newMock(node, node, node),
			[]proto.Message{
				requestMessage(replicaA, replicaA), changeMessage(replicaA, replicaB),
				requestMessage(replicaA, replicaB),
			},
			4,
		},
	}
	for i, test := range cases {
		test.trigger(changeMessage(replicaA, replicaA))
		for _, m := range test.messages {
			test.trigger(m)
		}
		for j, node := range test.mock.nodes {
			peer := node.(*VR)
			if peer.opLog.commitNum != test.expCommitNum {
				t.Errorf("#%d.%d: commit-number = %d, expected %d", i, j, peer.opLog.commitNum, test.expCommitNum)
			}
			entries := []proto.Entry{}
			for _, e := range safeEntries(peer, test.mock.stores[j]) {
				if e.Data != nil {
					entries = append(entries, e)
				}
			}
			messages := []*proto.Message{}
			for _, m := range test.messages {
				if m.Type == proto.Request {
					messages = append(messages, &m)
				}
			}
			for k, m := range messages {
				if !bytes.Equal(entries[k].Data, m.Entries[0].Data) {
					t.Errorf("#%d.%d: data = %d, expected %d", i, j, entries[k].Data, m.Entries[0].Data)
				}
			}
		}
	}
}

func TestRequest(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		*mock
		success bool
	}{
		{newMock(node, node, node), true},
		{newMock(node, node, hole), true},
		{newMock(node, hole, hole), false},
		{newMock(node, hole, hole, node), false},
		{newMock(node, hole, hole, node, node), true},
	}
	for j, test := range cases {
		send := func(m proto.Message) {
			defer func() {
				if !test.success {
					rc := recover()
					if rc != nil {
						t.Logf("#%d: err: %s", j, rc)
					}
				}
			}()
			test.trigger(m)
		}
		data := []byte("testdata")
		send(changeMessage(replicaA, replicaA))
		send(requestMessage(replicaA, replicaA))
		expectedLog := newOpLog(NewStore())
		if test.success {
			expectedLog = &opLog{
				store: &Store{
					entries: []proto.Entry{{}, {Data: nil, ViewStamp:v1o1}, {ViewStamp:v1o2, Data: data}},
				},
				unsafe:    unsafe{offset: 3},
				commitNum: 2}
		}
		base := stringOpLog(expectedLog)
		for i, node := range test.nodes {
			if peer, ok := node.(*VR); ok {
				l := stringOpLog(peer.opLog)
				if g := diff(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty oplog", i)
			}
		}
		peer := test.mock.peers(1)
		if vn := peer.ViewStamp.ViewNum; vn != 1 {
			t.Errorf("#%d: view-number = %d, expected %d", j, vn, 1)
		}
	}
}

func TestCommit(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		offsets []uint64
		entries []proto.Entry
		viewNum uint64
		exp     uint64
	}{
		{[]uint64{1},[]proto.Entry{{ViewStamp:v1o1}},1,1},
		{[]uint64{1},[]proto.Entry{{ViewStamp:v1o1}},2,0},
		{[]uint64{2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},2,2},
		{[]uint64{1},[]proto.Entry{{ViewStamp:v2o1}},2,1},

		{[]uint64{2, 1, 1},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},1,1},
		{[]uint64{2, 1, 1},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},
		{[]uint64{2, 1, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},2,2},
		{[]uint64{2, 1, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},

		{[]uint64{2, 1, 1, 1},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},1,1},
		{[]uint64{2, 1, 1, 1},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},
		{[]uint64{2, 1, 1, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},1,1},
		{[]uint64{2, 1, 1, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},
		{[]uint64{2, 1, 2, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}},2,2},
		{[]uint64{2, 1, 2, 2},[]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}},2,0},
	}
	for i, test := range cases {
		store := NewStore()
		store.Append(test.entries)
		store.hardState = proto.HardState{ViewStamp:proto.ViewStamp{ViewNum: test.viewNum}}
		vr := newVR(&Config{
			Num:               1,
			Peers:             []uint64{1},
			TransitionTimeout: 5,
			HeartbeatTimeout:  1,
			Store:             store,
			AppliedNum:        0,
		})
		for j := 0; j < len(test.offsets); j++ {
			vr.group.Set(uint64(j)+1, test.offsets[j], test.offsets[j]+1)
		}
		vr.tryCommit()
		if cn := vr.opLog.commitNum; cn != test.exp {
			t.Errorf("#%d: commit-number = %d, expected %d", i, cn, test.exp)
		}
	}
}

func TestIsTransitionTimeout(t *testing.T) {
	cases := []struct {
		pulse       int
		probability float64
		round       bool
	}{
		{5, 0, false},
		{13, 0.3, true},
		{15, 0.5, true},
		{18, 0.8, true},
		{20, 1, false},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               1,
			Peers:             []uint64{1},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             NewStore(),
			AppliedNum:        0,
		})
		vr.pulse = test.pulse
		c := 0
		for j := 0; j < 10000; j++ {
			if vr.isTransitionTimeout() {
				c++
			}
		}
		v := float64(c) / 10000.0
		if test.round {
			v = math.Floor(v*10+0.5) / 10.0
		}
		if v != test.probability {
			t.Errorf("#%d: possibility = %v, expected %v", i, v, test.probability)
		}
	}
}

func TestCallIgnoreLateViewNumMessage(t *testing.T) {
	called := false
	fakeCall := func(r *VR, m proto.Message) {
		called = true
	}
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             NewStore(),
		AppliedNum:        0,
	})
	vr.call = fakeCall
	vr.ViewStamp.ViewNum = 2
	vr.Call(proto.Message{Type: proto.Prepare, ViewStamp:proto.ViewStamp{ViewNum: vr.ViewStamp.ViewNum - 1}})
	if called == true {
		t.Errorf("call function called = %v , expected %v", called, false)
	}
}

func TestHandleMessagePrepare(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		m            proto.Message
		expOpNum     uint64
		expCommitNum uint64
		expIgnore    bool
	}{
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 3, CommitNum: 3},2,0,true},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o3, LogNum: 3, CommitNum: 3},2,0,true},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o1, LogNum: 1, CommitNum: 1},2,1,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o0, LogNum: 0, CommitNum: 1, Entries: []proto.Entry{{ViewStamp:v2o1}}},1, 1,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 2, CommitNum: 3, Entries: []proto.Entry{{ViewStamp:v2o3}, {ViewStamp:v2o4}}},4, 3, false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 2, CommitNum: 4, Entries: []proto.Entry{{ViewStamp:v2o3}}},3, 3,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o1, LogNum: 1, CommitNum: 4, Entries: []proto.Entry{{ViewStamp:v2o2}}},2, 2,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v1o1, LogNum: 1, CommitNum: 3},2,1, false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v1o1, LogNum: 1, CommitNum: 3, Entries: []proto.Entry{{ViewStamp:v2o2}}},2,2,false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 2, CommitNum: 3},2,2, false},
		{proto.Message{Type: proto.Prepare, ViewStamp:v2o2, LogNum: 2, CommitNum: 4},2,2, false},
	}
	for i, test := range cases {
		store := NewStore()
		store.Append([]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}})
		vr := newVR(&Config{
			Num:               1,
			Peers:             []uint64{1},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             store,
			AppliedNum:        0,
		})
		vr.becomeBackup(proto.ViewStamp{ViewNum:2}, None)
		vr.handleAppend(test.m)
		if vr.opLog.lastOpNum() != test.expOpNum {
			t.Errorf("#%d: last op-number = %d, expected %d", i, vr.opLog.lastOpNum(), test.expOpNum)
		}
		if vr.opLog.commitNum != test.expCommitNum {
			t.Errorf("#%d: commit-number = %d, expected %d", i, vr.opLog.commitNum, test.expCommitNum)
		}
		m := vr.handleMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: message = nil, expected 1", i)
		}
		//if m[0].Ignore != test.expIgnore {
		//	t.Errorf("#%d: ignore = %v, expected %v", i, m[0].Ignore, test.expIgnore)
		//}
	}
}

func TestHandleHeartbeat(t *testing.T) {
	initViewStampCase()
	commitNum := uint64(2)
	cases := []struct {
		m            proto.Message
		expCommitNum uint64
	}{
		{proto.Message{From: replicaB, To: replicaA, Type: proto.Prepare, ViewStamp:proto.ViewStamp{ViewNum: 2}, CommitNum: commitNum + 1}, commitNum + 1},
		{proto.Message{From: replicaB, To: replicaA, Type: proto.Prepare, ViewStamp:proto.ViewStamp{ViewNum: 2}, CommitNum: commitNum - 1}, commitNum},
	}
	for i, test := range cases {
		store := NewStore()
		store.Append([]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}, {ViewStamp:v3o3}})
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{1},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             store,
			AppliedNum:        0,
		})
		vr.becomeBackup(proto.ViewStamp{ViewNum:2}, replicaB)
		vr.opLog.commitTo(commitNum)
		vr.handleHeartbeat(test.m)
		if vr.opLog.commitNum != test.expCommitNum {
			t.Errorf("#%d: commit-number = %d, expected %d", i, vr.opLog.commitNum, test.expCommitNum)
		}
		m := vr.handleMessages()
		if len(m) != 1 {
			t.Fatalf("#%d: message = nil, expected 1", i)
		}
		if m[0].Type != proto.CommitOk {
			t.Errorf("#%d: type = %v, expected heartbeat commit-number ok", i, m[0].Type)
		}
	}
}

func TestHandleCommitOk(t *testing.T) {
	initViewStampCase()
	store := NewStore()
	store.Append([]proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v2o2}, {ViewStamp:v3o3}})
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{1, 2},
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             store,
		AppliedNum:        0,
	})
	vr.becomeReplica()
	vr.becomePrimary()
	vr.opLog.commitTo(vr.opLog.lastOpNum())
	vr.Call(proto.Message{From: 2, Type: proto.CommitOk})
	msgs := vr.handleMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d, expected 1", len(msgs))
	}
	if msgs[0].Type != proto.Prepare {
		t.Errorf("type = %v, expected prepare", msgs[0].Type)
	}
	vr.Call(proto.Message{From: 2, Type: proto.CommitOk})
	msgs = vr.handleMessages()
	if len(msgs) != 0 {
		t.Fatalf("len(messages) = %d, expected 0", len(msgs))
	}
	vr.broadcastHeartbeat()
	vr.Call(proto.Message{From: 2, Type: proto.CommitOk})
	msgs = vr.handleMessages()
	if len(msgs) != 2 {
		t.Fatalf("len(messages) = %d, expected 2", len(msgs))
	}
	if msgs[0].Type != proto.Commit {
		t.Errorf("type = %v, expected heartbeat commitNum", msgs[0].Type)
	}
	if msgs[1].Type != proto.Prepare {
		t.Errorf("type = %v, expected message prepare", msgs[1].Type)
	}
	vr.Call(proto.Message{
		From:  2,
		Type:  proto.PrepareOk,
		ViewStamp:proto.ViewStamp{OpNum: msgs[1].ViewStamp.OpNum + uint64(len(msgs[1].Entries))},
	})
	vr.handleMessages()
	vr.broadcastHeartbeat()
	vr.Call(proto.Message{From: 2, Type: proto.CommitOk})
	msgs = vr.handleMessages()
	if len(msgs) != 1 {
		t.Fatalf("len(messages) = %d, expected 1: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != proto.Commit {
		t.Errorf("type = %v, expected heartbeat commitNum", msgs[0].Type)
	}
}

func TestMessagePrepareOkDelayReset(t *testing.T) {
	vr := newVR(&Config{
		Num:               replicaA,
		Peers:             []uint64{replicaA, replicaB, replicaC},
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             NewStore(),
		AppliedNum:        0,
	})
	vr.becomeReplica()
	vr.becomePrimary()
	vr.broadcastAppend()
	vr.handleMessages()
	vr.Call(proto.Message{
		From:  2,
		Type:  proto.PrepareOk,
		ViewStamp:proto.ViewStamp{OpNum:1},
	})
	if vr.CommitNum != 1 {
		t.Fatalf("expecteded commit-number to be 1, got %d", vr.CommitNum)
	}
	vr.handleMessages()
	vr.Call(proto.Message{
		From:    1,
		Type:    proto.Request,
		Entries: []proto.Entry{{}},
	})
	msgs := vr.handleMessages()
	if len(msgs) != 1 {
		t.Fatalf("expecteded 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != proto.Prepare || msgs[0].To != 2 {
		t.Errorf("expecteded prepare to bus 2, got %s to %d", msgs[0].Type, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].ViewStamp.OpNum != 2 {
		t.Errorf("expecteded to trigger entry 2, but got %v", msgs[0].Entries)
	}
	vr.Call(proto.Message{
		From:  3,
		Type:  proto.PrepareOk,
		ViewStamp:proto.ViewStamp{OpNum:1},
	})
	msgs = vr.handleMessages()
	if len(msgs) != 1 {
		t.Fatalf("expecteded 1 message, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != proto.Prepare || msgs[0].To != 3 {
		t.Errorf("expecteded message prepare to bus 3, got %s to %d", msgs[0].Type, msgs[0].To)
	}
	if len(msgs[0].Entries) != 1 || msgs[0].Entries[0].ViewStamp.OpNum != 2 {
		t.Errorf("expecteded to trigger entry 2, but got %v", msgs[0].Entries)
	}
}

func TestStateTransition(t *testing.T) {
	cases := []struct {
		from       role
		to         role
		expAllow   bool
		expViewNum uint64
		expPrim    uint64
	}{
		{Backup,Backup,true,1,None},
		{Backup,Replica,true,1,None},
		{Backup,Primary,false,0,None},

		{Replica,Backup,true,0,None},
		{Replica,Replica,true,1,None},
		{Replica,Primary,true,0,1},

		{Primary,Backup,true,1,None},
		{Primary,Replica,false,1,None},
		{Primary,Primary,true,0,1},
	}
	for i, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if test.expAllow == true {
						t.Errorf("%d: allow = %v, expected %v", i, false, true)
					}
				}
			}()
			vr := newVR(&Config{
				Num:               1,
				Peers:             []uint64{1},
				TransitionTimeout: 10,
				HeartbeatTimeout:  1,
				Store:             NewStore(),
				AppliedNum:        0,
			})
			vr.role = test.from
			switch test.to {
			case Backup:
				vr.becomeBackup(proto.ViewStamp{ViewNum:test.expViewNum}, test.expPrim)
			case Replica:
				vr.becomeReplica()
			case Primary:
				vr.becomePrimary()
			}
			if vr.ViewStamp.ViewNum != test.expViewNum {
				t.Errorf("%d: view-number = %d, expected %d", i, vr.ViewStamp.ViewNum, test.expViewNum)
			}
			if vr.prim != test.expPrim {
				t.Errorf("%d: prim = %d, expected %d", i, vr.prim, test.expPrim)
			}
		}()
	}
}

func TestAllServerCallDown(t *testing.T) {
	cases := []struct {
		role       role
		expRole    role
		expViewNum uint64
		expOpNum   uint64
	}{
		{Backup,Backup,3,0},
		{Replica,Backup,3,0},
		{Primary,Backup,3,1},
	}
	msgTypes := [...]proto.MessageType{proto.Prepare}
	viewNum := uint64(3)
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               1,
			Peers:             []uint64{1, 2, 3},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             NewStore(),
			AppliedNum:        0,
		})
		switch test.role {
		case Backup:
			vr.becomeBackup(proto.ViewStamp{ViewNum:1}, None)
		case Replica:
			vr.becomeReplica()
		case Primary:
			vr.becomeReplica()
			vr.becomePrimary()
		}
		for j, msgType := range msgTypes {
			vr.Call(proto.Message{From: 2, Type: msgType, ViewStamp:proto.ViewStamp{ViewNum: viewNum}})

			if vr.role != test.expRole {
				t.Errorf("#%d.%d role = %v, expected %v", i, j, vr.role, test.expRole)
			}
			if vr.ViewStamp.ViewNum != test.expViewNum {
				t.Errorf("#%d.%d view-number = %v, expected %v", i, j, vr.ViewStamp.ViewNum, test.expViewNum)
			}
			if uint64(vr.opLog.lastOpNum()) != test.expOpNum {
				t.Errorf("#%d.%d op-number = %v, expected %v", i, j, vr.opLog.lastOpNum(), test.expOpNum)
			}
			if uint64(len(vr.opLog.totalEntries())) != test.expOpNum {
				t.Errorf("#%d.%d len(entries) = %v, expected %v", i, j, len(vr.opLog.totalEntries()), test.expOpNum)
			}
			expPrim := uint64(2)
			if vr.prim != expPrim {
				t.Errorf("#%d, vr.prim = %d, expected %d", i, vr.prim, None)
			}
		}
	}
}

func TestPrimaryPrepareOk(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		opNum        uint64
		expOffset    uint64
		expNext      uint64
		expMsgNum    int
		expOpNum     uint64
		expCommitNum uint64
	}{
		{2,2,4,2,2,2},
		{0,0,3,0,0,0},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB, replicaC},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             NewStore(),
			AppliedNum:        0,
		})
		vr.opLog = &opLog{
			store:  &Store{entries: []proto.Entry{{}, {ViewStamp:v0o1}, {ViewStamp:v1o2}}},
			unsafe: unsafe{offset: 3},
		}
		vr.becomeReplica()
		vr.becomePrimary()
		vr.handleMessages()
		vr.Call(proto.Message{From: replicaB, Type: proto.PrepareOk, ViewStamp:proto.ViewStamp{OpNum: test.opNum, ViewNum: vr.ViewStamp.ViewNum}, Note: test.opNum})
		window := vr.group.Replica(replicaB)
		if window.Ack != test.expOffset {
			t.Errorf("#%d offsets = %d, expected %d", i, window.Ack, test.expOffset)
		}
		if window.Next != test.expNext {
			t.Errorf("#%d next = %d, expected %d", i, window.Next, test.expNext)
		}
		msgs := vr.handleMessages()
		if len(msgs) != test.expMsgNum {
			t.Errorf("#%d message number = %d, expected %d", i, len(msgs), test.expMsgNum)
		}
		for j, msg := range msgs {
			if msg.ViewStamp.OpNum != test.expOpNum {
				t.Errorf("#%d.%d op-number = %d, expected %d", i, j, msg.ViewStamp.OpNum, test.expOpNum)
			}
			if msg.CommitNum != test.expCommitNum {
				t.Errorf("#%d.%d commit-number = %d, expected %d", i, j, msg.CommitNum, test.expCommitNum)
			}
		}
	}
}

func TestPrimaryRecovery(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		opNum        uint64
		Type         proto.MessageType
		expOffset    uint64
		expNext      uint64
		expMsgNum    int
		expOpNum     uint64
		expCommitNum uint64
	}{
		{3,proto.Recovery,0,3,0,0,0},
		{2,proto.Recovery,0,2,1,1,0},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB, replicaC},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             NewStore(),
			AppliedNum:        0,
		})
		vr.opLog = &opLog{
			store:  &Store{entries: []proto.Entry{{}, {ViewStamp:v0o1}, {ViewStamp:v1o2}}},
			unsafe: unsafe{offset: 3},
		}
		vr.becomeReplica()
		vr.becomePrimary()
		vr.handleMessages()
		vr.Call(proto.Message{From: replicaB, Type: test.Type, ViewStamp:proto.ViewStamp{OpNum: test.opNum, ViewNum: vr.ViewStamp.ViewNum}, Note: test.opNum})
		window := vr.group.Replica(replicaB)
		if window.Ack != test.expOffset {
			t.Errorf("#%d offsets = %d, expected %d", i, window.Ack, test.expOffset)
		}
		if window.Next != test.expNext {
			t.Errorf("#%d next = %d, expected %d", i, window.Next, test.expNext)
		}
		msgs := vr.handleMessages()
		if len(msgs) != test.expMsgNum {
			t.Errorf("#%d message number = %d, expected %d", i, len(msgs), test.expMsgNum)
		}
		for j, msg := range msgs {
			if msg.ViewStamp.OpNum != test.expOpNum {
				t.Errorf("#%d.%d op-number = %d, expected %d", i, j, msg.ViewStamp.OpNum, test.expOpNum)
			}
			if msg.CommitNum != test.expCommitNum {
				t.Errorf("#%d.%d commit-number = %d, expected %d", i, j, msg.CommitNum, test.expCommitNum)
			}
		}
	}
}

func TestPrimaryGetState(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		opNum        uint64
		Type         proto.MessageType
		expOffset    uint64
		expNext      uint64
		expMsgNum    int
		expOpNum     uint64
		expCommitNum uint64
	}{
		{3,proto.GetState,0,3,0,0,0},
		{2,proto.GetState,0,2,1,1,0},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB, replicaC},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             NewStore(),
			AppliedNum:        0,
		})
		vr.opLog = &opLog{
			store:  &Store{entries: []proto.Entry{{}, {ViewStamp:v0o1}, {ViewStamp:v1o2}}},
			unsafe: unsafe{offset: 3},
		}
		vr.becomeReplica()
		vr.becomePrimary()
		vr.handleMessages()
		vr.Call(proto.Message{From: replicaB, Type: test.Type, ViewStamp:proto.ViewStamp{OpNum: test.opNum, ViewNum: vr.ViewStamp.ViewNum}, Note: test.opNum})
		window := vr.group.Replica(replicaB)
		if window.Ack != test.expOffset {
			t.Errorf("#%d offsets = %d, expected %d", i, window.Ack, test.expOffset)
		}
		if window.Next != test.expNext {
			t.Errorf("#%d next = %d, expected %d", i, window.Next, test.expNext)
		}
		msgs := vr.handleMessages()
		if len(msgs) != test.expMsgNum {
			t.Errorf("#%d message number = %d, expected %d", i, len(msgs), test.expMsgNum)
		}
		for j, msg := range msgs {
			if msg.ViewStamp.OpNum != test.expOpNum {
				t.Errorf("#%d.%d op-number = %d, expected %d", i, j, msg.ViewStamp.OpNum, test.expOpNum)
			}
			if msg.CommitNum != test.expCommitNum {
				t.Errorf("#%d.%d commit-number = %d, expected %d", i, j, msg.CommitNum, test.expCommitNum)
			}
		}
	}
}

func TestBroadcastHeartbeat(t *testing.T) {
	offset := uint64(1000)
	as := proto.AppliedState{
		Applied: proto.Applied{
			ViewStamp:proto.ViewStamp{ViewNum: 1, OpNum: offset},
			// configure nodes node
		},
	}
	store := NewStore()
	store.SetAppliedState(as)
	vr := newVR(&Config{
		Num:               replicaA,
		Peers:             []uint64{replicaA, replicaB, replicaC},
		TransitionTimeout: 10,
		HeartbeatTimeout:  1,
		Store:             store,
		AppliedNum:        0,
	})
	vr.ViewStamp.ViewNum = 1
	vr.becomeReplica()
	vr.becomePrimary()
	for i := 0; i < 10; i++ {
		vr.appendEntry(proto.Entry{ViewStamp:proto.ViewStamp{OpNum: uint64(i) + 1}})
	}
	vr.group.Set(replicaB, 5, 6)
	vr.group.Set(replicaC, vr.opLog.lastOpNum(), vr.opLog.lastOpNum()+1)
	vr.Call(proto.Message{Type: proto.Heartbeat})
	msgs := vr.handleMessages()
	if len(msgs) != 2 {
		t.Fatalf("len(messages) = %v, expected 2", len(msgs))
	}
	expectedCommitMap := map[uint64]uint64{
		2: min(vr.opLog.commitNum, vr.group.Replica(2).Ack),
		3: min(vr.opLog.commitNum, vr.group.Replica(3).Ack),
	}
	for i, m := range msgs {
		if m.Type != proto.Commit {
			t.Fatalf("#%d: type = %v, expected = %v", i, m.Type, proto.Commit)
		}
		if m.ViewStamp.OpNum != 0 {
			t.Fatalf("#%d: prev op-number = %d, expected %d", i, m.ViewStamp.OpNum, 0)
		}
		if expectedCommitMap[m.To] == 0 {
			t.Fatalf("#%d: unexpecteded to %d", i, m.To)
		} else {
			if m.CommitNum != expectedCommitMap[m.To] {
				t.Fatalf("#%d: commit-number = %d, expected %d", i, m.CommitNum, expectedCommitMap[m.To])
			}
			delete(expectedCommitMap, m.To)
		}
		if len(m.Entries) != 0 {
			t.Fatalf("#%d: len(entries) = %d, expected 0", i, len(m.Entries))
		}
	}
}

func TestReceiveMessageHeartbeat(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		role   role
		expMsg int
	}{
		{Primary,2},
		{Replica,0},
		{Backup,0},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB, replicaC},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             NewStore(),
			AppliedNum:        0,
		})
		vr.opLog = &opLog{store: &Store{entries: []proto.Entry{{}, {ViewStamp:v0o1}, {ViewStamp:v1o2}}}}
		vr.ViewStamp.ViewNum = 1
		vr.role = test.role
		switch test.role {
		case Backup:
			vr.call = callBackup
		case Replica:
			vr.call = callReplica
		case Primary:
			vr.call = callPrimary
		}
		vr.Call(proto.Message{From: 1, To: 1, Type: proto.Heartbeat})
		msgs := vr.handleMessages()
		if len(msgs) != test.expMsg {
			t.Errorf("%d: len(messages) = %d, expected %d", i, len(msgs), test.expMsg)
		}
		for _, m := range msgs {
			if m.Type != proto.Commit {
				t.Errorf("%d: msg.type = %v, expected %v", i, m.Type, proto.Commit)
			}
		}
	}
}

func TestPrimaryIncreaseNext(t *testing.T) {
	initViewStampCase()
	prevEntries := []proto.Entry{{ViewStamp:v1o1}, {ViewStamp:v1o2}, {ViewStamp:v1o3}}
	cases := []struct {
		offset  uint64
		next    uint64
		expNext uint64
	}{
		{1,2,uint64(len(prevEntries) + 1 + 1 + 1)},
		{0,2,2},
	}
	for i, test := range cases {
		vr := newVR(&Config{
			Num:               replicaA,
			Peers:             []uint64{replicaA, replicaB},
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             NewStore(),
			AppliedNum:        0,
		})
		vr.opLog.append(prevEntries...)
		vr.becomeReplica()
		vr.becomePrimary()
		vr.group.Set(replicaB, test.offset, test.next)
		vr.Call(requestMessage(replicaA, replicaA))
		window := vr.group.Replica(replicaB)
		if window.Next != test.expNext {
			t.Errorf("#%d next = %d, expected %d", i, window.Next, test.expNext)
		}
	}
}

func TestRaising(t *testing.T) {
	num := uint64(1)
	cases := []struct {
		peers  []uint64
		expRai bool
	}{
		{[]uint64{replicaA},true},
		{[]uint64{replicaA, replicaB, replicaC},true},
		{nil,false},
		{[]uint64{replicaB, replicaC},false},
	}
	for i, test := range cases {
		r := newVR(&Config{
			num,
			test.peers,
			NewStore(),
			5,
			1,
			0,
			RoundRobin,
		})
		if rv := r.raising(); rv != test.expRai {
			t.Errorf("#%d: raising = %v, expected %v", i, rv, test.expRai)
		}
	}
}

func TestVRReplicas(t *testing.T) {
	cases := []struct {
		peers    []uint64
		expPeers []uint64
	}{
		{
			[]uint64{replicaA, replicaB, replicaC},
			[]uint64{replicaA, replicaB, replicaC},
		},
		{
			[]uint64{replicaC, replicaB, replicaA},
			[]uint64{replicaA, replicaB, replicaC},
		},
	}
	for i, test := range cases {
		r := newVR(&Config{
			Num:               replicaA,
			Peers:             test.peers,
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             NewStore(),
			AppliedNum:        0,
		})
		if !reflect.DeepEqual(r.group.ReplicaNums(), test.expPeers) {
			t.Errorf("#%d: replicas = %+v, expected %+v", i, r.group.Members(), test.expPeers)
		}
	}
}

func TestWindowDec(t *testing.T) {
	r := newVR(&Config{
		Num:               replicaA,
		Peers:             []uint64{replicaA, replicaB},
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             NewStore(),
		AppliedNum:        0,
	})
	r.becomeReplica()
	r.becomePrimary()
	r.group.Replica(replicaB).DelaySet(r.heartbeatTimeout*2)
	r.Call(proto.Message{From: 1, To: 1, Type: proto.Heartbeat})
	if r.group.Replica(replicaB).Delay != r.heartbeatTimeout*(2-1) {
		t.Errorf("delay = %d, expected %d", r.group.Replica(2).Delay, r.heartbeatTimeout*(2-1))
	}
}

func TestWindowDelay(t *testing.T) {
	r := newVR(&Config{
		Num:               replicaA,
		Peers:             []uint64{replicaA, replicaB},
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             NewStore(),
		AppliedNum:        0,
	})
	r.becomeReplica()
	r.becomePrimary()
	r.Call(requestMessage(replicaA, replicaA))
	r.Call(requestMessage(replicaA, replicaA))
	r.Call(requestMessage(replicaA, replicaA))
	rv := r.handleMessages()
	if len(rv) != 1 {
		t.Errorf("len(rv) = %d, expected 1", len(rv))
	}
}

func TestCannotCommitWithoutNewViewNumEntry(t *testing.T) {
	m := newMock(node, node, node, node, node)
	m.trigger(changeMessage(replicaA, replicaA))
	for _, route := range routes {
		m.cover(route[0], route[1])
	}
	m.trigger(requestMessage(replicaA, replicaA))
	m.trigger(requestMessage(replicaA, replicaA))
	peer := m.peers(replicaA)
	if peer.opLog.commitNum != 1 {
		t.Errorf("commit-number = %d, expected %d", peer.opLog.commitNum, 1)
	}
	m.reset()
	m.ignore(proto.Prepare)
	m.trigger(changeMessage(replicaB, replicaB))
	peer = m.peers(replicaB)
	if peer.opLog.commitNum != 1 {
		t.Errorf("commit-number = %d, expected %d", peer.opLog.commitNum, 1)
	}
	m.reset()
	m.trigger(heartbeatMessage(replicaB, replicaB))
	m.trigger(requestMessage(replicaA, replicaA))
	if peer.opLog.commitNum != 5 {
		t.Errorf("commit-number = %d, expected %d", peer.opLog.commitNum, 5)
	}
}

func TestCommitWithoutNewViewNumEntry(t *testing.T) {
	m := newMock(node, node, node, node, node)
	m.trigger(changeMessage(replicaA, replicaA))
	for _, route := range routes {
		m.cover(route[0], route[1])
	}
	m.trigger(requestMessage(replicaA, replicaA))
	m.trigger(requestMessage(replicaA, replicaA))
	peer := m.peers(replicaA)
	if peer.opLog.commitNum != 1 {
		t.Errorf("commit-number = %d, expected %d", peer.opLog.commitNum, 1)
	}
	m.reset()
	m.trigger(changeMessage(replicaB, replicaB))
	if peer.opLog.commitNum != 4 {
		t.Errorf("commit-number = %d, expected %d", peer.opLog.commitNum, 4)
	}
}

func TestLateMessages(t *testing.T) {
	initViewStampCase()
	m := newMock(node, node, node)
	m.trigger(changeMessage(replicaA, replicaA))
	m.trigger(changeMessage(replicaB, replicaB))
	m.trigger(changeMessage(replicaA, replicaA))
	m.trigger(proto.Message{From: replicaB, To: replicaA, Type: proto.Prepare, ViewStamp:proto.ViewStamp{ViewNum: 2}, Entries: []proto.Entry{{ViewStamp:v2o3}}})
	m.trigger(proto.Message{From: replicaA, To: replicaA, Type: proto.Request, Entries: []proto.Entry{{Data: []byte("testdata")}}})
	opLog := &opLog{
		store: &Store{
			entries: []proto.Entry{
				{}, {Data: nil, ViewStamp:v1o1},
				{Data: nil, ViewStamp:v2o2}, {Data: nil, ViewStamp:v3o3},
				{Data: []byte("testdata"), ViewStamp:v3o4},
			},
		},
		unsafe:    unsafe{offset: 5},
		commitNum: 4,
	}
	base := stringOpLog(opLog)
	for i, p := range m.nodes {
		if sm, ok := p.(*VR); ok {
			l := stringOpLog(sm.opLog)
			if g := diff(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty opLog", i)
		}
	}
}

func TestLazyReplicaRestore(t *testing.T) {
	m := newMock(node, node, node)
	m.trigger(changeMessage(replicaA, replicaA))
	m.shield(replicaC)
	for j := 0; j <= 100; j++ {
		m.trigger(requestMessageEmptyEntries(replicaA, replicaA))
	}
	prim := m.peers(replicaA)
	safeEntries(prim, m.stores[1])
	//m.stores[1].CreateAppliedState(prim.opLog.appliedNum, nil, nil) // need to configure
	//m.stores[1].Archive(prim.opLog.appliedNum)
	m.reset()
	m.trigger(requestMessageEmptyEntries(replicaA, replicaA))
	backup := m.peers(replicaC) // temp fix 2, skip test
	m.trigger(requestMessageEmptyEntries(replicaA, replicaA))
	if backup.opLog.commitNum != prim.opLog.commitNum {
		t.Errorf("backup.commit-number = %d, expected %d", backup.opLog.commitNum, prim.opLog.commitNum)
	}
}