package vr

import (
	"fmt"
	"testing"
	"github.com/open-rsm/vr/proto"
)

func testRoundRobin() error {
	cases := []struct {
		vs      proto.ViewStamp
		peers   []uint64
		expNum  uint64
	}{
		{proto.ViewStamp{ViewNum:replicaD},[]uint64{replicaA}, replicaA},
		{proto.ViewStamp{ViewNum:replicaC},[]uint64{replicaA, replicaB, replicaC}, replicaA},
		{proto.ViewStamp{ViewNum:replicaB},[]uint64{replicaA, replicaD}, replicaA},
		{proto.ViewStamp{ViewNum:replicaC},[]uint64{replicaA, replicaC}, replicaA},
	}
	for i, test := range cases {
		windows := len(test.peers)
		if rv := roundRobin(test.vs, windows); rv != test.expNum {
			return fmt.Errorf("#%d: round_robin = %v, expected %v", i, rv, test.expNum)
		}
	}
	return nil
}

func TestRoundRobinSelector(t *testing.T) {
	if err := testRoundRobin(); err != nil {
		t.Error(err)
	}
}

func TestJudgeInvalidSelector(t *testing.T) {
	const (
		SelectorUnknown = iota - 1
		SelectorA
		SelectorB
	)
	cases := []struct {
		elector    int
		expResult bool
	}{
		{SelectorUnknown,false},
		{SelectorA,true},
		{SelectorB,false},
	}
	for i, test := range cases {
		if rv := isInvalidSelector(test.elector); rv != test.expResult {
			t.Errorf("#%d: is_invalid_selector = %v, expected %v", i, rv, test.expResult)
		}
	}
}
