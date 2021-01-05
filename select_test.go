package vr

import (
	"fmt"
	"testing"
)

func testRoundRobin() error {
	buildWindows := func(nums []uint64) map[uint64]*Window {
		var windows = map[uint64]*Window{}
		for _, num := range nums {
			windows[num] = newWindow()
		}
		return windows
	}
	cases := []struct {
		viewNum uint64
		peers   []uint64
		windows func([]uint64) map[uint64]*Window
		expNum  uint64
	}{
		{replicaD,[]uint64{replicaA}, buildWindows,replicaA},
		{replicaC,[]uint64{replicaA, replicaB, replicaC}, buildWindows, replicaA},
		{replicaB,[]uint64{replicaA, replicaD}, buildWindows, replicaA},
		{replicaC,[]uint64{replicaA, replicaC}, buildWindows, replicaA},
	}
	for i, test := range cases {
		windows := test.windows(test.peers)
		if rv := roundRobin(test.viewNum, windows); rv != test.expNum {
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