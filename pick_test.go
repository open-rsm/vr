package vr

import "testing"

func TestRoundRobinPicker(t *testing.T) {
	buildWindows := func(nums []uint64) map[uint64]*Window {
		var windows = map[uint64]*Window{}
		for _, num := range nums {
			windows[num] = &Window{}
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
			t.Errorf("#%d: round_robin = %v, expected %v", i, rv, test.expNum)
		}
	}
}

func TestJudgeInvalidPicker(t *testing.T) {
	const (
		PickerUnknown = iota - 1
		PackerA
		PackerB
	)
	cases := []struct {
		picker    int
		expResult bool
	}{
		{PickerUnknown,false},
		{PackerA,true},
		{PackerB,false},
	}
	for i, test := range cases {
		if rv := isInvalidPicker(test.picker); rv != test.expResult {
			t.Errorf("#%d: is_invalid_picker = %v, expected %v", i, rv, test.expResult)
		}
	}
}
