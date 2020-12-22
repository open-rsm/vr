package vr

import (
	"reflect"
	"testing"
	"github.com/open-rsm/spec/proto"
)

func TestUnsafeTryViewNum(t *testing.T) {
	cases := []struct {
		entries    []proto.Entry
		offset     uint64
		opNum      uint64
		expOk      bool
		expViewNum uint64
	}{
		{[]proto.Entry{{OpNum: 5, ViewNum: 1}},5,5,true,1 },
		{[]proto.Entry{{OpNum: 5, ViewNum: 1}},5,6,false,0 },
		{[]proto.Entry{{OpNum: 5, ViewNum: 1}},5,4,false,0 },
		{[]proto.Entry{},0,5,false,0 },
	}
	for i, test := range cases {
		u := unsafe{
			entries: test.entries,
			offset:  test.offset,
		}
		viewNum, ok := u.tryGetViewNum(test.opNum)
		if ok != test.expOk {
			t.Errorf("#%d: ok = %t, expected %t", i, ok, test.expOk)
		}
		if viewNum != test.expViewNum {
			t.Errorf("#%d: view-number = %d, expected %d", i, viewNum, test.expViewNum)
		}
	}
}

func TestUnsafeRecover(t *testing.T) {
	us := unsafe{
		entries:  []proto.Entry{{OpNum: 5, ViewNum: 1}},
		offset:   5,
		appliedState: &proto.AppliedState{Applied: proto.Applied{OpNum: 4, ViewNum: 1}},
	}
	as := proto.AppliedState{Applied: proto.Applied{OpNum: 6, ViewNum: 2}}
	us.recover(as)

	if us.offset != as.Applied.OpNum+1 {
		t.Errorf("offset = %d, expected %d", us.offset, as.Applied.OpNum+1)
	}
	if len(us.entries) != 0 {
		t.Errorf("len = %d, expected 0", len(us.entries))
	}
	if !reflect.DeepEqual(us.appliedState, &as) {
		t.Errorf("applied state = %v, expected %v", us.appliedState, &as)
	}
}

func TestUnsafeSafeTo(t *testing.T) {
	cases := []struct {
		entries   []proto.Entry
		offset    uint64
		opNum     uint64
		viewNum   uint64
		expOffset uint64
		expLen    int
	}{
		{
			[]proto.Entry{}, 0,
			5, 1,
			0, 0,
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5,
			5, 1, // safe to the first entry
			6, 0,
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}, {OpNum: 6, ViewNum: 1}}, 5,
			5, 1, // safe to the first entry
			6, 1,
		},
		{
			[]proto.Entry{{OpNum: 6, ViewNum: 2}}, 5,
			6, 1, // safe to the first entry and view-number mismatch
			5, 1,
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5,
			4, 1, // safe to old entry
			5, 1,
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5,
			4, 2, // safe to old entry
			5, 1,
		},
	}
	for i, test := range cases {
		u := unsafe{
			entries: test.entries,
			offset:  test.offset,
		}
		u.safeTo(test.opNum, test.viewNum)
		if u.offset != test.expOffset {
			t.Errorf("#%d: offset = %d, expected %d", i, u.offset, test.expOffset)
		}
		if len(u.entries) != test.expLen {
			t.Errorf("#%d: len = %d, expected %d", i, len(u.entries), test.expLen)
		}
	}
}

func TestUnsafeTruncateAndAppend(t *testing.T) {
	cases := []struct {
		entries    []proto.Entry
		offset     uint64
		toAppend   []proto.Entry
		expOffset  uint64
		expEntries []proto.Entry
	}{
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5,
			[]proto.Entry{{OpNum: 6, ViewNum: 1}, {OpNum: 7, ViewNum: 1}},
			5, []proto.Entry{{OpNum: 5, ViewNum: 1}, {OpNum: 6, ViewNum: 1}, {OpNum: 7, ViewNum: 1}},
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5,
			[]proto.Entry{{OpNum: 5, ViewNum: 2}, {OpNum: 6, ViewNum: 2}},
			5, []proto.Entry{{OpNum: 5, ViewNum: 2}, {OpNum: 6, ViewNum: 2}},
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5,
			[]proto.Entry{{OpNum: 4, ViewNum: 2}, {OpNum: 5, ViewNum: 2}, {OpNum: 6, ViewNum: 2}},
			4, []proto.Entry{{OpNum: 4, ViewNum: 2}, {OpNum: 5, ViewNum: 2}, {OpNum: 6, ViewNum: 2}},
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}, {OpNum: 6, ViewNum: 1}, {OpNum: 7, ViewNum: 1}}, 5,
			[]proto.Entry{{OpNum: 6, ViewNum: 2}},
			5, []proto.Entry{{OpNum: 5, ViewNum: 1}, {OpNum: 6, ViewNum: 2}},
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}, {OpNum: 6, ViewNum: 1}, {OpNum: 7, ViewNum: 1}}, 5,
			[]proto.Entry{{OpNum: 7, ViewNum: 2}, {OpNum: 8, ViewNum: 2}},
			5, []proto.Entry{{OpNum: 5, ViewNum: 1}, {OpNum: 6, ViewNum: 1}, {OpNum: 7, ViewNum: 2}, {OpNum: 8, ViewNum: 2}},
		},
	}
	for i, test := range cases {
		u := unsafe{
			entries: test.entries,
			offset:  test.offset,
		}
		u.truncateAndAppend(test.toAppend)
		if u.offset != test.expOffset {
			t.Errorf("#%d: offset = %d, expected %d", i, u.offset, test.expOffset)
		}
		if !reflect.DeepEqual(u.entries, test.expEntries) {
			t.Errorf("#%d: entries = %v, expected %v", i, u.entries, test.expEntries)
		}
	}
}

func TestUnsafeTryStartOpNum(t *testing.T) {
	cases := []struct {
		entries  []proto.Entry
		offset   uint64
		as       *proto.AppliedState
		expOk    bool
		expOpNum uint64
	}{
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5, nil,
			false, 0,
		},
		{
			[]proto.Entry{}, 0, nil,
			false, 0,
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5, &proto.AppliedState{Applied: proto.Applied{OpNum: 4, ViewNum: 1}},
			true, 5,
		},
		{
			[]proto.Entry{}, 5, &proto.AppliedState{Applied: proto.Applied{OpNum: 4, ViewNum: 1}},
			true, 5,
		},
	}
	for i, test := range cases {
		us := unsafe{
			entries:  test.entries,
			offset:   test.offset,
			appliedState: test.as,
		}
		opNum, ok := us.tryGetStartOpNum()
		if ok != test.expOk {
			t.Errorf("#%d: ok = %t, expected %t", i, ok, test.expOk)
		}
		if opNum != test.expOpNum {
			t.Errorf("#%d: op-number = %d, expected %d", i, opNum, test.expOpNum)
		}
	}
}

func TestTryLastOpNum(t *testing.T) {
	cases := []struct {
		entries  []proto.Entry
		offset   uint64
		as       *proto.AppliedState
		expOk    bool
		expOpNum uint64
	}{
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5, nil,
			true, 5,
		},
		{
			[]proto.Entry{{OpNum: 5, ViewNum: 1}}, 5, &proto.AppliedState{Applied: proto.Applied{OpNum: 4, ViewNum: 1}},
			true, 5,
		},
		{
			[]proto.Entry{}, 5, &proto.AppliedState{Applied: proto.Applied{OpNum: 4, ViewNum: 1}},
			true, 4,
		},
		{
			[]proto.Entry{}, 0, nil,
			false, 0,
		},
	}
	for i, test := range cases {
		us := unsafe{
			entries: test.entries,
			offset:  test.offset,
			appliedState: test.as,
		}
		opNum, ok := us.tryGetLastOpNum()
		if ok != test.expOk {
			t.Errorf("#%d: ok = %t, expected %t", i, ok, test.expOk)
		}
		if opNum != test.expOpNum {
			t.Errorf("#%d: op-number = %d, expected %d", i, opNum, test.expOpNum)
		}
	}
}
