package vr

import (
	"reflect"
	"testing"
	"github.com/open-rsm/vr/proto"
)

func TestUnsafeTryViewNum(t *testing.T) {
	initViewStampCase()
	cases := []struct {
		entries    []proto.Entry
		offset     uint64
		opNum      uint64
		expOk      bool
		expViewNum uint64
	}{
		{[]proto.Entry{{ViewStamp:v1o5}},5,5,true,1 },
		{[]proto.Entry{{ViewStamp:v1o5}},5,6,false,0 },
		{[]proto.Entry{{ViewStamp:v1o5}},5,4,false,0 },
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
	initViewStampCase()
	us := unsafe{
		entries:  []proto.Entry{{ViewStamp:v1o5}},
		offset:   5,
		appliedState: &proto.AppliedState{Applied: proto.Applied{ViewStamp:v1o4}},
	}
	as := proto.AppliedState{Applied: proto.Applied{ViewStamp:v2o6}}
	us.recover(as)

	if us.offset != as.Applied.ViewStamp.OpNum+1 {
		t.Errorf("offset = %d, expected %d", us.offset, as.Applied.ViewStamp.OpNum+1)
	}
	if len(us.entries) != 0 {
		t.Errorf("len = %d, expected 0", len(us.entries))
	}
	if !reflect.DeepEqual(us.appliedState, &as) {
		t.Errorf("applied state = %v, expected %v", us.appliedState, &as)
	}
}

func TestUnsafeSafeTo(t *testing.T) {
	initViewStampCase()
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
			[]proto.Entry{{ViewStamp:v1o5}}, 5,
			5, 1, // safe to the first entry
			6, 0,
		},
		{
			[]proto.Entry{{ViewStamp:v1o5}, {ViewStamp:v1o6}}, 5,
			5, 1, // safe to the first entry
			6, 1,
		},
		{
			[]proto.Entry{{ViewStamp:v2o6}}, 5,
			6, 1, // safe to the first entry and view-number mismatch
			5, 1,
		},
		{
			[]proto.Entry{{ViewStamp:v1o5}}, 5,
			4, 1, // safe to old entry
			5, 1,
		},
		{
			[]proto.Entry{{ViewStamp:v1o5}}, 5,
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
	initViewStampCase()
	cases := []struct {
		entries    []proto.Entry
		offset     uint64
		toAppend   []proto.Entry
		expOffset  uint64
		expEntries []proto.Entry
	}{
		{
			[]proto.Entry{{ViewStamp:v1o5}}, 5,
			[]proto.Entry{{ViewStamp:v1o6}, {ViewStamp:v1o7}},
			5, []proto.Entry{{ViewStamp:v1o5}, {ViewStamp:v1o6}, {ViewStamp:v1o7}},
		},
		{
			[]proto.Entry{{ViewStamp:v1o5}}, 5,
			[]proto.Entry{{ViewStamp:v2o5}, {ViewStamp:v2o6}},
			5, []proto.Entry{{ViewStamp:v2o5}, {ViewStamp:v2o6}},
		},
		{
			[]proto.Entry{{ViewStamp:v1o5}}, 5,
			[]proto.Entry{{ViewStamp:v2o4}, {ViewStamp:v2o5}, {ViewStamp:v2o6}},
			4, []proto.Entry{{ViewStamp:v2o4}, {ViewStamp: v2o5}, {ViewStamp:v2o6}},
		},
		{
			[]proto.Entry{{ViewStamp:v1o5}, {ViewStamp:v1o6}, {ViewStamp:v1o7}}, 5,
			[]proto.Entry{{ViewStamp:v2o6}},
			5, []proto.Entry{{ViewStamp:v1o5}, {ViewStamp:v2o6}},
		},
		{
			[]proto.Entry{{ViewStamp:v1o5}, {ViewStamp:v1o6}, {ViewStamp:v1o7}}, 5,
			[]proto.Entry{{ViewStamp:v2o7}, {ViewStamp:v2o8}},
			5, []proto.Entry{{ViewStamp:v1o5}, {ViewStamp:v1o6}, {ViewStamp:v2o7}, {ViewStamp:v2o8}},
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
	initViewStampCase()
	cases := []struct {
		entries  []proto.Entry
		offset   uint64
		as       *proto.AppliedState
		expOk    bool
		expOpNum uint64
	}{
		{
			[]proto.Entry{{ViewStamp:v1o5}}, 5, nil,
			false, 0,
		},
		{
			[]proto.Entry{}, 0, nil,
			false, 0,
		},
		{
			[]proto.Entry{{ViewStamp:v1o5}}, 5, &proto.AppliedState{Applied: proto.Applied{ViewStamp:v1o4}},
			true, 5,
		},
		{
			[]proto.Entry{}, 5, &proto.AppliedState{Applied: proto.Applied{ViewStamp:v1o4}},
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
	initViewStampCase()
	cases := []struct {
		entries  []proto.Entry
		offset   uint64
		as       *proto.AppliedState
		expOk    bool
		expOpNum uint64
	}{
		{
			[]proto.Entry{{ViewStamp:v1o5}}, 5, nil,
			true, 5,
		},
		{
			[]proto.Entry{{ViewStamp:v1o5}}, 5, &proto.AppliedState{Applied: proto.Applied{ViewStamp:v1o4}},
			true, 5,
		},
		{
			[]proto.Entry{}, 5, &proto.AppliedState{Applied: proto.Applied{ViewStamp:v1o4}},
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
