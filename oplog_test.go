package vr

import (
	"reflect"
	"testing"
	"github.com/open-rsm/spec/proto"
)

func TestAppend(t *testing.T) {
	prevEntries := []proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 2}}
	cases := []struct {
		entries    []proto.Entry
		expOpNum   uint64
		expEntries []proto.Entry
		expUnsafe  uint64
	}{
		{[]proto.Entry{},2,[]proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 2}},3 },
		{[]proto.Entry{{OpNum: 3, ViewNum: 2}},3,[]proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 2}, {OpNum: 3, ViewNum: 2}},3 },
		{[]proto.Entry{{OpNum: 1, ViewNum: 2}},1,[]proto.Entry{{OpNum: 1, ViewNum: 2}},1 },
		{[]proto.Entry{{OpNum: 2, ViewNum: 3}, {OpNum: 3, ViewNum: 3}},3,[]proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 3}, {OpNum: 3, ViewNum: 3}}, 2 },
	}
	for i, test := range cases {
		store := NewStore()
		store.Append(prevEntries)
		log := newOpLog(store)
		opNum := log.append(test.entries...)
		if opNum != test.expOpNum {
			t.Errorf("#%d: last op-number = %d, expected %d", i, opNum, test.expOpNum)
		}
		if rv := log.entries(1); !reflect.DeepEqual(rv, test.expEntries) {
			t.Errorf("#%d: log entries = %+v, expected %+v", i, rv, test.expEntries)
		}
		if v := log.unsafe.offset; v != test.expUnsafe {
			t.Errorf("#%d: unsafe = %d, expected %d", i, v, test.expUnsafe)
		}
	}
}

func TestTryAppend(t *testing.T) {
	prevEntries := []proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 2}, {OpNum: 3, ViewNum: 3}}
	lastOpNum := uint64(3)
	lastViewNum := uint64(3)
	commitNum := uint64(1)
	cases := []struct {
		logViewNum   uint64
		opNum        uint64
		commitNum    uint64
		entries      []proto.Entry
		expLastOpNum uint64
		expAppend    bool
		expCommitNum uint64
		expPanic     bool
	}{
		{
			lastViewNum - 1,lastOpNum,lastOpNum,[]proto.Entry{{OpNum: lastOpNum + 1, ViewNum: 4}},
			0,false,commitNum,false,
		},
		{
			lastViewNum,lastOpNum + 1,lastOpNum,[]proto.Entry{{OpNum: lastOpNum + 2, ViewNum: 4}},
			0,false,commitNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum,nil,
			lastOpNum,true,lastOpNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum + 1,nil,
			lastOpNum,true,lastOpNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum - 1,nil,
			lastOpNum,true,lastOpNum - 1,false,
		},
		{
			lastViewNum,lastOpNum,0,nil,
			lastOpNum,true,commitNum,false,
		},
		{
			0,0,lastOpNum,nil,
			0,true,commitNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum,[]proto.Entry{{OpNum: lastOpNum + 1, ViewNum: 4}},
			lastOpNum + 1,true,lastOpNum,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum + 1,[]proto.Entry{{OpNum: lastOpNum + 1, ViewNum: 4}},
			lastOpNum + 1,true,lastOpNum + 1,false,
		},
		{
			lastViewNum,lastOpNum,lastOpNum + 2,[]proto.Entry{{OpNum: lastOpNum + 1, ViewNum: 4}},
			lastOpNum + 1,true,lastOpNum + 1,false,
		},
		{
			lastViewNum,lastOpNum, lastOpNum + 2, []proto.Entry{{OpNum: lastOpNum + 1, ViewNum: 4}, {OpNum: lastOpNum + 2, ViewNum: 4}},
			lastOpNum + 2,true, lastOpNum + 2, false,
		},
		{
			lastViewNum - 1,lastOpNum - 1,lastOpNum,[]proto.Entry{{OpNum: lastOpNum, ViewNum: 4}},
			lastOpNum,true,lastOpNum,false,
		},
		{
			lastViewNum - 2, lastOpNum - 2, lastOpNum, []proto.Entry{{OpNum: lastOpNum - 1, ViewNum: 4}},
			lastOpNum - 1, true, lastOpNum - 1, false,
		},
		{
			lastViewNum - 3,lastOpNum - 3,lastOpNum,[]proto.Entry{{OpNum: lastOpNum - 2, ViewNum: 4}},
			lastOpNum - 2,true,lastOpNum - 2,true,
		},
		{
			lastViewNum - 2,lastOpNum - 2,lastOpNum,[]proto.Entry{{OpNum: lastOpNum - 1, ViewNum: 4}, {OpNum: lastOpNum, ViewNum: 4}},
			lastOpNum,true,lastOpNum,false,
		},
	}
	for i, test := range cases {
		opLog := newOpLog(NewStore())
		opLog.append(prevEntries...)
		opLog.commitNum = commitNum
		func() {
			defer func() {
				if r := recover(); r != nil {
					if test.expPanic != true {
						t.Errorf("%d: panic = %v, expected %v", i, true, test.expPanic)
					}
				}
			}()
			rvLastOpNum, rvAppend := opLog.tryAppend(test.opNum, test.logViewNum, test.commitNum, test.entries...)
			vCommitNum := opLog.commitNum
			if rvLastOpNum != test.expLastOpNum {
				t.Errorf("#%d: last op-number = %d, expected %d", i, rvLastOpNum, test.expLastOpNum)
			}
			if rvAppend != test.expAppend {
				t.Errorf("#%d: append = %v, expected %v", i, rvAppend, test.expAppend)
			}
			if vCommitNum != test.expCommitNum {
				t.Errorf("#%d: commit-number = %d, expected %d", i, vCommitNum, test.expCommitNum)
			}
			if rvAppend && len(test.entries) != 0 {
				rvEntries := opLog.subset(opLog.lastOpNum()-uint64(len(test.entries))+1, opLog.lastOpNum()+1)
				if !reflect.DeepEqual(test.entries, rvEntries) {
					t.Errorf("%d: appended entries = %v, expected %v", i, rvEntries, test.entries)
				}
			}
		}()
	}
}

func TestNextEntries(t *testing.T) {
	appliedState := proto.AppliedState{
		Applied: proto.Applied{ViewNum: 1, OpNum: 3},
	}
	entries := []proto.Entry{
		{ViewNum: 1, OpNum: 4},
		{ViewNum: 1, OpNum: 5},
		{ViewNum: 1, OpNum: 6},
	}
	cases := []struct {
		appliedNum uint64
		expEntries []proto.Entry
	}{
		{0,entries[:2]},
		{3,entries[:2]},
		{4,entries[1:2]},
		{5,nil},
	}
	for i, test := range cases {
		store := NewStore()
		store.SetAppliedState(appliedState)
		opLog := newOpLog(store)
		opLog.append(entries...)
		opLog.tryCommit(5, 1)
		opLog.appliedTo(test.appliedNum)
		entryList := opLog.safeEntries()
		if !reflect.DeepEqual(entryList, test.expEntries) {
			t.Errorf("#%d: entry list = %+v, expected %+v", i, entryList, test.expEntries)
		}
	}
}

func TestUnsafeEntries(t *testing.T) {
	prevEntries := []proto.Entry{{ViewNum: 1, OpNum: 1}, {ViewNum: 2, OpNum: 2}}
	cases := []struct {
		unsafe     uint64
		expEntries []proto.Entry
	}{
		{3, nil},
		{1, prevEntries},
	}
	for i, test := range cases {
		store := NewStore()
		store.Append(prevEntries[:test.unsafe-1])
		log := newOpLog(store)
		log.append(prevEntries[test.unsafe-1:]...)
		entries := log.unsafeEntries()
		if l := len(entries); l > 0 {
			log.safeTo(entries[l-1].OpNum, entries[l-i].ViewNum)
		}
		if !reflect.DeepEqual(entries, test.expEntries) {
			t.Errorf("#%d: unsafe entries = %+v, expected %+v", i, entries, test.expEntries)
		}
		w := prevEntries[len(prevEntries)-1].OpNum + 1
		if g := log.unsafe.offset; g != w {
			t.Errorf("#%d: unsafe = %d, expected %d", i, g, w)
		}
	}
}

func TestCommitTo(t *testing.T) {
	prevEntries := []proto.Entry{{ViewNum: 1, OpNum: 1}, {ViewNum: 2, OpNum: 2}, {ViewNum: 3, OpNum: 3}}
	commitNum := uint64(2)
	cases := []struct {
		commitNum    uint64
		expCommitNum uint64
		expPanic     bool
	}{
		{3,3,false},
		{1,2,false},
		{4,0,true},
	}
	for i, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if test.expPanic != true {
						t.Errorf("%d: panic = %v, expected %v", i, true, test.expPanic)
					}
				}
			}()
			log := newOpLog(NewStore())
			log.append(prevEntries...)
			log.commitNum = commitNum
			log.commitTo(test.commitNum)
			if log.commitNum != test.expCommitNum {
				t.Errorf("#%d: commit-number = %d, expected %d", i, log.commitNum, test.expCommitNum)
			}
		}()
	}
}

func TestSafeTo(t *testing.T) {
	cases := []struct {
		safeOpNum   uint64
		safeViewNum uint64
		expUnsafe   uint64
	}{
		{1,1,2},
		{2,2,3},
		{2,1,1},
		{3,1,1},
	}
	for i, test := range cases {
		log := newOpLog(NewStore())
		log.append([]proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 2}}...)
		log.safeTo(test.safeOpNum, test.safeViewNum)
		if log.unsafe.offset != test.expUnsafe {
			t.Errorf("#%d: unsafe = %d, expected %d", i, log.unsafe.offset, test.expUnsafe)
		}
	}
}

func TestSafeToWithAppliedState(t *testing.T) {
	appliedStateOpNum, appliedStateViewNum := uint64(5), uint64(2)
	appliedState := proto.AppliedState{Applied: proto.Applied{OpNum: appliedStateOpNum, ViewNum: appliedStateViewNum}}
	cases := []struct {
		safeOpNum   uint64
		safeViewNum uint64
		newEntries  []proto.Entry
		expUnsafe   uint64
	}{
		{appliedStateOpNum + 1,appliedStateViewNum,nil,appliedStateOpNum + 1},
		{appliedStateOpNum,appliedStateViewNum,nil,appliedStateOpNum + 1},
		{appliedStateOpNum - 1,appliedStateViewNum,nil,appliedStateOpNum + 1},
		{appliedStateOpNum + 1,appliedStateViewNum + 1,nil,appliedStateOpNum + 1},
		{appliedStateOpNum,appliedStateViewNum + 1,nil,appliedStateOpNum + 1},
		{appliedStateOpNum - 1,appliedStateViewNum + 1,nil,appliedStateOpNum + 1},
		{appliedStateOpNum + 1,appliedStateViewNum,[]proto.Entry{{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}},appliedStateOpNum + 2},
		{appliedStateOpNum,appliedStateViewNum,[]proto.Entry{{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}},appliedStateOpNum + 1},
		{appliedStateOpNum - 1,appliedStateViewNum,[]proto.Entry{{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}},appliedStateOpNum + 1},
		{appliedStateOpNum + 1,appliedStateViewNum + 1,[]proto.Entry{{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}},appliedStateOpNum + 1},
		{appliedStateOpNum,appliedStateViewNum + 1,[]proto.Entry{{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}},appliedStateOpNum + 1},
		{appliedStateOpNum - 1,appliedStateViewNum + 1,[]proto.Entry{{OpNum: appliedStateOpNum + 1, ViewNum: appliedStateViewNum}},appliedStateOpNum + 1},
	}
	for i, test := range cases {
		store := NewStore()
		store.SetAppliedState(appliedState)
		log := newOpLog(store)
		log.append(test.newEntries...)
		log.safeTo(test.safeOpNum, test.safeViewNum)
		if log.unsafe.offset != test.expUnsafe {
			t.Errorf("#%d: unsafe = %d, expected %d", i, log.unsafe.offset, test.expUnsafe)
		}
	}
}

func TestArchive(t *testing.T) {
	cases := []struct {
		lastOpNum  uint64
		archive    []uint64
		expOverage []int
		expAllow   bool
	}{
		{1000,[]uint64{1001},[]int{-1},false},
		{1000,[]uint64{300, 500, 800, 900},[]int{700, 500, 200, 100},true},
		{1000,[]uint64{300, 299},[]int{700, -1},false},
	}
	for i, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if test.expAllow == true {
						t.Errorf("case %d: allow = %v, expected %v: %v", i, false, true, r)
					}
				}
			}()
			store := NewStore()
			for num := uint64(1); num <= test.lastOpNum; num++ {
				store.Append([]proto.Entry{{OpNum: num}})
			}
			opLog := newOpLog(store)
			opLog.tryCommit(test.lastOpNum, 0)
			opLog.appliedTo(opLog.commitNum)
			for j := 0; j < len(test.archive); j++ {
				err := store.Archive(test.archive[j])
				if err != nil {
					if test.expAllow {
						t.Errorf("case %d.%d allow = %t, expected %t", i, j, false, test.expAllow)
					}
					continue
				}
				if len(opLog.totalEntries()) != test.expOverage[j] {
					t.Errorf("case %d.%d len = %d, expected %d", i, j, len(opLog.totalEntries()), test.expOverage[j])
				}
			}
		}()
	}
}

func TestArchiveDisorder(t *testing.T) {
	var i uint64
	lastOpNum := uint64(1000)
	unsafeOpNum := uint64(750)
	lastViewNum := lastOpNum
	store := NewStore()
	for i = 1; i <= unsafeOpNum; i++ {
		store.Append([]proto.Entry{{ViewNum: uint64(i), OpNum: uint64(i)}})
	}
	opLog := newOpLog(store)
	for i = unsafeOpNum; i < lastOpNum; i++ {
		opLog.append(proto.Entry{ViewNum: uint64(i + 1), OpNum: uint64(i + 1)})
	}
	ok := opLog.tryCommit(lastOpNum, lastViewNum)
	if !ok {
		t.Fatalf("try commit returned false")
	}
	opLog.appliedTo(opLog.commitNum)
	offset := uint64(500)
	store.Archive(offset)
	if opLog.lastOpNum() != lastOpNum {
		t.Errorf("last op-number = %d, expected %d", opLog.lastOpNum(), lastOpNum)
	}
	for j := offset; j <= opLog.lastOpNum(); j++ {
		if opLog.viewNum(j) != j {
			t.Errorf("view-number(%d) = %d, expected %d", j, opLog.viewNum(j), j)
		}
	}
	for j := offset; j <= opLog.lastOpNum(); j++ {
		if !opLog.checkNum(j, j) {
			t.Errorf("check view-number(%d) = false, expected true", j)
		}
	}
	unsafeEntries := opLog.unsafeEntries()
	if l := len(unsafeEntries); l != 250 {
		t.Errorf("unsafe entries length = %d, expected = %d", l, 250)
	}
	if unsafeEntries[0].OpNum != 751 {
		t.Errorf("op-number = %d, expected = %d", unsafeEntries[0].OpNum, 751)
	}
	prev := opLog.lastOpNum()
	opLog.append(proto.Entry{OpNum: opLog.lastOpNum() + 1, ViewNum: opLog.lastOpNum() + 1})
	if opLog.lastOpNum() != prev+1 {
		t.Errorf("last op-number = %d, expected = %d", opLog.lastOpNum(), prev+1)
	}
	entries := opLog.entries(opLog.lastOpNum())
	if len(entries) != 1 {
		t.Errorf("entries length = %d, expected = %d", len(entries), 1)
	}
}

func TestLogStore(t *testing.T) {
	opNum, viewNum := uint64(1000), uint64(1000)
	applied := proto.Applied{OpNum: opNum, ViewNum: viewNum}
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied: applied})
	log := newOpLog(store)
	if len(log.totalEntries()) != 0 {
		t.Errorf("len = %d, expected 0", len(log.totalEntries()))
	}
	if log.commitNum != opNum {
		t.Errorf("commit-number = %d, expected %d", log.commitNum, opNum)
	}
	if log.unsafe.offset != opNum+1 {
		t.Errorf("unsafe = %v, expected %d", log.unsafe, opNum+1)
	}
	if log.viewNum(opNum) != viewNum {
		t.Errorf("view-number = %d, expected %d", log.viewNum(opNum), viewNum)
	}
}

func TestIsOutOfBounds(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied:proto.Applied{OpNum: offset}})
	opLog := newOpLog(store)
	for i := uint64(1); i <= num; i++ {
		opLog.append(proto.Entry{OpNum: i + offset})
	}
	start := offset + 1
	cases := []struct {
		low, up  uint64
		expPanic bool
	}{
		{start - 2,start + 1,true },
		{start - 1,start + 1,true },
		{start,start,false },
		{start + num/2,start + num/2,false },
		{start + num - 1,start + num - 1,false },
		{start + num,start + num,false },
		{start + num,start + num + 1,true },
		{start + num + 1,start + num + 1,true },
	}
	for i, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !test.expPanic {
						t.Errorf("%d: panic = %v, expected %v: %v", i, true, false, r)
					}
				}
			}()
			opLog.mustInspectionOverflow(test.low, test.up)
			if test.expPanic {
				t.Errorf("%d: panic = %v, expected %v", i, false, true)
			}
		}()
	}
}

func TestViewNum(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied:proto.Applied{OpNum: offset, ViewNum: 1}})
	l := newOpLog(store)
	for i = 1; i < num; i++ {
		l.append(proto.Entry{OpNum: offset + i, ViewNum: i})
	}
	cases := []struct {
		opNum uint64
		exp   uint64
	}{
		{offset - 1,0},
		{offset, 1},
		{offset + num/2,num / 2},
		{offset + num - 1,num - 1},
		{offset + num,0},
	}
	for j, test := range cases {
		viewNum := l.viewNum(test.opNum)
		if !reflect.DeepEqual(viewNum, test.exp) {
			t.Errorf("#%d: at = %d, expected %d", j, viewNum, test.exp)
		}
	}
}

func TestViewNumWithUnsafeAppliedState(t *testing.T) {
	storeAppliedStateOpNum := uint64(100)
	unsafeAppliedStateOpNum := storeAppliedStateOpNum + 5
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied:proto.Applied{OpNum: storeAppliedStateOpNum, ViewNum: 1}})
	opLog := newOpLog(store)
	opLog.recover(proto.AppliedState{Applied:proto.Applied{OpNum: unsafeAppliedStateOpNum, ViewNum: 1}})
	cases := []struct {
		opNum uint64
		exp   uint64
	}{
		{storeAppliedStateOpNum,0},
		{storeAppliedStateOpNum + 1,0},
		{unsafeAppliedStateOpNum - 1,0},
		{unsafeAppliedStateOpNum,1},
	}
	for i, test := range cases {
		viewNum := opLog.viewNum(test.opNum)
		if !reflect.DeepEqual(viewNum, test.exp) {
			t.Errorf("#%d: at = %d, expected %d", i, viewNum, test.exp)
		}
	}
}

func TestSeek(t *testing.T) {
	var i uint64
	offset := uint64(100)
	num := uint64(100)
	store := NewStore()
	store.SetAppliedState(proto.AppliedState{Applied:proto.Applied{OpNum: offset}})
	opLog := newOpLog(store)
	for i = 1; i < num; i++ {
		opLog.append(proto.Entry{OpNum: offset + i, ViewNum: offset + i})
	}
	cases := []struct {
		from     uint64
		to       uint64
		exp      []proto.Entry
		expPanic bool
	}{
		{offset - 1, offset + 1, nil, true},
		{offset, offset + 1, nil, true},
		{offset + num/2, offset + num/2 + 1, []proto.Entry{{OpNum: offset + num/2, ViewNum: offset + num/2}}, false},
		{offset + num - 1, offset + num, []proto.Entry{{OpNum: offset + num - 1, ViewNum: offset + num - 1}}, false},
		{offset + num, offset + num + 1, nil, true},
	}
	for j, test := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !test.expPanic {
						t.Errorf("%d: panic = %v, expected %v: %v", j, true, false, r)
					}
				}
			}()
			rv := opLog.subset(test.from, test.to)
			if !reflect.DeepEqual(rv, test.exp) {
				t.Errorf("#%d: from %d to %d = %v, expected %v", j, test.from, test.to, rv, test.exp)
			}
		}()
	}
}

func TestScanCollision(t *testing.T) {
	presetEntries := []proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 2}, {OpNum: 3, ViewNum: 3}}
	cases := []struct {
		entries      []proto.Entry
		expCollision uint64
	}{
		{[]proto.Entry{},0},
		{[]proto.Entry{},0},
		{[]proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 2}, {OpNum: 3, ViewNum: 3}},0},
		{[]proto.Entry{{OpNum: 2, ViewNum: 2}, {OpNum: 3, ViewNum: 3}}, 0},
		{[]proto.Entry{{OpNum: 3, ViewNum: 3}}, 0},
		{[]proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 2}, {OpNum: 3, ViewNum: 3}, {OpNum: 4, ViewNum: 4}, {OpNum: 5, ViewNum: 4}},4},
		{[]proto.Entry{{OpNum: 2, ViewNum: 2}, {OpNum: 3, ViewNum: 3}, {OpNum: 4, ViewNum: 4}, {OpNum: 5, ViewNum: 4}}, 4},
		{[]proto.Entry{{OpNum: 3, ViewNum: 3}, {OpNum: 4, ViewNum: 4}, {OpNum: 5, ViewNum: 4}},4},
		{[]proto.Entry{{OpNum: 4, ViewNum: 4}, {OpNum: 5, ViewNum: 4}}, 4},
		{[]proto.Entry{{OpNum: 1, ViewNum: 4}, {OpNum: 2, ViewNum: 4}}, 1},
		{[]proto.Entry{{OpNum: 2, ViewNum: 1}, {OpNum: 3, ViewNum: 4}, {OpNum: 4, ViewNum: 4}}, 2},
		{[]proto.Entry{{OpNum: 3, ViewNum: 1}, {OpNum: 4, ViewNum: 2}, {OpNum: 5, ViewNum: 4}, {OpNum: 6, ViewNum: 4}},3},
	}
	for i, test := range cases {
		log := newOpLog(NewStore())
		log.append(presetEntries...)
		collisionPos := log.scanCollision(test.entries)
		if collisionPos != test.expCollision {
			t.Errorf("#%d: collision = %d, expected %d", i, collisionPos, test.expCollision)
		}
	}
}

func TestIsUpToDate(t *testing.T) {
	prevEntries := []proto.Entry{{OpNum: 1, ViewNum: 1}, {OpNum: 2, ViewNum: 2}, {OpNum: 3, ViewNum: 3}}
	opLog := newOpLog(NewStore())
	opLog.append(prevEntries...)
	cases := []struct {
		lastOpNum   uint64
		viewNum     uint64
		expUpToDate bool
	}{
		{opLog.lastOpNum() - 1,4,true},
		{opLog.lastOpNum(), 4,true},
		{opLog.lastOpNum() + 1,4,true},
		{opLog.lastOpNum() - 1,2,false},
		{opLog.lastOpNum(), 2,false},
		{opLog.lastOpNum() + 1,2,false},
		{opLog.lastOpNum() - 1,3,false},
		{opLog.lastOpNum(),3,true},
		{opLog.lastOpNum() + 1,3,true},
	}
	for i, test := range cases {
		rv := opLog.isUpToDate(test.lastOpNum, test.viewNum)
		if rv != test.expUpToDate {
			t.Errorf("#%d: up to date = %v, expected %v", i, rv, test.expUpToDate)
		}
	}
}