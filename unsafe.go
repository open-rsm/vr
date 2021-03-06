package vr

import (
	"log"
	"github.com/open-rsm/vr/proto"
)

// used to manage the intermediate state that has not
// yet been written to disk
type unsafe struct {
	offset       uint64               // the position of the last log currently loaded
	entries      []proto.Entry        // temporary logs that have not been wal
	appliedState *proto.AppliedState  // applied state handler
}

func (u *unsafe) subset(low uint64, up uint64) []proto.Entry {
	if low > up {
		log.Panicf("vr.unsafe: invalid unsafe.subset %d > %d", low, up)
	}
	lower := u.offset
	upper := lower + uint64(len(u.entries))
	if low < lower || up > upper {
		log.Panicf("vr.unsafe: unsafe.subset[%d,%d) overflow [%d,%d]", low, up, lower, upper)
	}
	return u.entries[low-u.offset : up-u.offset]
}

func (u *unsafe) truncateAndAppend(entries []proto.Entry) {
	ahead := entries[0].ViewStamp.OpNum - 1
	if ahead == u.offset+uint64(len(u.entries))-1 {
		u.entries = append(u.entries, entries...)
	} else if ahead < u.offset {
		log.Printf("vr.unsafe: replace the unsafe entries from number %d", ahead+1)
		u.offset = ahead + 1
		u.entries = entries
	} else {
		log.Printf("vr.unsafe: truncate the unsafe entries to number %d", ahead)
		u.entries = append([]proto.Entry{}, u.subset(u.offset, ahead+1)...)
		u.entries = append(u.entries, entries...)
	}
}

func (u *unsafe) tryGetStartOpNum() (uint64, bool) {
	if as := u.appliedState; as != nil {
		return as.Applied.ViewStamp.OpNum + 1, true
	}
	return 0, false
}

func (u *unsafe) tryGetLastOpNum() (uint64, bool) {
	if el := len(u.entries); el != 0 {
		return u.offset + uint64(el) - 1, true
	}
	if as := u.appliedState; as != nil {
		return as.Applied.ViewStamp.OpNum, true
	}
	return 0, false
}

func (u *unsafe) tryGetViewNum(num uint64) (uint64, bool) {
	if num < u.offset {
		if u.appliedState == nil {
			return 0, false
		}
		if applied := u.appliedState.Applied; applied.ViewStamp.OpNum == num {
			return applied.ViewStamp.ViewNum, true
		}
		return 0, false
	}
	last, ok := u.tryGetLastOpNum()
	if !ok {
		return 0, false
	}
	if num > last {
		return 0, false
	}
	return u.entries[num-u.offset].ViewStamp.ViewNum, true
}

func (u *unsafe) safeTo(on, vn uint64) {
	this, ok := u.tryGetViewNum(on)
	if !ok {
		return
	}
	if this == vn && on >= u.offset {
		u.entries = u.entries[on+1-u.offset:]
		u.offset = on + 1
	}
}

func (u *unsafe) safeAppliedStateTo(num uint64) {
	if u.appliedState != nil && u.appliedState.Applied.ViewStamp.OpNum == num {
		u.appliedState = nil
	}
}

func (u *unsafe) recover(state proto.AppliedState) {
	u.entries = nil
	u.offset = state.Applied.ViewStamp.OpNum + 1
	u.appliedState = &state
}