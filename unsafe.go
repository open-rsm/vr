package vr

import (
	"log"
	"github.com/open-rsm/spec/proto"
)

type unsafe struct {
	appliedState *proto.AppliedState
	entries      []proto.Entry
	offset       uint64
}

func (u *unsafe) mustInspectionOverflow(low, up uint64) {
	if low > up {
		log.Panicf("vr.unsafe: invalid unsafe.subset %d > %d", low, up)
	}
	lower := u.offset
	upper := lower + uint64(len(u.entries))
	if low < lower || up > upper {
		log.Panicf("vr.unsafe: unsafe.subset[%d,%d) out of bound [%d,%d]", low, up, lower, upper)
	}
}

func (u *unsafe) seek(low uint64, up uint64) []proto.Entry {
	u.mustInspectionOverflow(low, up)
	return u.entries[low-u.offset : up-u.offset]
}

func (u *unsafe) tryGetStartOpNum() (uint64, bool) {
	if as := u.appliedState; as != nil {
		return as.Applied.OpNum + 1, true
	}
	return 0, false
}

func (u *unsafe) tryGetLastOpNum() (uint64, bool) {
	if el := len(u.entries); el != 0 {
		return u.offset + uint64(el) - 1, true
	}
	if as := u.appliedState; as != nil {
		return as.Applied.OpNum, true
	}
	return 0, false
}

func (u *unsafe) tryGetViewNum(num uint64) (uint64, bool) {
	if num < u.offset {
		if u.appliedState == nil {
			return 0, false
		}
		if applied := u.appliedState.Applied; applied.OpNum == num {
			return applied.ViewNum, true
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
	return u.entries[num-u.offset].ViewNum, true
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
	if u.appliedState != nil && u.appliedState.Applied.OpNum == num {
		u.appliedState = nil
	}
}

func (u *unsafe) recover(state proto.AppliedState) {
	u.entries = nil
	u.offset = state.Applied.OpNum + 1
	u.appliedState = &state
}

func (u *unsafe) truncateAndAppend(entries []proto.Entry) {
	ahead := entries[0].OpNum - 1
	if ahead == u.offset+uint64(len(u.entries))-1 {
		u.entries = append(u.entries, entries...)
	} else if ahead < u.offset {
		log.Printf("vr.unsafe: replace the unsafe entries from number %d", ahead+1)
		u.offset = ahead + 1
		u.entries = entries
	} else {
		log.Printf("vr.unsafe: truncate the unsafe entries to number %d", ahead)
		u.entries = append([]proto.Entry{}, u.seek(u.offset, ahead+1)...)
		u.entries = append(u.entries, entries...)
	}
}