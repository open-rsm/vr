package vr

import (
	"fmt"
	"log"
	"github.com/open-rsm/vr/proto"
)

// operation log manager for view stamped replication
type opLog struct {
	// has not been WAL
	unsafe

	store      *Store  // store handler
	commitNum  uint64  // current committed location
	appliedNum uint64  // logs that have been applied
}

// create and initialize an object handler to manage logs
func newOpLog(store *Store) *opLog {
	if store == nil {
		log.Panic("vr.oplog: stores must not be nil")
	}
	startOpNum, err := store.StartOpNum()
	if err != nil {
		panic(err)
	}
	lastOpNum, err := store.LastOpNum()
	if err != nil {
		panic(err)
	}
	opLog := &opLog{
		store: store,
		commitNum: startOpNum - 1,
		appliedNum: startOpNum - 1,
	}
	opLog.offset = lastOpNum + 1
	return opLog
}

func (r *opLog) mustInspectionOverflow(low, up uint64) {
	if low > up {
		log.Panicf("vr.oplog: invalid subset %d > %d", low, up)
	}
	length := r.lastOpNum() - r.startOpNum() + 1
	if low < r.startOpNum() || up > r.startOpNum()+length {
		log.Panicf("vr.oplog: subset[%d,%d) overflow [%d,%d]", low, up, r.startOpNum(), r.lastOpNum())
	}
}

// search part of log entries
func (r *opLog) subset(low uint64, up uint64) []proto.Entry {
	r.mustInspectionOverflow(low, up)
	if low == up {
		return nil
	}
	var entries []proto.Entry
	if r.unsafe.offset > low {
		storedEntries, err := r.store.Subset(low, min(up, r.unsafe.offset))
		if err == ErrNotReached {
			log.Panicf("vr.oplog: entries[%d:%d) is unavailable from stores", low, min(up, r.unsafe.offset))
		} else if err != nil {
			panic(err)
		}
		entries = storedEntries
	}
	if r.unsafe.offset < up {
		unsafe := r.unsafe.subset(max(low, r.unsafe.offset), up)
		if len(entries) > 0 {
			entries = append([]proto.Entry{}, entries...)
			entries = append(entries, unsafe...)
		} else {
			entries = unsafe
		}
	}
	return entries
}

func (r *opLog) tryAppend(opNum, logNum, commitNum uint64, entries ...proto.Entry) (lastNewOpNum uint64, ok bool) {
	lastNewOpNum = opNum + uint64(len(entries))
	if r.checkNum(opNum, logNum) {
		sc := r.scanCollision(entries)
		switch {
		case sc == 0:
		case sc <= r.commitNum:
			log.Panicf("vr.oplog: entry %d collision with commit-number entry [commit-number(%d)]", sc, r.commitNum)
		default:
			offset := opNum + 1
			r.append(entries[sc-offset:]...)
		}
		r.commitTo(min(commitNum, lastNewOpNum))
		return lastNewOpNum, true
	}
	return 0, false
}

func (r *opLog) append(entries ...proto.Entry) uint64 {
	if len(entries) == 0 {
		return r.lastOpNum()
	}
	if ahead := entries[0].ViewStamp.OpNum - 1; ahead < r.commitNum {
		log.Panicf("vr.oplog: ahead(%d) is out of range [commit-number(%d)]", ahead, r.commitNum)
	}
	r.unsafe.truncateAndAppend(entries)
	return r.lastOpNum()
}

func (r *opLog) scanCollision(entries []proto.Entry) uint64 {
	for _, entry := range entries {
		if !r.checkNum(entry.ViewStamp.OpNum, entry.ViewStamp.ViewNum) {
			if entry.ViewStamp.OpNum <= r.lastOpNum() {
				log.Printf("vr.oplog: scan to collision at op-number %d [existing view-number: %d, collision view-number: %d]",
					entry.ViewStamp.OpNum, r.viewNum(entry.ViewStamp.OpNum), entry.ViewStamp.ViewNum)
			}
			return entry.ViewStamp.OpNum
		}
	}
	return 0
}

func (r *opLog) unsafeEntries() []proto.Entry {
	if len(r.unsafe.entries) == 0 {
		return nil
	}
	return r.unsafe.entries
}

func (r *opLog) safeEntries() (entries []proto.Entry) {
	num := max(r.appliedNum+1, r.startOpNum())
	if r.commitNum+1 > num {
		return r.subset(num, r.commitNum+1)
	}
	return nil
}

func (r *opLog) appliedState() (proto.AppliedState, error) {
	if r.unsafe.appliedState != nil {
		return *r.unsafe.appliedState, nil
	}
	return r.store.GetAppliedState()
}

func (r *opLog) startOpNum() uint64 {
	if num, ok := r.unsafe.tryGetStartOpNum(); ok {
		return num
	}
	num, err := r.store.StartOpNum()
	if err != nil {
		panic(err)
	}
	return num
}

func (r *opLog) lastOpNum() uint64 {
	if num, ok := r.unsafe.tryGetLastOpNum(); ok {
		return num
	}
	num, err := r.store.LastOpNum()
	if err != nil {
		panic(err)
	}
	return num
}

func (r *opLog) commitTo(commitNum uint64) {
	if r.commitNum < commitNum {
		if r.lastOpNum() < commitNum {
			log.Panicf("vr.oplog: to commit-number(%d) is out of range [last-op-number(%d)]", commitNum, r.lastOpNum())
		}
		r.commitNum = commitNum
	}
}

func (r *opLog) appliedTo(num uint64) {
	if num == 0 {
		return
	}
	if r.commitNum < num || num < r.appliedNum {
		log.Panicf("vr.oplog: applied-number(%d) is out of range [prev-applied-number(%d), commit-number(%d)]", num, r.appliedNum, r.commitNum)
	}
	r.appliedNum = num
}

func (r *opLog) safeTo(on, vn uint64) {
	r.unsafe.safeTo(on, vn)
}

func (r *opLog) safeAppliedStateTo(num uint64) {
	r.unsafe.safeAppliedStateTo(num)
}

func (r *opLog) lastViewNum() uint64 {
	return r.viewNum(r.lastOpNum())
}

func (r *opLog) viewNum(num uint64) uint64 {
	if num < (r.startOpNum()-1) || num > r.lastOpNum() {
		return 0
	}
	if vn, ok := r.unsafe.tryGetViewNum(num); ok {
		return vn
	}
	svn, err := r.store.ViewNum(num)
	if err == nil {
		return svn
	}
	panic(err)
}

func (r *opLog) entries(num uint64) []proto.Entry {
	if num > r.lastOpNum() {
		return nil
	}
	return r.subset(num, r.lastOpNum()+1)
}

func (r *opLog) totalEntries() []proto.Entry {
	return r.entries(r.startOpNum())
}

func (r *opLog) isUpToDate(lastOpNum, viewNum uint64) bool {
	return viewNum > r.lastViewNum() || (viewNum == r.lastViewNum() && lastOpNum >= r.lastOpNum())
}

func (r *opLog) checkNum(on, vn uint64) bool {
	return r.viewNum(on) == vn
}

func (r *opLog) tryCommit(maxOpNum, viewNum uint64) bool {
	if maxOpNum > r.commitNum && r.viewNum(maxOpNum) == viewNum {
		r.commitTo(maxOpNum)
		return true
	}
	return false
}

func (r *opLog) recover(state proto.AppliedState) {
	log.Printf("vr.oplog: log [%s] starts to reset applied state [op-number: %d, view-number: %d]", r, state.Applied.ViewStamp.OpNum, state.Applied.ViewStamp.ViewNum)
	r.commitNum = state.Applied.ViewStamp.OpNum
	r.unsafe.recover(state)
}

func (r *opLog) String() string {
	return fmt.Sprintf("vr.oplog: commit-number=%d, applied-number=%d, unsafe.offsets=%d, len(unsafe.persistent_entries)=%d", r.commitNum, r.appliedNum, r.unsafe.offset, len(r.unsafe.entries))
}
