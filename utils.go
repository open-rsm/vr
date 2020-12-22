package vr

import "github.com/open-rsm/spec/proto"

type uint64s []uint64

func (r uint64s) Len() int {
	return len(r)
}

func (r uint64s) Less(i, j int) bool {
	return r[i] < r[j]
}

func (r uint64s) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func IsLocalMessage(m proto.Message) bool {
	return m.Type == proto.Change || m.Type == proto.Heartbeat
}

func IsReplyMessage(m proto.Message) bool {
	return m.Type == proto.Reply || m.Type == proto.PrepareOk
}

func IsHardStateEqual(a, b proto.HardState) bool {
	return hardStateCompare(a, b)
}

func IsInvalidHardState(hs proto.HardState) bool {
	return IsHardStateEqual(hs, nilHardState)
}

func IsInvalidAppliedState(as proto.AppliedState) bool {
	return as.Applied.OpNum == 0
}

func hardStateCompare(a, b proto.HardState) bool {
	return a.ViewNum == b.ViewNum && a.CommitNum == b.CommitNum
}

func limitSize(entries []proto.Entry, maxSize uint64) []proto.Entry {
	if len(entries) == 0 {
		return entries
	}
	size := len(entries[0].String())
	var limit int
	for limit = 1; limit < len(entries); limit++ {
		size += len(entries[limit].String())
		if uint64(size) > maxSize {
			break
		}
	}
	return entries[:limit]
}