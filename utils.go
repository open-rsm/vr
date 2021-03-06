package vr

import "github.com/open-rsm/vr/proto"

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

func IsIgnorableMessage(m proto.Message) bool {
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
	return as.Applied.ViewStamp.OpNum == 0
}

func hardStateCompare(a, b proto.HardState) bool {
	return a.ViewStamp.ViewNum == b.ViewStamp.ViewNum && a.CommitNum == b.CommitNum
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