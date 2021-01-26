package vr

import "github.com/open-rsm/vr/proto"

const (
	RoundRobin = iota
)

// This paper provides round-robin (section 4, figure 2) as the primary
// selection algorithm. From the engineering point of view, we default to
// adopt the same scheme as the article, but the selection algorithm is
// open to discussion, so in the aspect of engineering implementation,
// it gives engineers more space to play and imagine.
type selectFn func(vs proto.ViewStamp, windows int, f ... func()) uint64

var selectors = []selectFn{
	roundRobin,
}

// The primary is chosen round-robin, starting with replica 1, as the
// system moves to new views.
func roundRobin(vs proto.ViewStamp, windows int, _... func()) uint64 {
	if n := vs.ViewNum % uint64(windows); n != 0 {
		return n
	}
	return 1
}

func isInvalidSelector(num int) bool {
	if 0 <= num && num < len(selectors) {
		return true
	}
	return false
}

func loadSelector(sf *selectFn, num int) {
	*sf = selectors[num]
}
