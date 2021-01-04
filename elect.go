package vr

const (
	RoundRobin = iota
)

type electFn func(uint64, map[uint64]*Window, ... func()) uint64

var electors = []electFn{
	roundRobin,
}

func roundRobin(num uint64, ws map[uint64]*Window, _... func()) uint64 {
	if n := num % uint64(len(ws)); n != 0 {
		return n
	}
	return 1
}

func isInvalidElector(num int) bool {
	if 0 <= num && num < len(electors) {
		return true
	}
	return false
}

func loadElector(ef *electFn, num int) {
	*ef = electors[num]
}
