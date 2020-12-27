package vr

const (
	RoundRobin = iota
)

type pickFn func(uint64, map[uint64]*Window, ... func()) uint64

var pickers = []pickFn{
	roundRobin,
}

func roundRobin(num uint64, ws map[uint64]*Window, _... func()) uint64 {
	if n := num % uint64(len(ws)); n != 0 {
		return n
	}
	return 1
}
