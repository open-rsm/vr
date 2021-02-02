package progress

import "fmt"

// control and manage the current sync windows and status
// for a replica
type Progress struct {
	// the confirmed sync position of all nodes in the
	// replication group
	Ack   uint64
	// if data sync is currently not possible, how long
	// do I need to wait?
	Delay int
	// record the next location that needs to be sync
	Next  uint64
}

func New() *Progress {
	return &Progress{
		Next: 1,
	}
}

func (p *Progress) DelaySet(d int) {
	p.Delay = d
}

func (p *Progress) DelayReset() {
	p.Delay = 0
}

func (p *Progress) NeedDelay() bool {
	return p.Ack == 0 && p.Delay > 0
}

func (p *Progress) Update(n uint64) {
	p.DelayReset()
	if p.Ack < n {
		p.Ack = n
	}
	if p.Next < n + 1 {
		p.Next = n + 1
	}
}

func (p *Progress) NiceUpdate(n uint64) {
	p.Next = n + 1
}

func (p *Progress) TryDecTo(ignored, last uint64) bool {
	p.DelayReset()
	if p.Ack != 0 {
		if ignored <= p.Ack {
			return false
		}
		p.Next = p.Ack + 1
		return true
	}
	if p.Next-1 != ignored {
		return false
	}
	if p.Next = min(ignored, last+1); p.Next < 1 {
		p.Next = 1
	}
	return true
}

func (p *Progress) DelayDec(i int) {
	p.Delay -= i
	if p.Delay < 0 {
		p.Delay = 0
	}
}

func (p *Progress) DelayInc(i int) {
}

func (p *Progress) String() string {
	return fmt.Sprintf("next = %d, offsets = %d, delay = %v", p.Next, p.Ack, p.Delay)
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}