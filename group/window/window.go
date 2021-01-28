package window

import "fmt"

const (
	Normal = iota
	Fast
	Slow
)

// control and manage the current sync windows and status
// for a replica
type Window struct {
	// used to represent the progress status of the
	// current Window
	Status int
	// the confirmed sync position of all nodes in the
	// replication group
	Ack   uint64
	// if data sync is currently not possible, how long
	// do I need to wait?
	Delay int
	// record the next location that needs to be sync
	Next  uint64
}

func New() *Window {
	return &Window{
		Next: 1,
	}
}

func (w *Window) DelaySet(d int) {
	w.Delay = d
}

func (w *Window) DelayReset() {
	w.Delay = 0
}

func (w *Window) NeedDelay() bool {
	return w.Ack == 0 && w.Delay > 0
}

func (w *Window) Update(n uint64) {
	w.DelayReset()
	if w.Ack < n {
		w.Ack = n
	}
	if w.Next < n + 1 {
		w.Next = n + 1
	}
}

func (w *Window) NiceUpdate(n uint64) {
	w.Next = n + 1
}

func (w *Window) TryDecTo(ignored, last uint64) bool {
	w.DelayReset()
	if w.Ack != 0 {
		if ignored <= w.Ack {
			return false
		}
		w.Next = w.Ack + 1
		return true
	}
	if w.Next-1 != ignored {
		return false
	}
	if w.Next = min(ignored, last+1); w.Next < 1 {
		w.Next = 1
	}
	return true
}

func (w *Window) DelayDec(i int) {
	w.Delay -= i
	if w.Delay < 0 {
		w.Delay = 0
	}
}

func (w *Window) DelayInc(i int) {
}

func (w *Window) String() string {
	return fmt.Sprintf("next = %d, offsets = %d, delay = %v", w.Next, w.Ack, w.Delay)
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}