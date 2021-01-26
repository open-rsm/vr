package progress

import "fmt"

const (
	Normal = iota
	Fast
	Slow
)

// control and manage the current sync windows and status
// for a replica
type window struct {
	// used to represent the progress status of the
	// current window
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

func newWindow() *window {
	return &window{
		Next: 1,
	}
}

func (w *window) DelaySet(d int) {
	w.Delay = d
}

func (w *window) DelayReset() {
	w.Delay = 0
}

func (w *window) NeedDelay() bool {
	return w.Ack == 0 && w.Delay > 0
}

func (w *window) Update(n uint64) {
	w.DelayReset()
	if w.Ack < n {
		w.Ack = n
	}
	if w.Next < n + 1 {
		w.Next = n + 1
	}
}

func (w *window) NiceUpdate(n uint64) {
	w.Next = n + 1
}

func (w *window) TryDecTo(ignored, last uint64) bool {
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

func (w *window) DelayDec(i int) {
	w.Delay -= i
	if w.Delay < 0 {
		w.Delay = 0
	}
}

func (w *window) DelayInc(i int) {
}

func (w *window) String() string {
	return fmt.Sprintf("next = %d, offsets = %d, delay = %v", w.Next, w.Ack, w.Delay)
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}