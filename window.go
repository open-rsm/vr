package vr

import "fmt"

type Window struct {
	Ack   uint64
	Delay int
	Next  uint64
}

func (w *Window) delaySet(d int) {
	w.Delay = d
}

func (w *Window) delayReset() {
	w.Delay = 0
}

func (w *Window) needDelay() bool {
	return w.Ack == 0 && w.Delay > 0
}

func (w *Window) update(n uint64) {
	w.delayReset()
	if w.Ack < n {
		w.Ack = n
	}
	if w.Next < n + 1 {
		w.Next = n + 1
	}
}

func (w *Window) niceUpdate(n uint64) {
	w.Next = n + 1
}

func (w *Window) tryDecTo(ignored, last uint64) bool {
	w.delayReset()
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

func (w *Window) delayDec(i int) {
	w.Delay -= i
	if w.Delay < 0 {
		w.Delay = 0
	}
}

func (w *Window) String() string {
	return fmt.Sprintf("next = %d, offsets = %d, delay = %v", w.Next, w.Ack, w.Delay)
}