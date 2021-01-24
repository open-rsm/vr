package vr

import (
	"fmt"
	"sort"
)

type Windows struct {
	replicas map[uint64]*window
	warms    map[uint64]*window
}

func CreateWindows(peers []uint64) *Windows {
	w := &Windows{
		replicas: make(map[uint64]*window),
	}
	for _, peer := range peers {
		w.replicas[peer] = newWindow()
	}
	return w
}

func (w *Windows) IndexOf(i uint64) *window {
	if r, ok := w.replicas[i]; ok {
		return r
	}
	//TODO: panic?
	return nil
}

func (w *Windows) Exist(i uint64) bool {
	if _, ok := w.replicas[i]; ok {
		return true
	}
	return false
}

func (w *Windows) Len() int {
	return len(w.replicas)
}

func (w *Windows) Progress() uint64s {
	nums := make(uint64s, 0, w.Len())
	for i := range w.replicas {
		nums = append(nums, w.replicas[i].Ack)
	}
	return nums
}

func (w *Windows) Reset(opNum, replicaNum uint64) {
	for num := range w.replicas {
		w.replicas[num] = &window{Next: opNum + 1}
		if num == replicaNum {
			w.replicas[num].Ack = opNum
		}
	}
}

func (w *Windows) List() map[uint64]*window {
	return w.replicas
}

func (w *Windows) Replicas() []uint64 {
	replicas := make([]uint64, 0, w.Len())
	for window := range w.List() {
		replicas = append(replicas, window)
	}
	sort.Sort(uint64s(replicas))
	return replicas
}

func (w *Windows) Set(num, offset, next uint64) {
	w.replicas[num] = &window{Next: next, Ack: offset}
}

func (w *Windows) Del(num uint64) {
	delete(w.replicas, num)
}

// control and manage the current sync replicas and status
// for a replica
type window struct {
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
		Next: One,
	}
}

func (w *window) delaySet(d int) {
	w.Delay = d
}

func (w *window) delayReset() {
	w.Delay = 0
}

func (w *window) needDelay() bool {
	return w.Ack == 0 && w.Delay > 0
}

func (w *window) update(n uint64) {
	w.delayReset()
	if w.Ack < n {
		w.Ack = n
	}
	if w.Next < n + 1 {
		w.Next = n + 1
	}
}

func (w *window) niceUpdate(n uint64) {
	w.Next = n + 1
}

func (w *window) tryDecTo(ignored, last uint64) bool {
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

func (w *window) delayDec(i int) {
	w.Delay -= i
	if w.Delay < 0 {
		w.Delay = 0
	}
}

func (w *window) String() string {
	return fmt.Sprintf("next = %d, offsets = %d, delay = %v", w.Next, w.Ack, w.Delay)
}
