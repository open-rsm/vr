package group

import (
	"sort"
	"github.com/open-rsm/vr/group/window"
)

type Progress struct {
	windows map[uint64]*window.Window
	warms   map[uint64]*window.Window
}

func newProgress(peers []uint64) Progress {
	p := Progress{
		windows: make(map[uint64]*window.Window),
	}
	for _, peer := range peers {
		p.windows[peer] = window.New()
	}
	return p
}

func (w *Progress) IndexOf(i uint64) *window.Window {
	if r, ok := w.windows[i]; ok {
		return r
	}
	//TODO: panic?
	return nil
}

func (w *Progress) Exist(i uint64) bool {
	if _, ok := w.windows[i]; ok {
		return true
	}
	return false
}

func (w *Progress) Windows() int {
	return len(w.windows)
}

func (w *Progress) Progress() uint64s {
	nums := make(uint64s, 0, w.Windows())
	for i := range w.windows {
		nums = append(nums, w.windows[i].Ack)
	}
	return nums
}

func (w *Progress) Reset(opNum, replicaNum uint64) {
	for num := range w.windows {
		w.windows[num] = &window.Window{Next: opNum + 1}
		if num == replicaNum {
			w.windows[num].Ack = opNum
		}
	}
}

func (w *Progress) List() map[uint64]*window.Window {
	return w.windows
}

func (w *Progress) Replicas() []uint64 {
	replicas := make([]uint64, 0, w.Windows())
	for window := range w.List() {
		replicas = append(replicas, window)
	}
	sort.Sort(uint64s(replicas))
	return replicas
}

func (w *Progress) Set(num, offset, next uint64) {
	w.windows[num] = &window.Window{Next: next, Ack: offset}
}

func (w *Progress) Del(num uint64) {
	delete(w.windows, num)
}