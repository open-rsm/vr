package progress

import "sort"

type uint64s []uint64

func (r uint64s) Len() int {
	return len(r)
}

func (r uint64s) Less(i, j int) bool {
	return r[i] < r[j]
}

func (r uint64s) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type Progress struct {
	windows map[uint64]*window
	warms   map[uint64]*window
}

func New(peers []uint64) *Progress {
	p := &Progress{
		windows: make(map[uint64]*window),
	}
	for _, peer := range peers {
		p.windows[peer] = newWindow()
	}
	return p
}

func (w *Progress) IndexOf(i uint64) *window {
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

func (w *Progress) Len() int {
	return len(w.windows)
}

func (w *Progress) Progress() uint64s {
	nums := make(uint64s, 0, w.Len())
	for i := range w.windows {
		nums = append(nums, w.windows[i].Ack)
	}
	return nums
}

func (w *Progress) Reset(opNum, replicaNum uint64) {
	for num := range w.windows {
		w.windows[num] = &window{Next: opNum + 1}
		if num == replicaNum {
			w.windows[num].Ack = opNum
		}
	}
}

func (w *Progress) List() map[uint64]*window {
	return w.windows
}

func (w *Progress) Replicas() []uint64 {
	replicas := make([]uint64, 0, w.Len())
	for window := range w.List() {
		replicas = append(replicas, window)
	}
	sort.Sort(uint64s(replicas))
	return replicas
}

func (w *Progress) Set(num, offset, next uint64) {
	w.windows[num] = &window{Next: next, Ack: offset}
}

func (w *Progress) Del(num uint64) {
	delete(w.windows, num)
}