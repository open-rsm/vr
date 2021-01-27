package progress

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