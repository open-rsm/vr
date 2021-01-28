package group

import "sort"

type Group struct {
	Progress
}

func New(replicas []uint64) *Group {
	return &Group{
		Progress: newProgress(replicas),
	}
}

// This implies that each step of the protocol must be processed
// by f + 1 replicas. These f + 1 together with the f that may
// not respond give us the smallest group size of 2f + 1
func (g *Group) Smallest() int {
	return group(1)
}

func (g *Group) Faulty() int {
	return g.Progress.Windows()/2
}

// the quorum of replicas that processes a particular step
// of the protocol must have a non-empty intersection with
// the group of replicas available to handle the next step,
// since this way we can ensure that at each next step at
// least one participant knows what happened in the previous
// step. In a group of 2f + 1 replicas, f + 1 is the smallest
// quorum size that will work.
func (g *Group) Quorum() int {
	return g.Faulty() + 1
}

func (g *Group) Commit() uint64 {
	nums := g.Progress.Progress()
	sort.Sort(sort.Reverse(nums))
	return nums[g.Quorum()-1]
}

func group(f int) int {
	if f < 1 {
		panic("f must be greater than or equal to 1")
	}
	return 2*f + 1
}