package group

import "github.com/open-rsm/vr/group/progress"

const (
	Normal = iota
	WarmedUp
)

type Replica struct {
	*progress.Progress
	// used to represent the progress status of the
	// current Progress
	Status   int
}

func newReplica() *Replica {
	r := Replica{
		Progress: progress.New(),
		Status: Normal,
	}
	return &r
}

func (r *Replica) progress() uint64 {
	return r.Ack
}