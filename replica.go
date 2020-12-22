package vr

import (
	"log"
	"errors"
	"context"
	"github.com/open-rsm/spec/proto"
)

var (
	nilHardState = proto.HardState{}
	ErrStopped   = errors.New("vr.replica: replicator stopped")
)

type Option struct {
	Action int
	Filter func()
}

type Replicator interface {
	Advance()
	Change(ctx context.Context) error
	Clock()
	Ready(...Option) <-chan Ready
	Reconfiguration(context.Context, proto.Configuration) *proto.ConfigurationState
	Step(context.Context, proto.Message) error
	Status() Status
	Stop()
}

type Peer struct {
	ID      uint64
	Context []byte
}

func StartReplica(c *Config, peers []Peer) Replicator {
	rc := newReplica()
	vr := newVR(c)
	vr.becomeBackup(1, None)
	vr.opLog.commitNum = vr.opLog.lastOpNum()
	vr.CommitNum = vr.opLog.commitNum
	for _, peer := range peers {
		vr.createReplicator(peer.ID)
	}
	go rc.run(vr)
	return rc
}

func RestartReplica(c *Config) Replicator {
	rc := newReplica()
	vr := newVR(c)
	go rc.run(vr)
	return rc
}

type replica struct {
	requestC            chan proto.Message
	receiveC            chan proto.Message
	readyC              chan Ready
	advanceC            chan struct{}
	clockC              chan struct{}
	configurationC      chan proto.Configuration
	configurationStateC chan proto.ConfigurationState
	doneC               chan struct{}
	stopC               chan struct{}
	statusC             chan chan Status
}

func newReplica() *replica {
	return &replica{
		requestC:            make(chan proto.Message),
		receiveC:            make(chan proto.Message),
		readyC:              make(chan Ready),
		advanceC:            make(chan struct{}),
		configurationC:      make(chan proto.Configuration),
		configurationStateC: make(chan proto.ConfigurationState),
		clockC:              make(chan struct{}),
		doneC:               make(chan struct{}),
		stopC:               make(chan struct{}),
		statusC:             make(chan chan Status),
	}
}

func (r *replica) run(vr *VR) {
	var requestC chan proto.Message
	var readyC chan Ready
	var advanceC chan struct{}
	var prevUnsafeOpNum uint64
	var prevUnsafeViewNum uint64
	var needToSafe bool
	var prevAppliedStateOpNum uint64
	var f Ready

	prim := None
	prevSoftState := vr.softState()
	prevHardState := nilHardState

	for {
		if advanceC != nil {
			readyC = nil
		} else {
			f = newReady(vr, prevSoftState, prevHardState)
			if f.PreCheck() {
				readyC = r.readyC
			} else {
				readyC = nil
			}
		}
		if prim != vr.prim {
			if vr.existPrimary() {
				if prim == None {
					log.Printf("vr.replica: %x change primary %x at view-number %d", vr.replicaNum, vr.prim, vr.ViewNum)
				} else {
					log.Printf("vr.replica: %x changed primary from %x to %x at view-number %d", vr.replicaNum, prim, vr.prim, vr.ViewNum)
				}
				requestC = r.requestC
			} else {
				log.Printf("vr.replica: %x faulty primary %x at view-number %d", vr.replicaNum, prim, vr.ViewNum)
				requestC = nil
			}
			prim = vr.prim
		}
		select {
		case m := <-requestC:
			m.From = vr.replicaNum
			vr.Call(m)
		case m := <-r.receiveC:
			if _, ok := vr.windows[m.From]; ok || !IsReplyMessage(m) {
				vr.Call(m)
			}
		case rc := <-r.configurationC:
			_ = rc
		case <-r.clockC:
			vr.clockFn()
		case readyC <- f:
			if n := len(f.PersistentEntries); n > 0 {
				prevUnsafeOpNum = f.PersistentEntries[n-1].OpNum
				prevUnsafeViewNum = f.PersistentEntries[n-1].ViewNum
				needToSafe = true
			}
			if f.SoftState != nil {
				prevSoftState = f.SoftState
			}
			if !IsInvalidHardState(f.HardState) {
				prevHardState = f.HardState
			}
			if !IsInvalidAppliedState(f.AppliedState) {
				prevAppliedStateOpNum = f.AppliedState.Applied.OpNum
			}
			vr.messages = nil
			advanceC = r.advanceC
		case <-advanceC:
			if prevHardState.CommitNum != 0 {
				vr.opLog.appliedTo(prevHardState.CommitNum)
			}
			if needToSafe {
				vr.opLog.safeTo(prevUnsafeOpNum, prevUnsafeViewNum)
				needToSafe = false
			}
			// TODO: need to check?
			vr.opLog.safeAppliedStateTo(prevAppliedStateOpNum)
			advanceC = nil
		case c := <-r.statusC:
			c <- getStatus(vr)
		case <-r.stopC:
			close(r.doneC)
			return
		}
	}
}

func (r *replica) Advance() {
	select {
	case r.advanceC <- struct{}{}:
	case <-r.doneC:
	}
}

func (r *replica) Change(ctx context.Context) error {
	return r.call(ctx, proto.Message{Type: proto.Change})
}

func (r *replica) Step(ctx context.Context, m proto.Message) error {
	if IsLocalMessage(m) {
		return nil
	}
	return r.call(ctx, m)
}

func (r *replica) call(ctx context.Context, m proto.Message) error {
	ch := r.receiveC
	if m.Type == proto.Request {
		ch = r.requestC
	}
	select {
	case ch <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-r.doneC:
		return ErrStopped
	}
}

func (r *replica) Ready(opt ...Option) <-chan Ready {
	if opt != nil {
		for _, this := range opt {
			if f := this.Filter; f != nil {
				f()
			}
		}
	}
	return r.readyC
}

func (r *replica) Reconfiguration(context.Context, proto.Configuration) *proto.ConfigurationState {
	return nil
}

func (r *replica) Status() Status {
	c := make(chan Status)
	r.statusC <- c
	return <-c
}

func (r *replica) Clock() {
	select {
	case r.clockC <- struct{}{}:
	case <-r.doneC:
	}
}

func (r *replica) Stop() {
	select {
	case r.stopC <- struct{}{}:
	case <-r.doneC:
		return
	}
	<-r.doneC
}

type SoftState struct {
	Prim   uint64
	VRRole role
}

func (r *SoftState) equal(ss *SoftState) bool {
	return r.Prim == ss.Prim && r.VRRole == ss.VRRole
}

type Ready struct {
	proto.AppliedState
	proto.HardState
	*SoftState

	PersistentEntries []proto.Entry
	ApplicableEntries []proto.Entry
	Messages          []proto.Message
}

func newReady(vr *VR, prevSS *SoftState, prevHS proto.HardState) Ready {
	f := Ready{
		PersistentEntries: vr.opLog.unsafeEntries(),
		ApplicableEntries: vr.opLog.safeEntries(),
		Messages:          vr.messages,
	}
	if ss := vr.softState(); !ss.equal(prevSS) {
		f.SoftState = ss
	}
	if !hardStateCompare(vr.HardState, prevHS) {
		f.HardState = vr.HardState
	}
	if vr.opLog.unsafe.appliedState != nil {
		f.AppliedState = *vr.opLog.unsafe.appliedState
	}
	return f
}

func (r Ready) PreCheck() bool {
	return r.SoftState != nil || !IsInvalidHardState(r.HardState) || len(r.PersistentEntries) > 0 ||
		len(r.ApplicableEntries) > 0 || len(r.Messages) > 0
}
