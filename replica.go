package vr

import (
	"log"
	"errors"
	"context"
	"github.com/open-rsm/vr/proto"
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
	Call(context.Context, proto.Message) error
	Clock()
	Membership(context.Context, proto.Configuration)
	Propose(context.Context, []byte)
	Reconfiguration(proto.Configuration) *proto.ConfigurationState
	Tuple(...Option) <-chan Tuple
	Status() Status
	Stop()
}

func StartReplica(c *Config) Replicator {
	rc := newReplica()
	vr := newVR(c)
	vr.becomeBackup(proto.ViewStamp{ViewNum:One}, None)
	vr.opLog.commitNum = vr.opLog.lastOpNum()
	vr.CommitNum = vr.opLog.commitNum
	for _, num := range c.Peers {
		vr.createReplicator(num)
	}
	go rc.cycle(vr)
	return rc
}

func RestartReplica(c *Config) Replicator {
	rc := newReplica()
	vr := newVR(c)
	go rc.cycle(vr)
	return rc
}

type replica struct {
	requestC            chan proto.Message
	receiveC            chan proto.Message
	tupleC              chan Tuple
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
		tupleC:              make(chan Tuple),
		advanceC:            make(chan struct{}),
		configurationC:      make(chan proto.Configuration),
		configurationStateC: make(chan proto.ConfigurationState),
		clockC:              make(chan struct{}),
		doneC:               make(chan struct{}),
		stopC:               make(chan struct{}),
		statusC:             make(chan chan Status),
	}
}

func (r *replica) cycle(vr *VR) {
	var requestC chan proto.Message
	var tupleC chan Tuple
	var advanceC chan struct{}
	var prevUnsafeOpNum uint64
	var prevUnsafeViewNum uint64
	var needToSafe bool
	var prevAppliedStateOpNum uint64
	var tp Tuple

	prim := None
	prevSoftState := vr.softState()
	prevHardState := nilHardState

	for {
		if prim != vr.prim {
			if vr.existPrimary() {
				if prim == None {
					log.Printf("vr.replica: %x change primary %x at view-number %d", vr.replicaNum, vr.prim, vr.ViewStamp.ViewNum)
				} else {
					log.Printf("vr.replica: %x changed primary from %x to %x at view-number %d", vr.replicaNum, prim, vr.prim, vr.ViewStamp.ViewNum)
				}
				requestC = r.requestC
			} else {
				log.Printf("vr.replica: %x faulty primary %x at view-number %d", vr.replicaNum, prim, vr.ViewStamp.ViewNum)
				requestC = nil
			}
			prim = vr.prim
		}
		if advanceC != nil {
			tupleC = nil
		} else {
			tp = newTuple(vr, prevSoftState, prevHardState)
			if tp.PreCheck() {
				tupleC = r.tupleC
			} else {
				tupleC = nil
			}
		}
		select {
		case m := <-requestC:
			m.From = vr.replicaNum
			vr.Call(m)
		case m := <-r.receiveC:
			if vr.progress.Exist(m.From) || !IsReplyMessage(m) {
				vr.Call(m)
			}
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
		case tupleC <- tp:
			if n := len(tp.PersistentEntries); n > 0 {
				prevUnsafeOpNum = tp.PersistentEntries[n-1].ViewStamp.OpNum
				prevUnsafeViewNum = tp.PersistentEntries[n-1].ViewStamp.ViewNum
				needToSafe = true
			}
			if tp.SoftState != nil {
				prevSoftState = tp.SoftState
			}
			if !IsInvalidHardState(tp.HardState) {
				prevHardState = tp.HardState
			}
			if !IsInvalidAppliedState(tp.AppliedState) {
				prevAppliedStateOpNum = tp.AppliedState.Applied.ViewStamp.OpNum
			}
			vr.messages = nil
			advanceC = r.advanceC
		case c := <-r.statusC:
			c <- getStatus(vr)
		case rc := <-r.configurationC:
			r.handleConfiguration(rc)
		case <-r.clockC:
			vr.clock()
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

func (r *replica) Call(ctx context.Context, m proto.Message) error {
	if IsIgnorableMessage(m) {
		return nil
	}
	// TODO: notify the outside that the system is doing reconfiguration
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

func (r *replica) Membership(ctx context.Context, cfg proto.Configuration) {
	data, err := cfg.Marshal()
	if err != nil {
		log.Fatal("vr.replica membership configure data marshal error ", err)
	}
	r.Call(ctx, proto.Message{Type: proto.StartEpoch, Entries: []proto.Entry{{Type:proto.Configure, Data: data}}})
}

func (r *replica) Propose(ctx context.Context, data []byte) {
	r.Call(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Type:proto.Log, Data: data}}})
}

func (r *replica) Tuple(opt ...Option) <-chan Tuple {
	if opt != nil {
		for _, this := range opt {
			if f := this.Filter; f != nil {
				f()
			}
		}
	}
	return r.tupleC
}

func (r *replica) Reconfiguration(c proto.Configuration) *proto.ConfigurationState {
	var state proto.ConfigurationState
	select {
	case r.configurationC <- c:
	case <-r.doneC:
	}
	select {
	case state = <-r.configurationStateC:
	case <-r.doneC:
	}
	return &state
}

func (r *replica) handleConfiguration(c proto.Configuration) {

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
	Prim uint64
	Role role
}

func (r *SoftState) equal(ss *SoftState) bool {
	return r.Prim == ss.Prim && r.Role == ss.Role
}

type Tuple struct {
	proto.AppliedState
	proto.HardState
	*SoftState

	PersistentEntries []proto.Entry
	ApplicableEntries []proto.Entry
	Messages          []proto.Message
}

func newTuple(vr *VR, prevSS *SoftState, prevHS proto.HardState) Tuple {
	t := Tuple{
		PersistentEntries: vr.opLog.unsafeEntries(),
		ApplicableEntries: vr.opLog.safeEntries(),
		Messages:          vr.messages,
	}
	if ss := vr.softState(); !ss.equal(prevSS) {
		t.SoftState = ss
	}
	if !hardStateCompare(vr.HardState, prevHS) {
		t.HardState = vr.HardState
	}
	if vr.opLog.unsafe.appliedState != nil {
		t.AppliedState = *vr.opLog.unsafe.appliedState
	}
	return t
}

func (t Tuple) PreCheck() bool {
	return t.SoftState != nil || !IsInvalidHardState(t.HardState) || len(t.PersistentEntries) > 0 ||
		len(t.ApplicableEntries) > 0 || len(t.Messages) > 0
}
