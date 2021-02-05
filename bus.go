package vr

import (
	"log"
	"errors"
	"context"
	"github.com/open-rsm/vr/proto"
)

var (
	nilHardState = proto.HardState{}
	ErrStopped   = errors.New("vr.bus: replicator stopped")
)

type Option struct {
	Action int
	Filter func()
}

type Bus interface {
	Advance()
	Change(ctx context.Context) error
	Call(context.Context, proto.Message) error
	Clock()
	Membership(context.Context, proto.Configuration)
	Propose(context.Context, []byte)
	Reconfiguration(proto.Configuration) *proto.ConfigurationState
	Status() Status
	Stop()
	Tuple(...Option) <-chan Tuple
}

func Start(c *Config) Bus {
	b := newBus()
	vr := newVR(c)
	vr.becomeBackup(proto.ViewStamp{ViewNum:One}, None)
	vr.opLog.commitNum = vr.opLog.lastOpNum()
	vr.CommitNum = vr.opLog.commitNum
	for _, num := range c.Peers {
		vr.createReplica(num)
	}
	go b.cycle(vr)
	return b
}

func Restart(c *Config) Bus {
	b := newBus()
	vr := newVR(c)
	go b.cycle(vr)
	return b
}

type bus struct {
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

func newBus() *bus {
	return &bus{
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

func (b *bus) cycle(vr *VR) {
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
					log.Printf("vr.bus: %x change primary %x at view-number %d", vr.replicaNum, vr.prim, vr.ViewStamp.ViewNum)
				} else {
					log.Printf("vr.bus: %x changed primary from %x to %x at view-number %d", vr.replicaNum, prim, vr.prim, vr.ViewStamp.ViewNum)
				}
				requestC = b.requestC
			} else {
				log.Printf("vr.bus: %x faulty primary %x at view-number %d", vr.replicaNum, prim, vr.ViewStamp.ViewNum)
				requestC = nil
			}
			prim = vr.prim
		}
		if advanceC != nil {
			tupleC = nil
		} else {
			tp = newTuple(vr, prevSoftState, prevHardState)
			if tp.PreCheck() {
				tupleC = b.tupleC
			} else {
				tupleC = nil
			}
		}
		select {
		case m := <-requestC:
			m.From = vr.replicaNum
			vr.Call(m)
		case m := <-b.receiveC:
			if vr.group.Exist(m.From) || !IsReplyMessage(m) {
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
			advanceC = b.advanceC
		case c := <-b.statusC:
			c <- getStatus(vr)
		case c := <-b.configurationC:
			b.handleConfiguration(c, vr)
		case <-b.clockC:
			vr.clock()
		case <-b.stopC:
			close(b.doneC)
			return
		}
	}
}

func (b *bus) Advance() {
	select {
	case b.advanceC <- struct{}{}:
	case <-b.doneC:
	}
}

func (b *bus) Change(ctx context.Context) error {
	return b.call(ctx, proto.Message{Type: proto.Change})
}

func (b *bus) Call(ctx context.Context, m proto.Message) error {
	if IsIgnorableMessage(m) {
		return nil
	}
	// TODO: notify the outside that the system is doing reconfiguration
	return b.call(ctx, m)
}

func (b *bus) call(ctx context.Context, m proto.Message) error {
	ch := b.receiveC
	if m.Type == proto.Request {
		ch = b.requestC
	}
	select {
	case ch <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.doneC:
		return ErrStopped
	}
}

func (b *bus) Membership(ctx context.Context, cfg proto.Configuration) {
	data, err := cfg.Marshal()
	if err != nil {
		log.Fatal("vr.bus membership configure data marshal error ", err)
	}
	b.Call(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Type:proto.Configure, Data: data}}})
}

func (b *bus) Propose(ctx context.Context, data []byte) {
	b.Call(ctx, proto.Message{Type: proto.Request, Entries: []proto.Entry{{Type:proto.Log, Data: data}}})
}

func (b *bus) Tuple(opt ...Option) <-chan Tuple {
	if opt != nil {
		for _, this := range opt {
			if f := this.Filter; f != nil {
				f()
			}
		}
	}
	return b.tupleC
}

func (b *bus) Reconfiguration(c proto.Configuration) *proto.ConfigurationState {
	var state proto.ConfigurationState
	select {
	case b.configurationC <- c:
	case <-b.doneC:
	}
	select {
	case state = <-b.configurationStateC:
	case <-b.doneC:
	}
	return &state
}

func (b *bus) handleConfiguration(c proto.Configuration, vr *VR) {
	if c.ReplicaNum == None {
		select {
		case b.configurationStateC <- proto.ConfigurationState{Configuration: vr.group.ReplicaNums()}:
		case <-b.doneC:
		}
		return
	}
	switch c.Type {
	case proto.AddReplica:
		vr.createReplica(c.ReplicaNum)
	case proto.DelReplica:
		if c.ReplicaNum == vr.replicaNum {
			b.requestC = nil
		}
		vr.destroyReplica(c.ReplicaNum)
	default:
		panic("unexpected configuration type")
	}
	select {
	case b.configurationStateC <- proto.ConfigurationState{Configuration: vr.group.ReplicaNums()}:
	case <-b.doneC:
	}
}

func (b *bus) Status() Status {
	c := make(chan Status)
	b.statusC <- c
	return <-c
}

func (b *bus) Clock() {
	select {
	case b.clockC <- struct{}{}:
	case <-b.doneC:
	}
}

func (b *bus) Stop() {
	select {
	case b.stopC <- struct{}{}:
	case <-b.doneC:
		return
	}
	<-b.doneC
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
