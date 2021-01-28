package vr

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"
	"strings"
	"github.com/open-rsm/vr/proto"
	"github.com/open-rsm/vr/group"
)

const (
	None uint64 = iota
	One
)

const noLimit = math.MaxUint64

// status type represents the current status of a replica in a cluster.
type status uint64

// status code
const (
	Normal     status = iota
	ViewChange
	Recovering
	Transitioning
)

// status name
var statusName = [...]string{"Normal", "ViewChange", "Recovering", "Transitioning"}

// role type represents the current role of a replica in a cluster.
type role uint64

// role code
const (
	Replica role = iota
	Primary
	Backup
)

// role name
var roleName = [...]string{"Replica", "Primary", "Backup"}

// view change phrases
const (
	Change          = iota
	StartViewChange
	DoViewChange
)

// view stamped replication configure
type Config struct {
	Num               uint64         // replica number, from 1 start
	Peers             []uint64       // all the nodes in a replication group, include self
	Store             *Store         // state machine storage models
	TransitionTimeout time.Duration  // maximum processing time (ms) for primary
	HeartbeatTimeout  time.Duration  // maximum waiting time (ms) for backups
	AppliedNum        uint64         // where the log has been applied ?
	Selector          int            // new primary node selection strategy
}

// configure check
func (c *Config) validate() error {
	if c.Num < 0 || c.Num == None {
		return fmt.Errorf("vr: replica number cannot be zero or a number smaller than zero")
	}
	if c.Store == nil {
		return fmt.Errorf("vr: store is not initialized in config")
	}
	cs, err := c.Store.LoadConfigurationState()
	if err != nil {
		return err
	}
	if len(cs.Configuration) > 0 {
		if len(c.Peers) > 0 {
			return fmt.Errorf("vr: found that there are replicas in the configuration file, and ignore the user's input")
		}
		c.Peers = cs.Configuration
	}
	if n := len(c.Peers); c.Peers != nil && n < 1 {
		return fmt.Errorf("vr: there are no available nodes in the replication group")
	}
	if c.AppliedNum < 0 {
		return fmt.Errorf("vr: applied number cannot be smaller than zero")
	}
	if c.TransitionTimeout == 0 {
		c.TransitionTimeout = 300*time.Millisecond
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 100*time.Millisecond
	}
	if !isInvalidSelector(c.Selector) {
		return fmt.Errorf("vr: selector is invalid: %d", c.Selector)
	}
	return nil
}

// protocol control models for VR
type VR struct {
	// The key state of the replication group that has landed
	proto.HardState

	replicaNum uint64             // replica number, from 1 start
	opLog      *opLog             // used to manage operation logs
	group      *group.Group       // control and manage the current synchronization replicas
	status     status             // record the current replication group status
	role       role               // mark the current replica role
	views      [3]map[uint64]bool // count the views of each replica during the view change process
	messages   []proto.Message    // temporarily store messages that need to be sent
	prim       uint64             // who is the primary ?
	pulse      int                // occurrence frequency
	transitionTimeout int         // maximum processing time for primary
	heartbeatTimeout  int         // maximum waiting time for backups
	rand              *rand.Rand  // generate random seed
	call              callFn      // intervention automaton device through external events
	clock             clockFn     // drive clock oscillator
	selected   selectFn           // new primary node selection algorithm
	seq        uint64             // monotonically increasing number
}

func newVR(c *Config) *VR {
	if err := c.validate(); err != nil {
		panic(fmt.Sprintf("vr: config validate error: %v", err))
	}
	hs, err := c.Store.LoadHardState()
	if err != nil {
		panic(fmt.Sprintf("vr: load hard state error: %v", err))
	}
	vr := &VR{
		replicaNum:        c.Num,
		prim:              None,
		opLog:             newOpLog(c.Store),
		group:             group.New(c.Peers),
		HardState:         hs,
		transitionTimeout: int(c.TransitionTimeout),
		heartbeatTimeout:  int(c.HeartbeatTimeout),
	}
	vr.rand = rand.New(rand.NewSource(int64(c.Num)))
	vr.initSelector(c.Selector)
	if !hardStateCompare(hs, nilHardState) {
		vr.loadHardState(hs)
	}
	if num := c.AppliedNum; num > 0 {
		vr.opLog.appliedTo(num)
	}
	vr.becomeBackup(vr.ViewStamp, None)
	var replicaList []string
	for _, n := range vr.group.Replicas() {
		replicaList = append(replicaList, fmt.Sprintf("%x", n))
	}
	log.Printf("vr: new vr %x [nodes: [%s], view-number: %d, commit-number: %d, applied-number: %d, last-op-number: %d, last-view-number: %d]",
		vr.replicaNum, strings.Join(replicaList, ","), vr.ViewStamp.ViewNum, vr.opLog.commitNum, vr.opLog.appliedNum, vr.opLog.lastOpNum(), vr.opLog.lastViewNum())
	return vr
}

func (v *VR) becomePrimary() {
	if v.role == Backup {
		panic("vr: invalid transition [backup to primary]")
	}
	v.call = callPrimary
	v.clock = v.clockHeartbeat
	v.reset(v.ViewStamp.ViewNum)
	v.initEntry()
	v.prim = v.replicaNum
	v.role = Primary
	v.status = Normal
	log.Printf("vr: %x became primary at view-number %d", v.replicaNum, v.ViewStamp.ViewNum)
}

func (v *VR) becomeReplica() {
	if v.role == Primary {
		panic("vr: invalid transition [primary to replica]")
	}
	v.call = callReplica
	v.clock = v.clockTransition
	v.reset(v.ViewStamp.ViewNum + 1)
	v.role = Replica
	v.status = ViewChange
	log.Printf("vr: %x became replica at view-number %d", v.replicaNum, v.ViewStamp.ViewNum)
}

func (v *VR) becomeBackup(vs proto.ViewStamp, prim uint64) {
	v.call = callBackup
	v.clock = v.clockTransition
	v.reset(vs.ViewNum)
	v.prim = prim
	v.role = Backup
	v.status = Normal
	if v.prim == None {
		log.Printf("vr: %x became backup at view-number %d, primary not found", v.replicaNum, v.ViewStamp.ViewNum)
	} else {
		log.Printf("vr: %x became backup at view-number %d, primary is %d", v.replicaNum, v.ViewStamp.ViewNum, v.prim)
	}
}

func (v *VR) initEntry() {
	v.appendEntry(proto.Entry{Data: nil})
}

func (v *VR) existPrimary() bool {
	return v.prim != None
}

func (v *VR) tryCommit() bool {
	return v.opLog.tryCommit(v.group.Commit(), v.ViewStamp.ViewNum)
}

func (v *VR) resetViews()  {
	for i := 0; i < len(v.views); i++ {
		v.views[i] = map[uint64]bool{}
	}
}

func (v *VR) reset(ViewNum uint64) {
	if v.ViewStamp.ViewNum != ViewNum {
		v.ViewStamp.ViewNum = ViewNum
	}
	v.prim = None
	v.pulse = 0
	v.resetViews()
	v.group.Reset(v.opLog.lastOpNum(), v.replicaNum)
}

func (v *VR) Call(m proto.Message) error {
	if m.Type == proto.Change {
		log.Printf("vr: %x is starting a new view changes at view-number %d", v.replicaNum, v.ViewStamp.ViewNum)
		change(v)
		v.CommitNum = v.opLog.commitNum
		return nil
	}
	switch {
	case m.ViewStamp.ViewNum == 0:
	case m.ViewStamp.ViewNum > v.ViewStamp.ViewNum:
		prim := m.From
		if (m.Type == proto.StartViewChange) || m.Type == proto.DoViewChange {
			prim = None
		}
		log.Printf("vr: %x [view-number: %d] received a %s message with higher view-number from %x [view-number: %d]",
			v.replicaNum, v.ViewStamp.ViewNum, m.Type, m.To, m.ViewStamp.ViewNum)
		v.becomeBackup(m.ViewStamp, prim)
	case m.ViewStamp.ViewNum < v.ViewStamp.ViewNum:
		log.Printf("vr: %x [view-number: %d] ignored a %s message with lower view-number from %x [view-number: %d]",
			v.replicaNum, v.ViewStamp.ViewNum, m.Type, m.To, m.ViewStamp.ViewNum)
		return nil
	}
	v.call(v, m)
	v.CommitNum = v.opLog.commitNum
	return nil
}

func (v *VR) take(num uint64, phrase int) bool {
	return v.views[phrase][num]
}

func (v *VR) stats(phrase int) (count int) {
	for _, v := range v.views[phrase] {
		if v {
			count++
		}
	}
	return count
}

func (v *VR) collect(num uint64, view bool, phrase int) int {
	if view {
		log.Printf("vr: %x received view-change from %x at view-number %d and phrase %d",
			v.replicaNum, num, v.ViewStamp.ViewNum, phrase)
	} else {
		log.Printf("vr: %x received ignore view from %x at view-number %d and phrase %d",
			v.replicaNum, num, v.ViewStamp.ViewNum, phrase)
	}
	if _, ok := v.views[phrase][num]; !ok {
		v.views[phrase][num] = view
	}
	return v.stats(phrase)
}

func (v *VR) send(m proto.Message) {
	m.From = v.replicaNum
	if m.Type != proto.Request {
		m.ViewStamp.ViewNum = v.ViewStamp.ViewNum
	}
	v.messages = append(v.messages, m)
}

func (v *VR) sendAppend(to uint64, typ ...proto.MessageType) {
	window := v.group.IndexOf(to)
	if window.NeedDelay() {
		return
	}
	m := proto.Message{
		Type: proto.Prepare,
		To: to,
	}
	defer func() {
		v.send(m)
	}()
	if v.tryAppliedState(window.Next) {
		m.Type = proto.PrepareAppliedState
		state, err := v.opLog.appliedState()
		if err != nil {
			panic(err)
		}
		if IsInvalidAppliedState(state) {
			panic("must be a valid state")
		}
		m.AppliedState = state
		opNum, viewNum := state.Applied.ViewStamp.OpNum, state.Applied.ViewStamp.ViewNum
		log.Printf("vr: %x [start op-number: %d, commit-number: %d] sent applied-number state[op-number: %d, view-number: %d] to %x [%s]",
			v.replicaNum, v.opLog.startOpNum(), v.CommitNum, opNum, viewNum, to, window)
		window.DelaySet(v.transitionTimeout)
		return
	}
	if typ != nil {
		m.Type = typ[0]
	}
	m.ViewStamp.OpNum = window.Next - 1
	m.LogNum = v.opLog.viewNum(window.Next-1)
	m.Entries = v.opLog.entries(window.Next)
	m.CommitNum = v.opLog.commitNum
	if n := len(m.Entries); window.Ack != 0 && n != 0 {
		window.NiceUpdate(m.Entries[n-1].ViewStamp.OpNum)
	} else if window.Ack == 0 {
		window.DelaySet(v.heartbeatTimeout)
	}
}

func (v *VR) sendHeartbeat(to uint64) {
	commit := min(v.group.IndexOf(to).Ack, v.opLog.commitNum)
	v.send(proto.Message{
		To:        to,
		Type:      proto.Commit,
		CommitNum: commit,
	})
}

func (v *VR) sendto(m proto.Message, to uint64) {
	m.To = to
	v.send(m)
}

func (v *VR) tryAppliedState(num uint64) bool {
	return num < v.opLog.startOpNum()
}

type clockFn func()

func (v *VR) clockTransition() {
	if !v.raising() {
		v.pulse = 0
		return
	}
	v.pulse++
	if v.isTransitionTimeout() {
		v.pulse = 0
		v.Call(proto.Message{From: v.replicaNum, Type: proto.Change})
	}
}

func (v *VR) clockHeartbeat() {
	v.pulse++
	if v.pulse >= v.heartbeatTimeout {
		v.pulse = 0
		v.Call(proto.Message{From: v.replicaNum, Type: proto.Heartbeat})
	}
}

func (v *VR) raising() bool {
	return v.group.Exist(v.replicaNum)
}

func (v *VR) broadcastAppend() {
	for num := range v.group.List() {
		if num == v.replicaNum {
			continue
		}
		v.sendAppend(num)
	}
}

func (v *VR) broadcastHeartbeat() {
	for num := range v.group.List() {
		if num == v.replicaNum {
			continue
		}
		v.sendHeartbeat(num)
		v.group.IndexOf(num).DelayDec(v.heartbeatTimeout)
	}
}

func change(v *VR) {
	v.becomeReplica()
	if v.group.Quorum() == v.collect(v.replicaNum, true, Change) {
		v.becomePrimary()
		return
	}
	for num := range v.group.List() {
		if num == v.replicaNum {
			continue
		}
		log.Printf("vr: %x [oplog view-number: %d, op-number: %d] sent START-VIEW-CHANGE request to %x at view-number %d",
			v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), num, v.ViewStamp.ViewNum)
		v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.StartViewChange, ViewStamp: proto.ViewStamp{ViewNum:v.opLog.lastViewNum(),OpNum:v.opLog.lastOpNum()}})
	}
}

func startViewChange(v *VR, m *proto.Message) {
	num := v.replicaNum
	view := true
	if m != nil {
		num = m.From
	}
	views := v.collect(num, view, StartViewChange)
	if views == 1 && !v.take(v.replicaNum, Change) {
		for num := range v.group.List() {
			if num == v.replicaNum {
				continue
			}
			log.Printf("vr: %x [oplog view-number: %d, op-number: %d] sent START-VIEW-CHANGE request to %x at view-number %d",
				v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), num, v.ViewStamp.ViewNum)
			v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.StartViewChange, ViewStamp: proto.ViewStamp{ViewNum:v.opLog.lastViewNum(),OpNum:v.opLog.lastOpNum()}})
		}
	}
	if v.group.Faulty() == views {
		log.Printf("vr: %x has received %d views, start send DO-VIEW-CHANGE message", v.replicaNum, views)
		doViewChange(v, m)
	}
}

func doViewChange(v *VR, m *proto.Message) {
	// If it is not the primary node, send a DO-VIEW-CHANGE message to the new
	// primary node that has been pre-selected.
	if num := v.selected(v.ViewStamp, v.group.Windows()); num != v.replicaNum {
		log.Printf("vr: %x [oplog view-number: %d, op-number: %d] sent DO-VIEW-CHANGE request to %x at view-number %d, replicas: %d",
				v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), num, v.ViewStamp.ViewNum, v.group.Windows())
		v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.DoViewChange, ViewStamp: proto.ViewStamp{ViewNum:v.opLog.lastViewNum(),OpNum:v.opLog.lastOpNum()}})
		return
	} else {
		// If it is the primary node, first send yourself a DO-VIEW-CHANGE message.
		v.collect(v.replicaNum, true, DoViewChange)
	}
	num := v.replicaNum
	view := true
	if m != nil {
		num = m.From
	}
	if v.group.Quorum() == v.collect(num, view, DoViewChange) {
		for num := range v.group.List() {
			if num == v.replicaNum {
				continue
			}
			log.Printf("vr: %x [oplog view-number: %d, op-number: %d] sent START-VIEW request to %x at view-number %d",
				v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), num, v.ViewStamp.ViewNum)
			v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.StartView, ViewStamp: proto.ViewStamp{ViewNum:v.opLog.lastViewNum(),OpNum:v.opLog.lastOpNum()}})
		}
		v.becomePrimary()
		v.broadcastAppend()
		return
	}
}

type callFn func(*VR, proto.Message)

func callPrimary(v *VR, m proto.Message) {
	switch m.Type {
	case proto.Heartbeat:
		v.broadcastHeartbeat()
	case proto.Request:
		if len(m.Entries) == 0 {
			log.Panicf("vr: %x called empty request", v.replicaNum)
		}
		v.appendEntry(m.Entries...)
		v.broadcastAppend()
	case proto.PrepareOk:
		delay := v.group.IndexOf(m.From).NeedDelay()
		v.group.IndexOf(m.From).Update(m.ViewStamp.OpNum)
		if v.tryCommit() {
			v.broadcastAppend()
		} else if delay {
			v.sendAppend(m.From)
		}
	case proto.CommitOk:
		if v.group.IndexOf(m.From).Ack < v.opLog.lastOpNum() {
			v.sendAppend(m.From)
		}
	case proto.StartViewChange, proto.DoViewChange:
		// ignore message
		log.Printf("vr: %x [log-view-number: %d, op-number: %d] ignore %s from %x [op-number: %d] at view-number %d",
			v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), m.Type, m.From, m.ViewStamp.OpNum, m.ViewStamp.ViewNum)
	case proto.StartView:
		v.becomeBackup(v.ViewStamp, m.From)
		v.status = Normal
	case proto.Recovery:
		// TODO: verify the source
		log.Printf("vr: %x received recovery (last-op-number: %d) from %x for op-number %d to recovery response",
			v.replicaNum, m.X, m.From, m.ViewStamp.OpNum)
		if v.group.IndexOf(m.From).TryDecTo(m.ViewStamp.OpNum, m.Note) {
			log.Printf("vr: %x decreased replicas of %x to [%s]", v.replicaNum, m.From, v.group.IndexOf(m.From))
			v.sendAppend(m.From, proto.RecoveryResponse)
		}
	case proto.GetState:
		log.Printf("vr: %x received get state (last-op-number: %d) from %x for op-number %d to new state",
			v.replicaNum, m.X, m.From, m.ViewStamp.OpNum)
		if v.group.IndexOf(m.From).TryDecTo(m.ViewStamp.OpNum, m.Note) {
			log.Printf("v: %x decreased replicas of %x to [%s], send new state", v.replicaNum, m.From, v.group.IndexOf(m.From))
			v.sendAppend(m.From, proto.NewState)
		}
	case proto.EpochStarted:

	}
}

func callReplica(v *VR, m proto.Message) {
	switch m.Type {
	case proto.Request:
		log.Printf("vr: %x no primary (replica) at view-number %d; dropping request", v.replicaNum, v.ViewStamp.ViewNum)
		return
	case proto.Prepare:
		v.becomeBackup(v.ViewStamp, m.From)
		v.handleAppend(m)
		v.status = Normal
	case proto.PrepareAppliedState:
		v.becomeBackup(v.ViewStamp, m.From)
		v.handleAppliedState(m)
		v.status = Normal
	case proto.Commit:
		v.becomeBackup(v.ViewStamp, m.From)
		v.handleHeartbeat(m)
		v.status = Normal
	case proto.StartViewChange:
		startViewChange(v, &m)
	case proto.DoViewChange:
		doViewChange(v, &m)
	case proto.StartView:
		v.becomeBackup(v.ViewStamp, m.From)
		v.status = Normal
	}
}

func callBackup(v *VR, m proto.Message) {
	switch m.Type {
	case proto.Request:
		if v.prim == None {
			log.Printf("vr: %x no primary (backup) at view-number %d; dropping request", v.replicaNum, v.ViewStamp.ViewNum)
			return
		}
		v.sendto(m, v.prim)
	case proto.Prepare:
		v.pulse = 0
		v.prim = m.From
		v.handleAppend(m)
	case proto.PrepareAppliedState:
		v.pulse = 0
		v.handleAppliedState(m)
	case proto.Commit:
		v.pulse = 0
		v.prim = m.From
		v.handleHeartbeat(m)
	case proto.StartViewChange:
		v.status = ViewChange
		startViewChange(v, &m)
	case proto.DoViewChange:
		v.status = ViewChange
		doViewChange(v, &m)
	case proto.StartView:
		v.becomeBackup(v.ViewStamp, m.From)
		v.status = Normal
	case proto.RecoveryResponse:
		// TODO: do load balancing to reduce the pressure on the primary
		v.pulse = 0
		v.prim = m.From
		v.handleAppend(m)
	case proto.NewState:
		v.pulse = 0
		v.prim = m.From
		v.handleAppend(m)
	case proto.StartEpoch:
	}
}

func (v *VR) appendEntry(entries ...proto.Entry) {
	lo := v.opLog.lastOpNum()
	for i := range entries {
		entries[i].ViewStamp.ViewNum = v.ViewStamp.ViewNum
		entries[i].ViewStamp.OpNum = lo + uint64(i) + 1
	}
	v.opLog.append(entries...)
	v.group.IndexOf(v.replicaNum).Update(v.opLog.lastOpNum())
	// TODO need check return value?
	v.tryCommit()
}

func (v *VR) handleAppend(m proto.Message) {
	msgLastOpNum, ok := v.opLog.tryAppend(m.ViewStamp.OpNum, m.LogNum, m.CommitNum, m.Entries...);
	if ok {
		v.send(proto.Message{To: m.From, Type: proto.PrepareOk, ViewStamp: proto.ViewStamp{OpNum: msgLastOpNum}})
		return
	}
	switch v.status {
	case Normal:
		log.Printf("vr: %x normal state [log-number: %d, op-number: %d] find [log-number: %d, op-number: %d] from %x",
			v.replicaNum, v.opLog.viewNum(m.ViewStamp.OpNum), m.ViewStamp.OpNum, m.LogNum, m.ViewStamp.OpNum, m.From)
		v.send(proto.Message{To: m.From, Type: proto.GetState, ViewStamp: proto.ViewStamp{OpNum: m.ViewStamp.OpNum}, Note: v.opLog.lastOpNum()})
	case Recovering:
		log.Printf("vr: %x recovering state [log-number: %d, op-number: %d] find [log-number: %d, op-number: %d] from %x",
			v.replicaNum, v.opLog.viewNum(m.ViewStamp.OpNum), m.ViewStamp.OpNum, m.LogNum, m.ViewStamp.OpNum, m.From)
		v.send(proto.Message{To: m.From, Type: proto.Recovery, ViewStamp: proto.ViewStamp{OpNum: m.ViewStamp.OpNum}, X: v.seq, Note: v.opLog.lastOpNum()})
	default:
		log.Printf("vr: %x [log-number: %d, op-number: %d] ignored prepare [log-number: %d, op-number: %d] from %x",
			v.replicaNum, v.opLog.viewNum(m.ViewStamp.OpNum), m.ViewStamp.OpNum, m.LogNum, m.ViewStamp.OpNum, m.From)
	}
}

func (v *VR) handleAppliedState(m proto.Message) {
	opNum, viewNum := m.AppliedState.Applied.ViewStamp.OpNum, m.AppliedState.Applied.ViewStamp.ViewNum
	if v.recover(m.AppliedState) {
		log.Printf("vr: %x [commit-number: %d] recovered applied state [op-number: %d, view-number: %d]",
			v.replicaNum, v.CommitNum, opNum, viewNum)
		v.send(proto.Message{To: m.From, Type: proto.PrepareOk, ViewStamp: proto.ViewStamp{OpNum: v.opLog.lastOpNum()}})
		return
	}
	log.Printf("vr: %x [commit-number: %d] ignored applied state [op-number: %d, view-number: %d]",
		v.replicaNum, v.CommitNum, opNum, viewNum)
	v.send(proto.Message{To: m.From, Type: proto.PrepareOk, ViewStamp: proto.ViewStamp{OpNum: v.opLog.commitNum}})
}

func (v *VR) handleHeartbeat(m proto.Message) {
	v.opLog.commitTo(m.CommitNum)
	v.send(proto.Message{To: m.From, Type: proto.CommitOk})
}

func (v *VR) softState() *SoftState {
	return &SoftState{Prim: v.prim, Role: v.role}
}

func (v *VR) loadHardState(hs proto.HardState) {
	if hs.CommitNum < v.opLog.commitNum || hs.CommitNum > v.opLog.lastOpNum() {
		log.Panicf("vr: %x commit-number %d is out of range [%d, %d]",
			v.replicaNum, hs.CommitNum, v.opLog.commitNum, v.opLog.lastOpNum())
	}
	v.opLog.commitNum = hs.CommitNum
	v.ViewStamp.ViewNum = hs.ViewStamp.ViewNum
	v.CommitNum = hs.CommitNum
}

func (v *VR) initSelector(num int) {
	loadSelector(&v.selected, num)
}

func (v *VR) createReplicator(num uint64) {
	if v.group.Exist(num) {
		return
	}
	v.group.Set(num,0, v.opLog.lastOpNum()+1)
}

func (v *VR) destroyReplicator(num uint64) {
	v.group.Del(num)
}

func (v *VR) isTransitionTimeout() bool {
	delta := v.pulse - v.transitionTimeout
	if delta < 0 {
		return false
	}
	return delta > v.rand.Int()%v.transitionTimeout
}

func (v *VR) recover(as proto.AppliedState) bool {
	if as.Applied.ViewStamp.OpNum <= v.opLog.commitNum {
		return false
	}
	if v.opLog.checkNum(as.Applied.ViewStamp.OpNum, as.Applied.ViewStamp.ViewNum) {
		log.Printf("vr: %x [commit-number: %d, last-op-number: %d, last-view-number: %d] skip commit to applied state [op-number: %d, view-number: %d]",
			v.replicaNum, v.CommitNum, v.opLog.lastOpNum(), v.opLog.lastViewNum(), as.Applied.ViewStamp.OpNum, as.Applied.ViewStamp.ViewNum)
		v.opLog.commitTo(as.Applied.ViewStamp.OpNum)
		return false
	}
	log.Printf("vr: %x [commit-number: %d, last-op-number: %d, last-view-number: %d] starts to recover applied state [op-number: %d, view-number: %d]",
		v.replicaNum, v.CommitNum, v.opLog.lastOpNum(), v.opLog.lastViewNum(), as.Applied.ViewStamp.OpNum, as.Applied.ViewStamp.ViewNum)
	v.opLog.recover(as)
	return true
}