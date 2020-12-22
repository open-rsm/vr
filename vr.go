package vr

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"github.com/open-rsm/spec/proto"
)

const None uint64 = 0
const noLimit = math.MaxUint64

// role type represents the current role of a replica in a cluster.
type status uint64

// replica role code
const (
	Normal     status = iota
	ViewChange
	Recovering
)

// role name
var statusName = [...]string{"Normal", "ViewChange", "Recovering"}

// role type represents the current role of a replica in a cluster.
type role uint64

// role code
const (
	Replica = iota
	Primary
	Backup
)

// role name
var roleName = [...]string{"Replica", "Primary", "Backup"}

const (
	Change          = iota
	StartViewChange
	DoViewChange
)

type Config struct {
	Num               uint64
	Peers             []uint64
	TransitionTimeout int
	HeartbeatTimeout  int
	Store             *Store
	AppliedNum        uint64
}

func (c *Config) validate() error {
	return nil
}

type VR struct {
	proto.HardState
	replicaNum        uint64
	seq               uint64
	opLog             *opLog
	windows           map[uint64]*Window
	status            status
	role              role
	views             [3]map[uint64]bool
	messages          []proto.Message
	prim              uint64
	pulse             int
	transitionTimeout int
	heartbeatTimeout  int
	rand              *rand.Rand
	callFn            func(*VR, proto.Message)
	clockFn           func()
}

func newVR(cfg *Config) *VR {
	if cfg.Num == None {
		panic("vr: not found num for config")
	}
	opLog := newOpLog(cfg.Store)
	hs, rs, err := cfg.Store.InitState()
	if err != nil {
		panic(err)
	}
	if len(rs.Replicas) > 0 {
		if len(cfg.Peers) > 0 {
			panic("vr: cannot specify both nodes and reconfiguration state nodes)")
		}
		cfg.Peers = rs.Replicas
	}
	vr := &VR{
		replicaNum:        cfg.Num,
		prim:              None,
		opLog:             opLog,
		windows:           make(map[uint64]*Window),
		HardState:         hs,
		transitionTimeout: cfg.TransitionTimeout,
		heartbeatTimeout:  cfg.HeartbeatTimeout,
	}
	vr.rand = rand.New(rand.NewSource(int64(cfg.Num)))
	for _, p := range cfg.Peers {
		vr.windows[p] = &Window{Next: 1}
	}
	if !hardStateCompare(hs, nilHardState) {
		vr.loadHardState(hs)
	}
	if num := cfg.AppliedNum; num > 0 {
		opLog.appliedTo(num)
	}
	vr.becomeBackup(uint64(vr.ViewNum), None)
	var replicaList []string
	for _, n := range vr.replicas() {
		replicaList = append(replicaList, fmt.Sprintf("%x", n))
	}
	log.Printf("vr: new vr %x [nodes: [%s], view-number: %d, commit-number: %d, applied-number: %d, last-op-number: %d, last-view-number: %d]",
		vr.replicaNum, strings.Join(replicaList, ","), vr.ViewNum, vr.opLog.commitNum, vr.opLog.appliedNum, vr.opLog.lastOpNum(), vr.opLog.lastViewNum())
	return vr
}

func (v *VR) existPrimary() bool {
	return v.prim != None
}

func (v *VR) becomePrimary() {
	if v.role == Backup {
		panic("vr: invalid transition [backup -> primary]")
	}
	v.callFn = callPrimary
	v.clockFn = v.clockHeartbeat
	v.reset(v.ViewNum)
	v.prim = v.replicaNum
	v.role = Primary
	v.status = Normal
	v.appendEntry(proto.Entry{Data: nil})
	log.Printf("vr: %x became primary at view-number %d", v.replicaNum, v.ViewNum)
}

func (v *VR) becomeReplica() {
	if v.role == Primary {
		panic("vr: invalid transition [primary -> replica]")
	}
	v.callFn = callReplica
	v.reset(v.ViewNum + 1)
	v.clockFn = v.clockTransition
	v.role = Replica
	v.status = ViewChange
	log.Printf("vr: %x became replica at view-number %d", v.replicaNum, v.ViewNum)
}

func (v *VR) becomeBackup(viewNum, prim uint64) {
	v.callFn = callBackup
	v.clockFn = v.clockTransition
	v.reset(viewNum)
	v.prim = prim
	v.role = Backup
	v.status = Normal
	if v.prim == None {
		log.Printf("vr: %x became backup at view-number %d, primary not found", v.replicaNum, v.ViewNum)
	} else {
		log.Printf("vr: %x became backup at view-number %d, primary is %d", v.replicaNum, v.ViewNum, v.prim)
	}
}

func (v *VR) appendEntry(entries ...proto.Entry) {
	lo := v.opLog.lastOpNum()
	for i := range entries {
		entries[i].ViewNum = v.ViewNum
		entries[i].OpNum = lo + uint64(i) + 1
	}
	v.opLog.append(entries...)
	v.windows[v.replicaNum].update(v.opLog.lastOpNum())
	// TODO need check return value?
	v.tryCommit()
}

func (v *VR) tryCommit() bool {
	nums := make(uint64s, 0, len(v.windows))
	for i := range v.windows {
		nums = append(nums, v.windows[i].Ack)
	}
	sort.Sort(sort.Reverse(nums))
	num := nums[v.quorums()-1]
	return v.opLog.tryCommit(num, v.ViewNum)
}

func (v *VR) resetViews()  {
	for i := 0; i < len(v.views); i++ {
		v.views[i] = map[uint64]bool{}
	}
}

func (v *VR) reset(ViewNum uint64) {
	if v.ViewNum != ViewNum {
		v.ViewNum = ViewNum
	}
	v.prim = None
	v.pulse = 0
	v.resetViews()
	for i := range v.windows {
		v.windows[i] = &Window{Next: v.opLog.lastOpNum() + 1}
		if i == v.replicaNum {
			v.windows[i].Ack = v.opLog.lastOpNum()
		}
	}
}

func (v *VR) Call(m proto.Message) error {
	if m.Type == proto.Change {
		log.Printf("vr: %x is starting a new view changes at view-number %d", v.replicaNum, v.ViewNum)
		change(v)
		v.CommitNum = v.opLog.commitNum
		return nil
	}
	switch {
	case m.ViewNum == 0:
	case m.ViewNum > v.ViewNum:
		prim := m.From
		if (m.Type == proto.StartViewChange) || m.Type == proto.DoViewChange {
			prim = None
		}
		log.Printf("vr: %x [view-number: %d] received a %s message with higher view-number from %x [view-number: %d]",
			v.replicaNum, v.ViewNum, m.Type, m.To, m.ViewNum)
		v.becomeBackup(m.ViewNum, prim)
	case m.ViewNum < v.ViewNum:
		log.Printf("vr: %x [view-number: %d] ignored a %s message with lower view-number from %x [view-number: %d]",
			v.replicaNum, v.ViewNum, m.Type, m.To, m.ViewNum)
		return nil
	}
	v.callFn(v, m)
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
			v.replicaNum, num, v.ViewNum, phrase)
	} else {
		log.Printf("vr: %x received ignore view from %x at view-number %d and phrase %d",
			v.replicaNum, num, v.ViewNum, phrase)
	}
	if _, ok := v.views[phrase][num]; !ok {
		v.views[phrase][num] = view
	}
	return v.stats(phrase)
}

func (v *VR) send(m proto.Message) {
	m.From = v.replicaNum
	if m.Type != proto.Request {
		m.ViewNum = v.ViewNum
	}
	v.messages = append(v.messages, m)
}

func (v *VR) sendAppend(to uint64, typ ...proto.MessageType) {
	window := v.windows[to]
	if window.needDelay() {
		return
	}
	m := proto.Message{
		To: to,
	}
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
		opNum, viewNum := state.Applied.OpNum, state.Applied.ViewNum
		log.Printf("vr: %x [start op-number: %d, commit-number: %d] sent applied-number state[op-number: %d, view-number: %d] to %x [%s]",
			v.replicaNum, v.opLog.startOpNum(), v.CommitNum, opNum, viewNum, to, window)
		window.delaySet(v.transitionTimeout)
	} else {
		m.Type = proto.Prepare
		if typ != nil {
			m.Type = typ[0]
		}
		m.OpNum = window.Next - 1
		m.LogNum = v.opLog.viewNum(window.Next-1)
		m.Entries = v.opLog.entries(window.Next)
		m.CommitNum = v.opLog.commitNum
		if n := len(m.Entries); window.Ack != 0 && n != 0 {
			window.niceUpdate(m.Entries[n-1].OpNum)
		} else if window.Ack == 0 {
			window.delaySet(v.heartbeatTimeout)
		}
	}
	v.send(m)
}

func (v *VR) sendHeartbeat(to uint64) {
	commit := min(v.windows[to].Ack, v.opLog.commitNum)
	v.send(proto.Message{
		To:        to,
		Type:      proto.Commit,
		CommitNum: commit,
	})
}

func (v *VR) fallback(m proto.Message) {
	m.To = v.prim
	v.send(m)
}

func (v *VR) tryAppliedState(num uint64) bool {
	return num < v.opLog.startOpNum()
}

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
	_, ok := v.windows[v.replicaNum]
	return ok
}

func (v *VR) pick() uint64 {
	if n := v.ViewNum % uint64(len(v.windows)); n != 0 {
		return n
	}
	return 1
}

func (v *VR) broadcastAppend() {
	for num := range v.windows {
		if num == v.replicaNum {
			continue
		}
		v.sendAppend(num)
	}
}

func (v *VR) broadcastHeartbeat() {
	for num := range v.windows {
		if num == v.replicaNum {
			continue
		}
		v.sendHeartbeat(num)
		v.windows[num].delayDec(v.heartbeatTimeout)
	}
}

func change(v *VR) {
	v.becomeReplica()
	if v.quorums() == v.collect(v.replicaNum, true, Change) {
		v.becomePrimary()
		return
	}
	for num := range v.windows {
		if num == v.replicaNum {
			continue
		}
		log.Printf("vr: %x [oplog view-number: %d, op-number: %d] sent START-VIEW-CHANGE request to %x at view-number %d",
			v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), num, v.ViewNum)
		v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.StartViewChange, OpNum: v.opLog.lastOpNum(), ViewNum: v.opLog.lastViewNum()})
	}
}

func startViewChange(v *VR, m *proto.Message) {
	num := v.replicaNum
	view := true
	if m != nil {
		num = m.From
		view = !m.Ignore
	}
	views := v.collect(num, view, StartViewChange)
	if views == 1 && !v.take(v.replicaNum, Change) {
		for num := range v.windows {
			if num == v.replicaNum {
				continue
			}
			log.Printf("vr: %x [oplog view-number: %d, op-number: %d] sent START-VIEW-CHANGE request to %x at view-number %d",
				v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), num, v.ViewNum)
			v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.StartViewChange, OpNum: v.opLog.lastOpNum(), ViewNum: v.opLog.lastViewNum()})
		}
	}
	if v.quorum() == views {
		log.Printf("vr: %x has received %d views, start send DO-VIEW-CHANGE message", v.replicaNum, views)
		doViewChange(v, m)
	}
}


func doViewChange(v *VR, m *proto.Message) {
	// If it is not the primary node, send a DO-VIEW-CHANGE message to the new
	// primary node that has been pre-selected.
	if num := v.pick(); num != v.replicaNum {
		log.Printf("vr: %x [oplog view-number: %d, op-number: %d] sent DO-VIEW-CHANGE request to %x at view-number %d, windows: %d",
				v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), num, v.ViewNum, len(v.windows))
		v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.DoViewChange, OpNum: v.opLog.lastOpNum(), ViewNum: v.opLog.lastViewNum()})
		return
	} else {
		// If it is the primary node, first send yourself a DO-VIEW-CHANGE message.
		v.collect(v.replicaNum, true, DoViewChange)
	}
	num := v.replicaNum
	view := true
	if m != nil {
		num = m.From
		view = !m.Ignore
	}
	if v.quorums() == v.collect(num, view, DoViewChange) {
		for num := range v.windows {
			if num == v.replicaNum {
				continue
			}
			log.Printf("vr: %x [oplog view-number: %d, op-number: %d] sent START-VIEW request to %x at view-number %d",
				v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), num, v.ViewNum)
			v.send(proto.Message{From: v.replicaNum, To: num, Type: proto.StartView, OpNum: v.opLog.lastOpNum(), ViewNum: v.opLog.lastViewNum()})
		}
		v.becomePrimary()
		v.broadcastAppend()
		return
	}
}

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
		if m.Ignore {
			log.Printf("vr: %x received prepare ok ignore(last-op-number: %d) from %x for op-number %d",
				v.replicaNum, m.Note, m.From, m.OpNum)
			if v.windows[m.From].tryDecTo(m.OpNum, m.Note) {
				log.Printf("vr: %x decreased window of %x to [%s]", v.replicaNum, m.From, v.windows[m.From])
				v.sendAppend(m.From)
			}
		} else {
			delay := v.windows[m.From].needDelay()
			fmt.Println("")
			v.windows[m.From].update(m.OpNum)
			if v.tryCommit() {
				v.broadcastAppend()
			} else if delay {
				v.sendAppend(m.From)
			}
		}
	case proto.CommitOk:
		if v.windows[m.From].Ack < v.opLog.lastOpNum() {
			v.sendAppend(m.From)
		}
	case proto.StartViewChange, proto.DoViewChange:
		log.Printf("vr: %x [log-view-number: %d, op-number: %d] ignore %s from %x [op-number: %d] at view-number %d",
			v.replicaNum, v.opLog.lastViewNum(), v.opLog.lastOpNum(), m.Type, m.From, m.OpNum, m.ViewNum)
		//v.send(proto.Message{To: m.From, Type: m.Type, Ignore: true})
	case proto.StartView:
		v.becomeBackup(v.ViewNum, m.From)
		v.status = Normal
	case proto.Recovery:
		log.Printf("vr: %x received recovery (last-op-number: %d) from %x for op-number %d to recovery response",
			v.replicaNum, m.X, m.From, m.OpNum)
		if v.windows[m.From].tryDecTo(m.OpNum, m.Note) {
			log.Printf("vr: %x decreased windows of %x to [%s]", v.replicaNum, m.From, v.windows[m.From])
			v.sendAppend(m.From, proto.RecoveryResponse)
		}
	case proto.GetState:
		log.Printf("vr: %x received get state (last-op-number: %d) from %x for op-number %d to new state",
			v.replicaNum, m.X, m.From, m.OpNum)
		if v.windows[m.From].tryDecTo(m.OpNum, m.Note) {
			log.Printf("v: %x decreased windows of %x to [%s], send new state", v.replicaNum, m.From, v.windows[m.From])
			v.sendAppend(m.From, proto.NewState)
		}
	}
}

func callReplica(v *VR, m proto.Message) {
	switch m.Type {
	case proto.Request:
		log.Printf("vr: %x no primary (replica) at view-number %d; dropping request", v.replicaNum, v.ViewNum)
		return
	case proto.Prepare:
		v.becomeBackup(v.ViewNum, m.From)
		v.handleAppend(m)
		v.status = Normal
	case proto.PrepareAppliedState:
		v.becomeBackup(v.ViewNum, m.From)
		v.handleAppliedState(m)
		v.status = Normal
	case proto.Commit:
		v.becomeBackup(v.ViewNum, m.From)
		v.handleHeartbeat(m)
		v.status = Normal
	case proto.StartViewChange:
		startViewChange(v, &m)
	case proto.DoViewChange:
		doViewChange(v, &m)
	case proto.StartView:
		v.becomeBackup(v.ViewNum, m.From)
		v.status = Normal
	}
}

func callBackup(v *VR, m proto.Message) {
	switch m.Type {
	case proto.Request:
		if v.prim == None {
			log.Printf("vr: %x no primary (backup) at view-number %d; dropping request", v.replicaNum, v.ViewNum)
			return
		}
		v.fallback(m)
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
		v.becomeBackup(v.ViewNum, m.From)
		v.status = Normal
	case proto.RecoveryResponse:
		v.pulse = 0
		v.prim = m.From
		v.handleAppend(m)
	case proto.NewState:
		v.pulse = 0
		v.prim = m.From
		v.handleAppend(m)
	}
}

func (v *VR) handleAppend(m proto.Message) {
	if msgLastOpNum, ok := v.opLog.tryAppend(m.OpNum, m.LogNum, m.CommitNum, m.Entries...); ok {
		v.send(proto.Message{To: m.From, Type: proto.PrepareOk, OpNum: msgLastOpNum})
		return
	}
	switch v.status {
	case Normal:
		log.Printf("vr: %x normal state [log-number: %d, op-number: %d] find [log-number: %d, op-number: %d] from %x",
			v.replicaNum, v.opLog.viewNum(m.OpNum), m.OpNum, m.LogNum, m.OpNum, m.From)
		v.send(proto.Message{To: m.From, Type: proto.GetState, OpNum: m.OpNum, Note: v.opLog.lastOpNum()})
	case Recovering:
		log.Printf("vr: %x recovering state [log-number: %d, op-number: %d] find [log-number: %d, op-number: %d] from %x",
			v.replicaNum, v.opLog.viewNum(m.OpNum), m.OpNum, m.LogNum, m.OpNum, m.From)
		v.send(proto.Message{To: m.From, Type: proto.Recovery, OpNum: m.OpNum, X: v.seq, Note: v.opLog.lastOpNum()})
	default:
		log.Printf("vr: %x [log-number: %d, op-number: %d] ignored prepare [log-number: %d, op-number: %d] from %x",
			v.replicaNum, v.opLog.viewNum(m.OpNum), m.OpNum, m.LogNum, m.OpNum, m.From)
		v.send(proto.Message{To: m.From, Type: proto.PrepareOk, OpNum: m.OpNum, Ignore: true, Note: v.opLog.lastOpNum()})
	}
}

func (v *VR) handleAppliedState(m proto.Message) {
	opNum, viewNum := m.AppliedState.Applied.OpNum, m.AppliedState.Applied.ViewNum
	if v.recover(m.AppliedState) {
		log.Printf("vr: %x [commit-number: %d] restored applied state [op-number: %d, view-number: %d]",
			v.replicaNum, v.CommitNum, opNum, viewNum)
		v.send(proto.Message{To: m.From, Type: proto.PrepareOk, OpNum: v.opLog.lastOpNum()})
	} else {
		log.Printf("vr: %x [commit-number: %d] ignored applied state [op-number: %d, view-number: %d]",
			v.replicaNum, v.CommitNum, opNum, viewNum)
		v.send(proto.Message{To: m.From, Type: proto.PrepareOk, OpNum: v.opLog.commitNum})
	}
}

func (v *VR) handleHeartbeat(m proto.Message) {
	v.opLog.commitTo(m.CommitNum)
	v.send(proto.Message{To: m.From, Type: proto.CommitOk})
}

func (v *VR) softState() *SoftState {
	return &SoftState{Prim: v.prim, VRRole: v.role}
}

func (v *VR) quorum() int {
	return len(v.windows)/2
}

func (v *VR) quorums() int {
	return v.quorum() + 1
}

func (v *VR) replicas() []uint64 {
	replicas := make([]uint64, 0, len(v.windows))
	for window := range v.windows {
		replicas = append(replicas, window)
	}
	sort.Sort(uint64s(replicas))
	return replicas
}

func (v *VR) loadHardState(hs proto.HardState) {
	if hs.CommitNum < v.opLog.commitNum || hs.CommitNum > v.opLog.lastOpNum() {
		log.Panicf("vr: %x commit-number %d is out of range [%d, %d]",
			v.replicaNum, hs.CommitNum, v.opLog.commitNum, v.opLog.lastOpNum())
	}
	v.opLog.commitNum = hs.CommitNum
	v.ViewNum = hs.ViewNum
	v.CommitNum = hs.CommitNum
}

func (v *VR) createReplicator(num uint64) {
	if _, ok := v.windows[num]; ok {
		return
	}
	v.setWindow(num, 0, v.opLog.lastOpNum()+1)
}

func (v *VR) destroyReplicator(id uint64) {
	v.delWindow(id)
}

func (v *VR) setWindow(num, offset, next uint64) {
	v.windows[num] = &Window{Next: next, Ack: offset}
}

func (v *VR) delWindow(num uint64) {
	delete(v.windows, num)
}

func (v *VR) isTransitionTimeout() bool {
	delta := v.pulse - v.transitionTimeout
	if delta < 0 {
		return false
	}
	return delta > v.rand.Int()%v.transitionTimeout
}

func (v *VR) recover(as proto.AppliedState) bool {
	if as.Applied.OpNum <= v.opLog.commitNum {
		return false
	}
	if v.opLog.checkNum(as.Applied.OpNum, as.Applied.ViewNum) {
		log.Printf("vr: %x [commit-number: %d, last-op-number: %d, last-view-number: %d] skip commit to applied state [op-number: %d, view-number: %d]",
			v.replicaNum, v.CommitNum, v.opLog.lastOpNum(), v.opLog.lastViewNum(), as.Applied.OpNum, as.Applied.ViewNum)
		v.opLog.commitTo(as.Applied.OpNum)
		return false
	}
	log.Printf("vr: %x [commit-number: %d, last-op-number: %d, last-view-number: %d] starts to recover applied state [op-number: %d, view-number: %d]",
		v.replicaNum, v.CommitNum, v.opLog.lastOpNum(), v.opLog.lastViewNum(), as.Applied.OpNum, as.Applied.ViewNum)
	v.opLog.recover(as)
	return true
}
