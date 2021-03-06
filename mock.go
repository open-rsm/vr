package vr

import (
	"io"
	"os"
	"fmt"
	"strings"
	"os/exec"
	"io/ioutil"
	"math/rand"
	"github.com/open-rsm/vr/proto"
	"github.com/open-rsm/vr/group"
)

type Node interface {
	Call(m proto.Message) error
	handleMessages() []proto.Message
}

func (v *VR) handleMessages() []proto.Message {
	msgs := v.messages
	v.messages = make([]proto.Message, 0)
	return msgs
}

type mock struct {
	numbers []uint64
	nodes   map[uint64]Node
	stores  map[uint64]*Store
	routers map[route]float64
	ignores map[proto.MessageType]bool
}

type route struct {
	from, to uint64
}

func newMock(nodes ...Node) *mock {
	n := len(nodes)
	m := &mock{
		numbers: numberBySize(n),
		nodes:   make(map[uint64]Node, n),
		stores:  make(map[uint64]*Store, n),
		routers: make(map[route]float64),
		ignores: make(map[proto.MessageType]bool),
	}
	for i, node := range nodes {
		if err := m.build(i, node, n); err != nil {
			panic(err)
		}
	}
	return m
}

func (m *mock) build(index int, node Node, n int) error {
	if n <= 0 {
		return fmt.Errorf("node too small: %d", n)
	}
	num := m.numbers[index]
	switch v := node.(type) {
	case *real:
		m.stores[num] = NewStore()
		vr := newVR(&Config{
			Num:               num,
			Peers:             m.numbers,
			TransitionTimeout: 10,
			HeartbeatTimeout:  1,
			Store:             m.stores[num],
			AppliedNum:        0,
		})
		m.nodes[num] = vr
	case *faker:
		m.nodes[num] = v
	case *VR:
		v.replicaNum = num
		var peers []uint64
		for i := 0; i < n; i++ {
			peers = append(peers, m.numbers[i])
		}
		v.group = group.New(peers)
		v.reset(0)
		m.nodes[num] = v
	default:
		return fmt.Errorf("unexpecteded node type: %T", node)
	}
	return nil
}

func (m *mock) trigger(msgs ...proto.Message) {
	for len(msgs) > 0 {
		msg := msgs[0]
		peer := m.nodes[msg.To]
		peer.Call(msg)
		adds := m.handler(peer.handleMessages())
		msgs = append(msgs[1:], adds...)
	}
}

func (m *mock) peers(num uint64) *VR {
	return m.nodes[num].(*VR)
}

func (m *mock) delete(from, to uint64, percent float64) {
	m.routers[route{from, to}] = percent
}

func (m *mock) cover(one, other uint64) {
	m.delete(one, other, 1)
	m.delete(other, one, 1)
}

func (m *mock) shield(num uint64) {
	for i := 0; i < len(m.nodes); i++ {
		other := uint64(i) + 1
		if other != num {
			m.delete(num, other, 1.0)
			m.delete(other, num, 1.0)
		}
	}
}

func (m *mock) ignore(mt proto.MessageType) {
	m.ignores[mt] = true
}

func (m *mock) reset() {
	m.numbers = []uint64{}
	m.routers = make(map[route]float64)
	m.ignores = make(map[proto.MessageType]bool)
}

func (m *mock) handler(msgs []proto.Message) []proto.Message {
	ms := []proto.Message{}
	for _, msg := range msgs {
		if err := m.filter(msg, &ms); err != nil {
			panic(err)
		}
	}
	return ms
}

func (m *mock) filter(msg proto.Message, msgs *[]proto.Message) error {
	if m.ignores[msg.Type] {
		return nil
	}
	if msg.Type == proto.Change {
		return fmt.Errorf("unexpected change")
	} else {
		router := m.routers[route{msg.From, msg.To}]
		if n := rand.Float64(); n < router {
			return nil
		}
	}
	*msgs = append(*msgs, msg)
	return nil
}

type real struct {}

func (real) Call(proto.Message) error     {
	return nil
}

func (real) handleMessages() []proto.Message {
	return nil
}

var node = &real{}

type faker struct{}

func (faker) Call(proto.Message) error     {
	return nil
}

func (faker) handleMessages() []proto.Message {
	return nil
}

var hole = &faker{}

func numberBySize(size int) []uint64 {
	nums := make([]uint64, size)
	for i := 0; i < size; i++ {
		nums[i] = 1 + uint64(i)
	}
	return nums
}

func entries(viewNums ...uint64) *VR {
	s := NewStore()
	for i, viewNum := range viewNums {
		s.Append([]proto.Entry{{ViewStamp: proto.ViewStamp{OpNum: uint64(i + 1), ViewNum: viewNum}}})
	}
	vr := newVR(&Config{
		Num:               1,
		Peers:             nil,
		TransitionTimeout: 5,
		HeartbeatTimeout:  1,
		Store:             NewStore(),
		AppliedNum:        0,
	})
	vr.reset(0)
	return vr
}

func safeEntries(vr *VR, s *Store) (entries []proto.Entry) {
	s.Append(vr.opLog.unsafeEntries())
	vr.opLog.safeTo(vr.opLog.lastOpNum(), vr.opLog.lastViewNum())
	entries = vr.opLog.safeEntries()
	vr.opLog.appliedTo(vr.opLog.commitNum)
	return entries
}

func diff(a, b string) string {
	if a == b {
		return ""
	}
	alpha, beta := mustTempFile("alpha*", a), mustTempFile("beta*", b)
	defer func() {
		os.Remove(alpha)
		os.Remove(beta)
	}()
	cmd := exec.Command("diff", "-u", alpha, beta)
	buf, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return string(buf)
		}
		panic(err)
	}
	return string(buf)
}

func mustTempFile(pattern, data string) string {
	f, err := ioutil.TempFile("", pattern)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, strings.NewReader(data))
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func stringOpLog(ol *opLog) string {
	s := fmt.Sprintf("commit-number: %d\n", ol.commitNum)
	s += fmt.Sprintf("applied-number:  %d\n", ol.appliedNum)
	for i, e := range ol.totalEntries() {
		s += fmt.Sprintf("#%d: %+v\n", i, e)
	}
	return s
}