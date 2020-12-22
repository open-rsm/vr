package vr

import (
	"io"
	"os"
	"fmt"
	"strings"
	"os/exec"
	"io/ioutil"
	"math/rand"
	"github.com/open-rsm/spec/proto"
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
	nodes   map[uint64]Node
	stores  map[uint64]*Store
	removes map[route]float64
	ignores map[proto.MessageType]bool
}

type route struct {
	from, to uint64
}

func newMock(nodes ...Node) *mock {
	size := len(nodes)
	replicas := replicasBySize(size)
	peers := make(map[uint64]Node, size)
	stores := make(map[uint64]*Store, size)
	for i, node := range nodes {
		num := replicas[i]
		switch v := node.(type) {
		case *real:
			stores[num] = NewStore()
			vr := newVR(&Config{
				Num:               num,
				Peers:             replicas,
				TransitionTimeout: 10,
				HeartbeatTimeout:  1,
				Store:             stores[num],
				AppliedNum:        0,
			})
			peers[num] = vr
		case *VR:
			v.replicaNum = num
			v.windows = make(map[uint64]*Window)
			for i := 0; i < size; i++ {
				v.windows[replicas[i]] = &Window{}
			}
			v.reset(0)
			peers[num] = v
		case *faker:
			peers[num] = v
		default:
			panic(fmt.Sprintf("unexpecteded state machine type: %T", node))
		}
	}
	return &mock{
		nodes:   peers,
		stores:  stores,
		removes: make(map[route]float64),
		ignores: make(map[proto.MessageType]bool),
	}
}

func (m *mock) trigger(msgs ...proto.Message) {
	for len(msgs) > 0 {
		msg := msgs[0]
		peer := m.nodes[msg.To]
		peer.Call(msg)
		msgs = append(msgs[1:], m.filter(peer.handleMessages())...)
	}
}

func (m *mock) peers(num uint64) *VR {
	return m.nodes[num].(*VR)
}

func (m *mock) delete(from, to uint64, percent float64) {
	m.removes[route{from, to}] = percent
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
	m.removes = make(map[route]float64)
	m.ignores = make(map[proto.MessageType]bool)
}

func (m *mock) filter(msgs []proto.Message) []proto.Message {
	ms := []proto.Message{}
	for _, msg := range msgs {
		if m.ignores[msg.Type] {
			continue
		}
		switch msg.Type {
		case proto.Change:
			// change never go over the mock, so don'm delete them but panic
			panic("unexpected change")
		default:
			drop := m.removes[route{msg.From, msg.To}]
			if n := rand.Float64(); n < drop {
				continue
			}
		}
		ms = append(ms, msg)
	}
	return ms
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

func replicasBySize(size int) []uint64 {
	nums := make([]uint64, size)
	for i := 0; i < size; i++ {
		nums[i] = 1 + uint64(i)
	}
	return nums
}

func entries(viewNums ...uint64) *VR {
	s := NewStore()
	for i, viewNum := range viewNums {
		s.Append([]proto.Entry{{OpNum: uint64(i + 1), ViewNum: viewNum}})
	}
	vr := newVR(&Config{
		Num:               1,
		Peers:             []uint64{},
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
	alpha, beta := mustTemp("alpha", a), mustTemp("beta", b)
	defer os.Remove(alpha)
	defer os.Remove(beta)
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

func mustTemp(pattern, data string) string {
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