package progress

import "testing"

func TestProgressUpdate(t *testing.T) {
	prevAck, prevNext := uint64(3), uint64(5)
	cases := []struct {
		update  uint64
		expAck  uint64
		expNext uint64
	}{
		{prevAck - 1,prevAck,prevNext},
		{prevAck,prevAck,prevNext},
		{prevAck + 1,prevAck + 1,prevNext},
		{prevAck + 2,prevAck + 2,prevNext + 1},
	}
	for i, test := range cases {
		s := &Progress{
			Ack:  prevAck,
			Next: prevNext,
		}
		s.Update(test.update)
		if s.Ack != test.expAck {
			t.Errorf("#%d: prev ack= %d, expected %d", i, s.Ack, test.expAck)
		}
		if s.Next != test.expNext {
			t.Errorf("#%d: prev next= %d, expected %d", i, s.Next, test.expNext)
		}
	}
}

func TestProgressTryDec(t *testing.T) {
	cases := []struct {
		ack  uint64
		next    uint64
		ignored uint64
		last    uint64
		exp     bool
		expNext uint64
	}{
		{1,0,0,0,false,0 },
		{5,10,5,5,false,10 },
		{5,10,4,4,false,10 },
		{5,10,9,9,true,6 },
		{0,0,0,0,false,0 },
		{0,10,5,5,false,10 },
		{0,10,9,9,true,9 },
		{0,2,1,1,true,1 },
		{0,1,0,0,true,1 },
		{0,10,9,2,true,3 },
		{0,10,9,0,true,1 },
	}
	for i, test := range cases {
		s := &Progress{
			Ack:  test.ack,
			Next: test.next,
		}
		if rv := s.TryDecTo(test.ignored, test.last); rv != test.exp {
			t.Errorf("#%d: try dec to= %t, expected %t", i, rv, test.exp)
		}
		if s.Ack != test.ack {
			t.Errorf("#%d: ack= %d, expected %d", i, s.Ack, test.ack)
		}
		if s.Next != test.expNext {
			t.Errorf("#%d: next= %d, expected %d", i, s.Next, test.expNext)
		}
	}
}

func TestProgressNeedDelay(t *testing.T) {
	cases := []struct {
		ack   uint64
		delay int
		exp   bool
	}{
		{1,0,false},
		{1,1,false},
		{0,1,true},
		{0,0,false},
	}
	for i, test := range cases {
		s := &Progress{
			Ack:   test.ack,
			Delay: test.delay,
		}
		if rv := s.NeedDelay(); rv != test.exp {
			t.Errorf("#%d: need delay = %t, expected %t", i, rv, test.exp)
		}
	}
}

func TestProgressDelayReset(t *testing.T) {
	s := &Progress{
		Delay: 1,
	}
	s.TryDecTo(1, 1)
	if s.Delay != 0 {
		t.Errorf("delay= %d, expected 0", s.Delay)
	}
	s.Delay = 1
	s.Update(2)
	if s.Delay != 0 {
		t.Errorf("delay= %d, expected 0", s.Delay)
	}
}