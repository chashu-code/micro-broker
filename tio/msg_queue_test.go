package tio

import (
	"strconv"
	"testing"

	"github.com/streamrail/concurrent-map"
	"github.com/stretchr/testify/assert"
)

func Test_MsgQueueFIFO(t *testing.T) {
	count := 3
	q := NewMsgQueue(1)

	go func() {
		for i := 0; i < count; i++ {
			ok := q.Push(&Msg{Nav: strconv.Itoa(i)}, true)
			assert.True(t, ok)
		}
	}()

	for i := 0; i < count; i++ {
		v, ok := q.Pop(true)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i), v.Nav)
	}
}

func Test_MultiMsgQueuePoper_Pop(t *testing.T) {
	keys := []string{"two", "one"}

	mapOp := cmap.New()
	p := NewMultiMsgQueuePoper(mapOp, keys, 10, 1)

	v, _ := mapOp.Get("one")
	one := v.(*MsgQueue)

	v, _ = mapOp.Get("two")
	two := v.(*MsgQueue)

	msg, ok := p.Pop()

	assert.Nil(t, msg)
	assert.False(t, ok)

	two.C <- &Msg{Action: "two"}
	one.C <- &Msg{Action: "one"}

	msg, ok = p.Pop()
	assert.True(t, ok)
	assert.Contains(t, keys, msg.Action)

	msg, ok = p.Pop()
	assert.True(t, ok)
	assert.Contains(t, keys, msg.Action)

	_, ok = p.Pop()
	assert.False(t, ok)
}

func Test_MultiMsgQueuePoper_Clone(t *testing.T) {
	keys := []string{"two", "one"}
	mapOp := cmap.New()
	p := NewMultiMsgQueuePoper(mapOp, keys, 10, 1)

	v, _ := mapOp.Get("one")
	one := v.(*MsgQueue)

	v, _ = mapOp.Get("two")
	two := v.(*MsgQueue)

	pc := p.Clone()

	two.C <- &Msg{Action: "two"}
	one.C <- &Msg{Action: "one"}

	_, ok := pc.Pop()
	assert.True(t, ok)

	_, ok = pc.Pop()
	assert.True(t, ok)

}
