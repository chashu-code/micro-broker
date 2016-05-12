package tio

import (
	"strconv"
	"testing"

	"github.com/streamrail/concurrent-map"
	"github.com/stretchr/testify/assert"
)

// 性能压测
func Benchmark_MsgQueue(b *testing.B) {

	count := b.N
	q := NewMsgQueue(1)

	go func() {
		for i := 0; i < count; i++ {
			q.Push(&Msg{}, true)
		}
	}()

	for i := 0; i < count; i++ {
		q.Pop(true)
	}
}

// 顺序写入读取
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

// Test_MultiMsgQueuePoper test
func Test_MultiMsgQueuePoper(t *testing.T) {
	mapOp := cmap.New()
	p := NewMultiMsgQueuePoper(mapOp, []string{"one", "two"})

	v, _ := mapOp.Get("one")
	one := v.(*MsgQueue)

	v, _ = mapOp.Get("two")
	two := v.(*MsgQueue)

	msg, ok := p.Pop()

	assert.Nil(t, msg)
	assert.False(t, ok)

	two.C <- &Msg{Action: "two"}
	one.C <- &Msg{Action: "one"}

	keysMay := []string{"two", "one"}

	msg, ok = p.Pop()
	assert.True(t, ok)
	assert.Contains(t, keysMay, msg.Action)

	msg, ok = p.Pop()
	assert.True(t, ok)
	assert.Contains(t, keysMay, msg.Action)

	_, ok = p.Pop()
	assert.False(t, ok)
}
