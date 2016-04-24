package sprite

import (
	"strconv"
	"testing"

	cmap "github.com/streamrail/concurrent-map"
	"github.com/stretchr/testify/assert"
)

// 性能压测
func Benchmark_MsgQueueOp(b *testing.B) {

	count := b.N
	c := make(chan *Msg)
	q := NewMsgQueueOp(c, 1)

	go func() {
		e := NewMsgQueueOpExecer()
		for i := 0; i < count; i++ {
			e.Push(q, &Msg{}, true)
		}
	}()

	e := NewMsgQueueOpExecer()
	for i := 0; i < count; i++ {
		e.Pop(q, true)
	}
}

// 顺序写入读取
func Test_MsgQueueOpFIFO(t *testing.T) {
	c := make(chan *Msg)
	count := 3
	q := NewMsgQueueOp(c, 1)

	go func() {
		e := NewMsgQueueOpExecer()
		for i := 0; i < count; i++ {
			ok := e.Push(q, &Msg{Channel: strconv.Itoa(i)}, true)
			assert.True(t, ok)
		}
	}()

	e := NewMsgQueueOpExecer()
	for i := 0; i < count; i++ {
		v, ok := e.Pop(q, true)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i), v.Channel)
	}
}

// MultiMsgQueueOpPoper test
func Test_MultiMsgQueueOpPoper(t *testing.T) {
	mapOp := cmap.New()
	p := NewMultiMsgQueueOpPoper(mapOp, []string{"one", "two"})

	v, _ := mapOp.Get("one")
	one := v.(*MsgQueueOp)

	v, _ = mapOp.Get("two")
	two := v.(*MsgQueueOp)

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
