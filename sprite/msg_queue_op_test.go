package sprite

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 性能压测
func Benchmark_MsgQueueOp(b *testing.B) {

	count := b.N
	c := make(chan *Msg)

	go func() {
		q := NewMsgQueueOp(c, 1)
		for i := 0; i < count; i++ {
			q.Push(&Msg{}, true)
		}
	}()

	q := NewMsgQueueOp(c, 1)
	for i := 0; i < count; i++ {
		q.Pop(true)
	}
}

// 顺序写入读取
func Test_MsgQueueOpFIFO(t *testing.T) {
	c := make(chan *Msg)
	count := 3

	go func() {
		q := NewMsgQueueOp(c, 1)
		for i := 0; i < count; i++ {
			ok := q.Push(&Msg{Channel: strconv.Itoa(i)}, true)
			assert.True(t, ok)
		}
	}()

	q := NewMsgQueueOp(c, 1)
	for i := 0; i < count; i++ {
		v, ok := q.Pop(true)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i), v.Channel)
	}
}
