package manage

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MsgQueueFIFO_Block(t *testing.T) {
	isBlock := true
	count := 2
	q := NewMsgQueueWithSize(1, count)

	go func() {
		for i := 0; i < count; i++ {
			ok := q.Push(&Msg{Nav: strconv.Itoa(i)}, isBlock)
			assert.True(t, ok)
		}
	}()

	for i := 0; i < count; i++ {
		v, ok := q.Pop(isBlock)
		assert.True(t, ok)
		assert.Equal(t, strconv.Itoa(i), v.Nav)
	}
}

func Test_MsgQueue_Empty_And_Full(t *testing.T) {
	bs := []bool{true, false}
	for _, isBlock := range bs {
		count := 1
		q := NewMsgQueueWithSize(1, count)

		msg, ok := q.Pop(isBlock)
		assert.False(t, ok)
		assert.Nil(t, msg)

		for i := 0; i < 2; i++ {
			ok := q.Push(&Msg{Nav: strconv.Itoa(i)}, isBlock)
			if i > 0 {
				assert.False(t, ok)
			} else {
				assert.True(t, ok)
			}
		}

		for i := 0; i < 2; i++ {
			msg, ok := q.Pop(isBlock)
			if i > 0 {
				assert.Nil(t, msg)
				assert.False(t, ok)
			} else {
				assert.NotNil(t, msg)
				assert.True(t, ok)
			}
		}

	}
}
