package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// 性能压测
func Benchmark_PushPop(b *testing.B) {
	q := New()

	count := (int)(b.N / 2)
	go func() {

		for i := 0; i < count; i++ {
			q.Push(i)
		}
	}()

	go func() {
		for i := count; i < b.N; i++ {
			q.Push(i)
		}
	}()

	for i := 0; i < b.N; i++ {
		q.Pop()
	}
}

// 顺序写入读取
func Test_FIFO(t *testing.T) {
	q := New()

	for i := 0; i < 3; i++ {
		q.Push(i)
	}

	for i := 0; i < 3; i++ {
		v := q.Pop().(int)
		assert.Equal(t, i, v)
	}
}

// Pop 阻塞
func Test_PopBlocking(t *testing.T) {
	q := New()

	count := 0

	go func() {
		count += q.Pop().(int)
		count += q.Pop().(int)
		count += q.Pop().(int)
		count++
	}()

	for i := 0; i < 2; i++ {
		q.Push(i)
	}

	time.Sleep(1 * time.Millisecond)

	assert.Equal(t, 0, q.Len())
	assert.Equal(t, 1, count)
}
