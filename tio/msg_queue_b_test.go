package tio

import "testing"

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
