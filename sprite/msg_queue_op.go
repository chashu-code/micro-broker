package sprite

import "time"

// MsgQueueOp  Goroutine 以 Queue 的方式操作 channel
type MsgQueueOp struct {
	C               chan *Msg
	T               *time.Timer
	durationTimeout time.Duration
}

// NewMsgQueueOp 构造一个 channel queue
func NewMsgQueueOp(c chan *Msg, msTimeout int) *MsgQueueOp {
	durationTimeout := time.Millisecond * time.Duration(msTimeout)

	q := &MsgQueueOp{
		C:               c,
		T:               time.NewTimer(durationTimeout),
		durationTimeout: durationTimeout,
	}
	return q
}

// ResetTimer 重置计时器
func (q *MsgQueueOp) ResetTimer() {
	q.T.Reset(q.durationTimeout)
}

// Push 添加一个队列成员
func (q *MsgQueueOp) Push(msg *Msg, isBlock bool) bool {
	if isBlock {
		q.ResetTimer()
		select {
		case q.C <- msg:
			return true
		case <-q.T.C:
			return false
		}
	}

	select {
	case q.C <- msg:
		return true
	default:
		return false
	}
}

// Pop 返回一个队列成员
func (q *MsgQueueOp) Pop(isBlock bool) (*Msg, bool) {
	if isBlock {
		q.ResetTimer()
		select {
		case item := <-q.C:
			return item, true
		case <-q.T.C:
			return nil, false
		}
	}

	select {
	case item := <-q.C:
		return item, true
	default:
		return nil, false
	}
}
