package sprite

import "time"

// MsgQueueOp  Goroutine 以 Queue 的方式操作 channel
type MsgQueueOp struct {
	C               chan *Msg
	timer           *time.Timer
	durationTimeout time.Duration
}

// NewMsgQueueOp 构造一个 channel queue
func NewMsgQueueOp(c chan *Msg, msTimeout int) *MsgQueueOp {
	durationTimeout := time.Millisecond * time.Duration(msTimeout)

	q := &MsgQueueOp{
		C:               c,
		timer:           time.NewTimer(durationTimeout),
		durationTimeout: durationTimeout,
	}
	return q
}

// Push 添加一个队列成员
func (q *MsgQueueOp) Push(msg *Msg, isBlock bool) bool {
	if isBlock {
		q.timer.Reset(q.durationTimeout)
		select {
		case q.C <- msg:
			return true
		case <-q.timer.C:
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
		q.timer.Reset(q.durationTimeout)
		select {
		case item := <-q.C:
			return item, true
		case <-q.timer.C:
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
