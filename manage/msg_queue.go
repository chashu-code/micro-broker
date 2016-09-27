package manage

import "time"

// MsgChan 消息 Channel
type MsgChan chan *Msg

// MsgQueue  存储 channel 和 超时信息
type MsgQueue struct {
	C          MsgChan
	DurTimeout time.Duration
}

// NewMsgQueueWithSize 构造一个 MsgQueue，可以指定缓冲大小
func NewMsgQueueWithSize(msTimeout int, size int) *MsgQueue {
	mq := make(MsgChan, size)
	durationTimeout := time.Millisecond * time.Duration(msTimeout)

	q := &MsgQueue{
		C:          mq,
		DurTimeout: durationTimeout,
	}
	return q
}

// Push 添加一个队列成员
func (q *MsgQueue) Push(msg *Msg, isBlock bool) bool {
	if isBlock {
		select {
		case q.C <- msg:
			return true
		case <-time.After(q.DurTimeout):
			return false
		}
	} else {
		select {
		case q.C <- msg:
			return true
		default:
			return false
		}
	}

}

// Pop 返回一个队列成员
func (q *MsgQueue) Pop(isBlock bool) (*Msg, bool) {
	if isBlock {
		select {
		case item := <-q.C:
			return item, true
		case <-time.After(q.DurTimeout):
			return nil, false
		}
	} else {
		select {
		case item := <-q.C:
			return item, true
		default:
			return nil, false
		}
	}
}
