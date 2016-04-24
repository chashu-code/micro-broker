package sprite

import (
	"reflect"
	"time"

	cmap "github.com/streamrail/concurrent-map"
)

// MsgQueueOp  存储 channel 和 超时信息
type MsgQueueOp struct {
	C          chan *Msg
	DurTimeout time.Duration
}

// MultiMsgQueueOpPoper 多个 MsgQueueOp 读取者
type MultiMsgQueueOpPoper struct {
	durTimeout time.Duration
	selCase    []reflect.SelectCase
	posTimeout int
}

// MsgQueueOpExecer MsgQueueOp 执行者（每个Goruntine一个）
type MsgQueueOpExecer struct {
}

// NewMsgQueueOp 构造一个 channel queue with timeout
func NewMsgQueueOp(c chan *Msg, msTimeout int) *MsgQueueOp {
	durationTimeout := time.Millisecond * time.Duration(msTimeout)

	q := &MsgQueueOp{
		C:          c,
		DurTimeout: durationTimeout,
	}
	return q
}

// NewMsgQueueOpExecer 构造 MsgQueueOp 执行者
func NewMsgQueueOpExecer() *MsgQueueOpExecer {
	return &MsgQueueOpExecer{}
}

// NewMultiMsgQueueOpPoper 构造多队列读取者
func NewMultiMsgQueueOpPoper(mapQueueOp cmap.ConcurrentMap, keys []string) *MultiMsgQueueOpPoper {
	poper := &MultiMsgQueueOpPoper{}

	size := len(keys)

	poper.posTimeout = size
	poper.durTimeout = time.Duration(MsgQueueOpTimeoutDefault) * time.Millisecond
	poper.selCase = make([]reflect.SelectCase, size+1)

	var op *MsgQueueOp
	var mq chan *Msg

	for i, key := range keys {
		op = nil
		if v, ok := mapQueueOp.Get(key); ok {
			op, _ = v.(*MsgQueueOp)
		}

		if op == nil {
			mq = make(chan *Msg, MsgQueueSizeDefault)
			op = NewMsgQueueOp(mq, MsgQueueOpTimeoutDefault)
			mapQueueOp.Set(key, op)
		} else {
			mq = op.C
		}

		poper.selCase[i].Dir = reflect.SelectRecv
		poper.selCase[i].Chan = reflect.ValueOf(mq)
	}

	poper.selCase[poper.posTimeout].Dir = reflect.SelectRecv
	return poper
}

// Pop 弹出有效获取的消息，若超时或失败，则返回 nil, false
func (p *MultiMsgQueueOpPoper) Pop() (*Msg, bool) {
	p.selCase[p.posTimeout].Chan = reflect.ValueOf(time.After(p.durTimeout))
	chosen, recv, recvOk := reflect.Select(p.selCase)
	if recvOk && chosen != p.posTimeout {
		return recv.Interface().(*Msg), true
	}
	return nil, false
}

// Push 添加一个队列成员
func (e *MsgQueueOpExecer) Push(op *MsgQueueOp, msg *Msg, isBlock bool) bool {
	if isBlock {
		select {
		case op.C <- msg:
			return true
		case <-time.After(op.DurTimeout):
			return false
		}
	}

	select {
	case op.C <- msg:
		return true
	default:
		return false
	}
}

// Pop 返回一个队列成员
func (e *MsgQueueOpExecer) Pop(op *MsgQueueOp, isBlock bool) (*Msg, bool) {
	if isBlock {
		select {
		case item := <-op.C:
			return item, true
		case <-time.After(op.DurTimeout):
			return nil, false
		}
	}

	select {
	case item := <-op.C:
		return item, true
	default:
		return nil, false
	}
}
