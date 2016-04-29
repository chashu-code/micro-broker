package tio

import (
	"reflect"
	"sync"
	"time"

	"github.com/streamrail/concurrent-map"
)

// MsgChan 消息 Channel
type MsgChan chan *Msg

// MsgQueue  存储 channel 和 超时信息
type MsgQueue struct {
	C          MsgChan
	DurTimeout time.Duration
}

// NewMsgQueue 构造一个 MsgQueue
func NewMsgQueue(msTimeout int) *MsgQueue {
	return NewMsgQueueWithSize(msTimeout, MsgQueueSizeDefault)
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

// MultiMsgQueuePoper 多个 MsgQueue 读取者
type MultiMsgQueuePoper struct {
	timeoutPop time.Duration
	selCase    []reflect.SelectCase
	posTimer   int
}

var mutexQueueBuild sync.Mutex

// NewMultiMsgQueuePoper 构造多队列读取者
func NewMultiMsgQueuePoper(mapQueue cmap.ConcurrentMap, keys []string) *MultiMsgQueuePoper {
	poper := &MultiMsgQueuePoper{}

	size := len(keys)

	poper.posTimer = size
	poper.timeoutPop = time.Duration(MsgQueueOpTimeoutDefault) * time.Millisecond
	poper.selCase = make([]reflect.SelectCase, size+1)

	var op *MsgQueue
	var mq MsgChan

	mutexQueueBuild.Lock()
	defer mutexQueueBuild.Unlock()

	for i, key := range keys {
		op = nil
		if v, ok := mapQueue.Get(key); ok {
			op, _ = v.(*MsgQueue)
		}

		if op == nil {
			op = NewMsgQueue(MsgQueueOpTimeoutDefault)
			mapQueue.Set(key, op)
		}

		mq = op.C

		poper.selCase[i].Dir = reflect.SelectRecv
		poper.selCase[i].Chan = reflect.ValueOf(mq)
	}

	poper.selCase[poper.posTimer].Dir = reflect.SelectRecv
	return poper
}

// Pop 弹出有效获取的消息，若超时或失败，则返回 nil, false
func (p *MultiMsgQueuePoper) Pop() (*Msg, bool) {
	p.selCase[p.posTimer].Chan = reflect.ValueOf(time.After(p.timeoutPop))
	chosen, recv, recvOk := reflect.Select(p.selCase)
	if recvOk && chosen != p.posTimer {
		return recv.Interface().(*Msg), true
	}
	return nil, false
}
