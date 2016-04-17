// Package queue 实现队列功能
package queue

import "container/list"
import "sync"

// Queue FIFO队列，队列为空时阻塞Pop
type Queue struct {
	list *list.List
	cond *sync.Cond
}

// New 构造一个Queue
func New() *Queue {
	m := new(sync.Mutex)
	q := &Queue{
		list: list.New(),
		cond: sync.NewCond(m),
	}
	return q
}

// Push 添加一个成员至队列尾
func (q *Queue) Push(item interface{}) {
	q.cond.L.Lock()
	q.list.PushBack(item)
	q.cond.L.Unlock()
	q.cond.Signal()
}

// Pop 从队列头部 pop 一个成员，若队列为空，则阻塞至有成员 push
func (q *Queue) Pop() interface{} {

	q.cond.L.Lock()

	if q.list.Len() == 0 {
		q.cond.Wait()
	}

	item := q.list.Remove(q.list.Front())

	q.cond.L.Unlock()

	return item
}

// Len 返回队列成员数
func (q *Queue) Len() int {
	return q.list.Len()
}
