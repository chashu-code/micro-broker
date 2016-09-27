package work

import (
	"testing"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/pool"
	"github.com/stretchr/testify/assert"
)

func newClearWorker() *ClearWorker {
	w := &ClearWorker{
		IP: defaults.IPLocal,
	}
	w.mgr = newManager()
	return w
}

func Test_ClearWorker_checkInboxPidExist(t *testing.T) {
	w := newClearWorker()
	key := "xxx"
	assert.Equal(t, 0, w.checkInboxPidExist(key))

	key = w.mgr.Inbox("hello")
	assert.Equal(t, 1, w.checkInboxPidExist(key))

	key = w.mgr.Inbox("10")
	assert.Equal(t, 2, w.checkInboxPidExist(key))

	key = w.mgr.Inbox("0")
	assert.Equal(t, 3, w.checkInboxPidExist(key))
}

func Test_ClearWorker_clearResQueue(t *testing.T) {
	w := newClearWorker()
	w.redisPoolMap = pool.NewRedisPoolMap()
	p, _, _ := w.redisPoolMap.FetchOrNew(w.IP, 1)
	sink := w.newSinkLog()

	p.Cmd("set", w.mgr.Inbox("test"), 0)
	p.Cmd("set", w.mgr.Inbox("0"), 0)
	p.Cmd("set", w.mgr.Inbox("10"), 0)

	w.clearResQueue()
	logHas(t, sink, "del overdue list", "ms:inbox:10")
	logNotHas(t, sink, "ms:inbox:0", "ms:inbox:test")

	p.Cmd("del", w.mgr.Inbox("test"))
	p.Cmd("del", w.mgr.Inbox("0"))
}
