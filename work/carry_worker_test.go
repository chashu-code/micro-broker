package work

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/pool"
	"github.com/stretchr/testify/assert"
)

func newCarryWorker() *CarryWorker {
	w := &CarryWorker{}
	w.mgr = newManager()
	w.redisPoolMap = pool.NewRedisPoolMap()
	w.beanPoolMap = pool.NewBeanPoolMap()
	return w
}

func Test_CarryWorker_pushMsg(t *testing.T) {
	w := newCarryWorker()
	msg := &manage.Msg{}
	sink := w.newSinkLog()
	destIP := defaults.IPLocal
	boxName := w.mgr.Inbox(destIP)
	w.pushMsg(destIP, boxName, msg)

	logHas(t, sink, "pack msg fail")
	msg.V = 1

	errDestIP := "127.0.0.1:6366"
	sink = w.newSinkLog()
	w.pushMsg(errDestIP, boxName, msg)
	logHas(t, sink, "pool fail")

	sink = w.newSinkLog()
	w.pushMsg(destIP, boxName, msg)
	assert.Empty(t, sink.Logs())
}

func Test_CarrayWorker_processWrongMsgQ(t *testing.T) {
	w := newCarryWorker()
	sink := w.newSinkLog()
	w.process()
	assert.Empty(t, sink.Logs())

	msg := &manage.Msg{
		Action: "unknow",
		Topic:  "test",
		V:      1,
	}
	w.mgr.MsgQ.Push(msg, false)
	w.process()
	logHas(t, sink, "can't carry")
}

func Test_CarrayWorker_processREQ(t *testing.T) {
	w := newCarryWorker()
	p, _, _ := w.redisPoolMap.FetchOrNew(defaults.IPLocal, 1)
	sink := w.newSinkLog()
	msg := &manage.Msg{
		Action: manage.ActReq,
		Topic:  "test",
		V:      1,
	}
	lstName := w.mgr.Inbox(msg.Topic)

	p.Cmd("del", lstName)
	w.mgr.MsgQ.Push(msg, false)
	w.process()
	logHas(t, sink, "req --->>")
	v, _ := p.Cmd("llen", lstName).Int()
	assert.Equal(t, 1, v)
	p.Cmd("del", lstName)
}

func Test_CarrayWorker_processRES(t *testing.T) {
	w := newCarryWorker()
	p, _, _ := w.redisPoolMap.FetchOrNew(defaults.IPLocal, 1)
	sink := w.newSinkLog()
	msg := &manage.Msg{
		Action: manage.ActRes,
		V:      1,
	}
	w.mgr.MsgQ.Push(msg, false)
	w.process()
	logHas(t, sink, "res <<---", "get pid fail")

	pid := "0"
	msg.RID = fmt.Sprintf("%s|%s", pid, "1234")
	lstName := w.mgr.Inbox(pid)
	p.Cmd("del", lstName)
	w.mgr.MsgQ.Push(msg, false)
	sink = w.newSinkLog()
	w.process()
	logHas(t, sink, "res <<---")
	v, _ := p.Cmd("llen", lstName).Int()
	assert.Equal(t, 1, v)
	p.Cmd("del", lstName)
}

func Test_CarrayWorker_processJOB(t *testing.T) {
	w := newCarryWorker()
	p, _, _ := w.beanPoolMap.FetchOrNew(defaults.IPLocal, defaults.DefaultJobPoolSize)
	assert.NotNil(t, p)
	// pack err => put job fail
	msg := &manage.Msg{
		Action: manage.ActJob,
		Topic:  "test",
		RID:    "1|xxxxx",
		V:      0,
	}
	sink := w.newSinkLog()
	w.mgr.MsgQ.Push(msg, false)
	w.process()

	// 即使投递失败，也会尝试应答
	logHas(t, sink, "put job fail", "error version", "job res <<---")

	// ok
	client := p.Get()
	defer p.Put(client)
	mInfo, _ := client.Stats(msg.TubeName())
	countOld, _ := strconv.Atoi(mInfo["current-jobs-ready"])

	msg.V = 1
	sink = w.newSinkLog()
	w.mgr.MsgQ.Push(msg, false)
	w.process()
	logNotHas(t, sink, "put job fail")
	logHas(t, sink, "job res <<---")
	mInfo, _ = client.Stats(msg.TubeName())
	countNew, _ := strconv.Atoi(mInfo["current-jobs-ready"])
	assert.Equal(t, countOld+1, countNew)
}
