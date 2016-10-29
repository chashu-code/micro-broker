package work

import (
	"strconv"

	rxpool "github.com/mediocregopher/radix.v2/pool"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/pool"
	"github.com/uber-go/zap"
)

// CarryWorker 消息搬运工作器
type CarryWorker struct {
	Worker
}

// CarryWorkerRun 运行多个CarryWorker
func CarryWorkerRun(mgr *manage.Manager, ip string, count int) {
	for i := 0; i < count; i++ {
		w := &CarryWorker{}
		go w.Run(mgr, "carry:"+strconv.Itoa(i), w.process)
	}
}

func (w *CarryWorker) logMsg(info string, msg *manage.Msg) {
	w.Log.Info(info, msgPackField(msg))
}

func (w *CarryWorker) process() {
	msg, ok := w.mgr.MsgQ.Pop(true)
	if !ok {
		return
	}

	switch msg.Action {
	case manage.ActReq:
		w.processReq("req --->>", msg)
	case manage.ActRes:
		w.processRes("res <<---", msg)
	case manage.ActJob:
		w.processJob("job --->>", msg)
	default:
		w.logMsg("?? --- can't carry", msg)
	}
}

func (w *CarryWorker) processReq(log string, msg *manage.Msg) {
	// TODO router / connect
	msg.FillWithReq(w.mgr)
	msg.BID = defaults.IPLocal // 暂时仅支持本机
	w.logMsg(log, msg)
	w.pushMsg(defaults.IPLocal, msg.Topic, msg)
}

func (w *CarryWorker) processJob(log string, msg *manage.Msg) {
	// TODO router
	msg.FillWithReq(w.mgr)
	w.logMsg(log, msg)

	p, _, err := w.beanPoolMap.FetchOrNew(defaults.IPLocal, w.mgr.Conf.JobPoolSize)
	if err != nil {
		w.Log.Error("fetch local job pool fail", zap.Error(err))
		return
	}

	err = p.With(func(c *pool.BeanClient) error {
		pri, delay, ttr, errWith := msg.CodeToPutArgs()
		if errWith != nil {
			w.Log.Warn("put job code fail, use the default")
		}

		var bts []byte
		bts, errWith = w.mgr.Pack(msg)
		if errWith != nil {
			return errWith
		}
		_, errWith = c.Put(msg.TubeName(), bts, pri, delay, ttr)
		if errWith != nil {
			return errWith
		}
		return nil
	})

	msgRes := msg.Clone(manage.ActRes)

	if err != nil {
		msgRes.Code = "500"
		msgRes.Data = "put job fail:" + err.Error()
	} else {
		msgRes.Data = "ok"
	}

	w.processRes("job res <<---", msgRes)
}

func (w *CarryWorker) processRes(log string, msg *manage.Msg) {
	w.logMsg(log, msg)
	pid, err := msg.PidOfRID()
	if err != nil {
		w.Log.Error("get pid fail", zap.Error(err))
		return
	}
	w.pushMsg(defaults.IPLocal, pid, msg)
}

func (w *CarryWorker) pushMsg(destIP, boxName string, msg *manage.Msg) {

	bts, err := w.mgr.Pack(msg)
	if err != nil {
		w.Log.Error("pack msg fail", zap.Error(err))
		return
	}
	var pool *rxpool.Pool
	pool, _, err = w.redisPoolMap.FetchOrNew(destIP, w.mgr.Conf.PoolSize)

	if err != nil {
		w.Log.Error("fetch "+destIP+" pool fail", zap.Error(err))
		return
	}

	res := pool.Cmd("rpush", w.mgr.Inbox(boxName), bts)

	if res.Err != nil {
		w.Log.Error("redis rpush fail", zap.Error(res.Err), msgPackField(msg))
	}
}
