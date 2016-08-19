package work

import (
	"errors"
	"strconv"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/pool"
	rxpool "github.com/mediocregopher/radix.v2/pool"
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

func (w *CarryWorker) process() error {
	msg, ok := w.mgr.MsgQ.Pop(true)
	if !ok {
		return errors.New("msgQ empty")
	}

	logMsg := func(info string) {
		w.Log.Info(info, zap.Marshaler("msgHash", msg))
	}

	switch msg.Action {
	case manage.ActReq:
		msg.FillWithReq(w.mgr)
		logMsg("req --->>")
		return w.processReq(msg)
	case manage.ActRes:
		logMsg("res <<---")
		return w.processRes(msg)
	case manage.ActJob:
		msg.FillWithReq(w.mgr)
		logMsg("job --->>")
		return w.processJob(msg)
	default:
		logMsg("?? --- can't carry")
		return nil
	}
}

func (w *CarryWorker) processReq(msg *manage.Msg) error {
	// TODO router / connect
	return w.pushMsg(defaults.IPLocal, msg.Topic, msg)
}

func (w *CarryWorker) processJob(msg *manage.Msg) error {
	// TODO router
	p, _, err := w.beanPoolMap.Fetch(defaults.IPLocal, w.mgr.Conf.JobPoolSize)
	if err != nil {
		w.Log.Error("fetch local job pool fail", zap.Error(err))
		return err
	}

	err = p.With(func(c *pool.BeanClient) error {
		pri, delay, ttr, errWith := c.PutArgsWithCode(msg.Code)
		if errWith != nil {
			w.Log.Warn("put job code fail, use default.")
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

	if err != nil {
		w.Log.Error("put job fail", zap.Error(err))
		return err
	}

	msgRes := msg.Clone(manage.ActRes)
	msgRes.Data = "ok"
	w.Log.Info("job res <<---", zap.Marshaler("msgHash", msgRes))
	return w.processRes(msgRes)
}

func (w *CarryWorker) processRes(msg *manage.Msg) error {
	pid, err := msg.PidOfRID()
	if err != nil {
		w.Log.Error("get pid fail", zap.Error(err))
		return err
	}
	return w.pushMsg(defaults.IPLocal, pid, msg)
}

func (w *CarryWorker) pushMsg(destIP, boxName string, msg *manage.Msg) error {

	bts, err := w.mgr.Pack(msg)
	if err != nil {
		w.Log.Error("pack msg fail", zap.Error(err))
		return err
	}
	var pool *rxpool.Pool
	pool, _, err = w.redisPoolMap.Fetch(destIP, w.mgr.Conf.PoolSize)

	if err != nil {
		w.Log.Error("fetch "+destIP+" pool fail", zap.Error(err))
		return err
	}

	res := pool.Cmd("rpush", w.mgr.Inbox(boxName), bts)

	if res.Err != nil {
		w.Log.Error("redis rpush fail", zap.Error(err))
		return res.Err
	}

	return nil
}
