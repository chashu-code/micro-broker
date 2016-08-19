package work

import (
	"errors"
	"time"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/uber-go/zap"
)

// SubWorker 订阅处理工作器
type SubWorker struct {
	Worker
	// IP redis ip
	IP string
}

// SubWorkerRun 运行多个SubWorker
func SubWorkerRun(mgr *manage.Manager, ip string, count int) {
	for i := 0; i < count; i++ {
		w := &SubWorker{
			IP: ip,
		}
		go w.Run(mgr, "sub:"+ip, w.process)
	}
}

func (w *SubWorker) process() error {
	durPause := time.Duration(w.mgr.Conf.WrkPauseSecs) * time.Second
	pool, _, err := w.redisPoolMap.Fetch(w.IP, w.mgr.Conf.PoolSize)

	if err != nil { // 获取或构造 redis pool fail，暂缓一些时间
		time.Sleep(durPause)
		return err
	}

	res := pool.Cmd("blpop", w.mgr.Outbox(w.IP), w.mgr.Conf.PopTimeoutSecs)

	if res.IsType(redis.Nil) {
		// w.Log.Debug("redis blpop is nil")
		return errors.New("redis blpop empty")
	}

	lstBytes, err := res.ListBytes()

	if err != nil { // 无法转换为 bytes array
		w.Log.Error("redis res to bytes fail", zap.Error(err))
		return err
	}

	bts := lstBytes[1]
	msg, err := w.mgr.Unpack(bts)

	if err != nil {
		w.Log.Error("unpack msg fail", zap.Error(err))
		return err
	}

	if msg.IsDead() {
		w.Log.Warn("msg is dead", zap.Marshaler("msgHash", msg))
		return errors.New("msg is dead")
	}

	ok := w.mgr.MsgQ.Push(msg, true)
	if !ok {
		w.Log.Error("push msgQ timeout", zap.Marshaler("msgHash", msg))
		return errors.New("push msgQ timeout")
	}

	return nil
}
