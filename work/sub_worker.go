package work

import (
	"errors"
	"time"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/manage"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/uber-go/zap"
)

// SubWorker 订阅处理工作器
type SubWorker struct {
	Worker
	// destIP redis ip
	destIP string
	// subIP redis ip
	subIP string
}

// SubWorkerRun 运行多个SubWorker
func SubWorkerRun(mgr *manage.Manager, destIP string, count int) {
	for i := 0; i < count; i++ {
		w := &SubWorker{
			destIP: destIP,
			subIP:  adjustSubIP(mgr.IP(), destIP), // 兼容单机rb client
		}
		go w.Run(mgr, "sub:"+destIP, w.process)
	}
}

func adjustSubIP(mgrIP, destIP string) string {
	if mgrIP == destIP {
		return defaults.IPLocal
	}
	return mgrIP
}

func (w *SubWorker) resToMsg(res *redis.Resp) (*manage.Msg, error) {
	if res.IsType(redis.Nil) {
		return nil, nil
	}

	lstBytes, err := res.ListBytes()

	if err != nil { // 无法转换为 bytes array
		return nil, err
	}

	bts := lstBytes[1]
	msg, err := w.mgr.Unpack(bts)

	if err != nil {
		return nil, err
	}

	if msg.IsDead() {
		return msg, errors.New("msg is dead")
	}

	return msg, nil
}

func (w *SubWorker) process() {
	durPause := time.Duration(w.mgr.Conf.WrkPauseSecs) * time.Second
	pool := w.redisPoolMap.Fetch(w.destIP)

	if pool == nil { // 无法获取Pool
		w.Log.Warn("can't fetch redis pool")
		time.Sleep(durPause)
		return
	}

	res := pool.Cmd("blpop", w.mgr.Outbox(w.subIP), w.mgr.Conf.PopTimeoutSecs)

	msg, err := w.resToMsg(res)

	if err != nil {
		w.Log.Error("unexpected msg", msgPackField(msg), zap.Error(err))
		return
	}

	if msg == nil { // redis list empty
		return
	}

	ok := w.mgr.MsgQ.Push(msg, true)
	if !ok {
		w.Log.Error("push msgQ timeout", msgPackField(msg))
	}
}
