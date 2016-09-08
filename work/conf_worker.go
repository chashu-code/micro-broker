package work

import (
	"errors"
	"time"

	"github.com/chashu-code/micro-broker/manage"
	rxpool "github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/uber-go/zap"
)

// ConfWorker 配置更新工作器
type ConfWorker struct {
	Worker
	// IP redis ip
	IP string
	// V Version
	V string
}

// ConfWorkerRun 运行1个 ConfWorkerRun
func ConfWorkerRun(mgr *manage.Manager, ip string, count int) {
	w := &ConfWorker{
		IP: ip,
	}
	go w.Run(mgr, "conf:"+ip, w.process)
}

func (w *ConfWorker) process() {
	durPause := time.Duration(w.mgr.Conf.WrkPauseSecs) * time.Second
	// 不管出错或是成功，都需要间歇一会
	time.Sleep(durPause)

	pool, _, err := w.redisPoolMap.FetchOrNew(w.IP, w.mgr.Conf.PoolSize)

	if err != nil { // 获取或构造 redis pool fail，暂缓一些时间
		w.Log.Error("get conf redis pool fail", zap.Error(err))
		return
	}

	w.processCrontab(pool)
}

func (w *ConfWorker) processCrontab(pool *rxpool.Pool) {
	tabName := w.mgr.CrontabName()
	res := pool.Cmd("hget", tabName, "v")

	v, err := w.resToV(res)

	if err != nil {
		w.Log.Warn("get crontab version fail", zap.Error(err))
		return
	}

	// 若版本信息一致，则不作处理
	if v == w.V {
		return
	}

	w.V = v

	// 无版本信息，清零
	if w.V == "" {
		w.Log.Info("no version, clear crontab job")
		w.mgr.Conf.CrontabJobDslMap = map[string]string{}
		return
	}

	// 有版本信息，尝试获取整个hash table
	res = pool.Cmd("hgetall", tabName)
	if mp, err := res.Map(); err == nil {
		w.mgr.Conf.CrontabJobDslMap = mp
		w.Log.Info("get crontab success", zap.Object("config", mp))
	} else {
		w.Log.Warn("get crontab fail", zap.Error(err))
	}
}

func (w *ConfWorker) resToV(res *redis.Resp) (string, error) {
	// 空，就当清零
	if res.IsType(redis.Nil) {
		return "", nil
	}

	v, err := res.Str()

	if err != nil {
		return "", errors.New("version parse fail: " + err.Error())
	}

	return v, nil
}
