package work

import (
	"time"

	"github.com/chashu-code/micro-broker/manage"
	rxpool "github.com/mediocregopher/radix.v2/pool"
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
	go w.Run(mgr, "conf", w.process)
}

func (w *ConfWorker) process() error {
	durPause := time.Duration(w.mgr.Conf.WrkPauseSecs) * time.Second
	// 不管出错或是成功，都需要间歇一会
	time.Sleep(durPause)

	w.processSync()

	pool, _, err := w.redisPoolMap.Fetch(w.IP, w.mgr.Conf.PoolSize)

	if err != nil { // 获取或构造 redis pool fail，暂缓一些时间
		w.Log.Error("get conf redis pool fail", zap.Error(err), zap.String("confIP", w.IP))
		return err
	}

	w.processCrontab(pool)

	return nil
}

func (w *ConfWorker) processSync() error {
	if err := w.mgr.LogSync(); err != nil {
		w.Log.Error("sync log fail", zap.Error(err))
		return err
	}
	return nil
}

func (w *ConfWorker) processCrontab(pool *rxpool.Pool) error {
	tabName := w.mgr.CrontabName()
	res := pool.Cmd("hget", tabName, "v")
	if v, err := res.Str(); err != nil {
		w.Log.Warn("get crontab version fail.", zap.Error(err))
		return err
	} else {
		if v == w.V {
			// 版本一致
			return nil
		}
		w.V = v
	}

	// 成功获取，更新配置
	res = pool.Cmd("hgetall", tabName)
	if mp, err := res.Map(); err == nil {
		w.mgr.Conf.CrontabJobDslMap = mp
		w.Log.Info("get crontab success", zap.Object("config", mp))
	} else {
		w.Log.Warn("get crontab fail", zap.Error(err))
		return err
	}

	return nil
}
