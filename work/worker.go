package work

import (
	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/pool"
	"github.com/uber-go/zap"
)

// WrkProcFn 工作器处理回调
type WrkProcFn func() error

// Worker 工作器基类
type Worker struct {
	redisPoolMap *pool.RedisPoolMap
	beanPoolMap  *pool.BeanPoolMap

	mgr     *manage.Manager
	process WrkProcFn

	Log zap.Logger
}

// Run 运行工作器
func (w *Worker) Run(mgr *manage.Manager, wrkName string, proc WrkProcFn) {
	w.mgr = mgr
	w.process = proc
	w.Log = mgr.Log.With(
		zap.String("wrk", wrkName),
	)

	if pmap, ok := mgr.RedisPoolMap.(*pool.RedisPoolMap); ok {
		w.redisPoolMap = pmap
	}

	if pmap, ok := mgr.BeanPoolMap.(*pool.BeanPoolMap); ok {
		w.beanPoolMap = pmap
	}

	w.Log.Debug("worker run")

	mgr.WaitAdd()

	defer func() {
		w.Log.Debug("worker stop")
		mgr.WaitDone()
	}()

	for !mgr.IsShutdown() {
		w.process()
	}
}
