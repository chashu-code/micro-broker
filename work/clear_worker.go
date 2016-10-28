package work

import (
	"strconv"
	"strings"
	"time"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/utils"
	"github.com/uber-go/zap"
)

// ClearWorker 清理工作器
type ClearWorker struct {
	Worker
	// IP redis ip
	IP string

	// clear
	clearCounter int
	// redis pool ping
	pingCounter int
}

// ClearWorkerRun 运行1个 ClearWorkerRun
func ClearWorkerRun(mgr *manage.Manager, ip string, count int) {
	w := &ClearWorker{
		IP: ip,
	}
	go w.Run(mgr, "clear:"+ip, w.process)
}

func (w *ClearWorker) process() {
	durPause := time.Duration(w.mgr.Conf.WrkPauseSecs) * time.Second
	time.Sleep(durPause)

	w.syncLog()
	w.clearResQueue()
	w.redisPoolPing()
}

// redisPoolPing 定时ping redis connection，预防超时
func (w *ClearWorker) redisPoolPing() {
	// 120s 处理一次
	w.pingCounter = w.pingCounter % 120

	if w.pingCounter == 0 {
		p := w.redisPoolMap.Fetch(w.IP)

		if p == nil {
			w.Log.Warn("redisPool unfound", zap.String("pool", w.IP))
			return
		}

		for i := 0; i < w.mgr.Conf.PoolSize; i++ {
			p.Cmd("PING")
		}
	}

	w.pingCounter++
}

// clearResQueue 清理redis应答队列（pid不存在的）
func (w *ClearWorker) clearResQueue() {
	// 30s 处理一次
	w.clearCounter = w.clearCounter % 30

	if w.clearCounter == 0 {
		p := w.redisPoolMap.Fetch(w.IP)

		if p == nil {
			w.Log.Warn("redisPool unfound", zap.String("pool", w.IP))
			return
		}

		res := p.Cmd("keys", "ms:inbox:[123456789]*")
		keys, err := res.List()
		if err != nil {
			w.Log.Warn("redis keys fail", zap.Error(err))
		} else {
			for _, key := range keys {
				if w.checkInboxPidExist(key) == 2 { // 不存在，则清除该List
					if res := p.Cmd("del", key); res.Err != nil {
						w.Log.Warn("del overdue list fail", zap.String("key", key), zap.Error(res.Err))
					} else {
						w.Log.Info("del overdue list", zap.String("key", key))
					}
				}
			}
		}
	}

	w.clearCounter++
}

// checkInboxPidExist 检测Pid是否存在 0 错误key，1 非pid， 2 不存在， 3 存在
func (w *ClearWorker) checkInboxPidExist(key string) int {
	sym := w.mgr.Inbox("")
	ss := strings.Split(key, sym)
	if len(ss) == 2 {
		pid, err := strconv.Atoi(ss[1])
		if err != nil {
			return 1
		}

		if utils.IsProcessExist(pid) {
			return 3
		}

		return 2
	}

	return 0

}

func (w *ClearWorker) syncLog() {
	if err := w.mgr.LogSync(); err != nil {
		w.Log.Error("sync log fail", zap.Error(err))
	}
}
