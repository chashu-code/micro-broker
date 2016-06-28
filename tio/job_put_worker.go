package tio

import (
	"errors"
	"time"

	"github.com/chashu-code/micro-broker/log"
	"github.com/chashu-code/micro-broker/manage"
)

// JobPutWorker 推送job服务
type JobPutWorker struct {
	Worker
	queue *MsgQueue
}

// NewJobPutWorker 初始化
func NewJobPutWorker(manager manage.IManager, client IJobClient) *JobPutWorker {
	v, ok := manager.MapGet(manage.IDMapQueue, manage.KeyJobQueue)
	if !ok {
		panic(errors.New("JobPutWorker need job queue"))
	}
	w := &JobPutWorker{}
	w.queue = v.(*MsgQueue)
	w.init(manager, client, w.process)

	return w
}

func (w *JobPutWorker) process() {
	defer func() {
		if err := recover(); err != nil {
			w.Log(log.Fields{
				"error": err,
			}).Error("JobPutWorker catch error")
		}
	}()

	msg, ok := w.queue.Pop(true)
	if !ok {
		return
	}

	_, err := w.client.Put(msg.Service, msg.Data, 1, 0, time.Minute)
	if err != nil {
		w.Log(log.Fields{
			"tube":  msg.Service,
			"error": err,
		}).Error("JobPutWorker put fail")
		return
	}

	_, tid, _ := msg.BTRids()
	if v, ok := w.manager.MapGet(manage.IDMapQueue, tid); ok {
		if queue, ok := v.(*MsgQueue); ok {
			msgRes := &Msg{
				Action: CmdResRecv,
				From:   msg.From,
				Code:   "0",
			}
			if !queue.Push(msgRes, false) {
				w.Log(log.Fields{
					"tube":   msg.Service,
					"tidRes": tid,
				}).Error("JobPutWorker res timeout")
			}
			return
		}
	}

	w.Log(log.Fields{
		"tube":   msg.Service,
		"tidRes": tid,
	}).Error("JobPutWorker unfound res queue")

}
