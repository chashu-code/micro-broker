package tio

import (
	"errors"
	"time"

	"github.com/chashu-code/micro-broker/log"
	"github.com/chashu-code/micro-broker/manage"
	"github.com/kr/beanstalk"
)

// JobPutWorker 推送job服务
type JobPutWorker struct {
	log.Able
	manager manage.IManager
	queue   *MsgQueue
	c       *beanstalk.Conn
	addr    string
}

// NewJobPutWorker 初始化
func NewJobPutWorker(manager manage.IManager) *JobPutWorker {
	w := &JobPutWorker{
		manager: manager,
	}

	v, ok := manager.MapGet(manage.IDMapQueue, KeyJobQueue)
	if !ok {
		panic(errors.New("JobPutWorker need job queue"))
	}
	w.queue = v.(*MsgQueue)

	return w
}

// Run 运行JobPutWorker
func (w *JobPutWorker) Run() {
	for !w.manager.IsShutdown() {
		w.checkThenPut()
	}
}

func (w *JobPutWorker) checkThenPut() {
	defer func() {
		if err := recover(); err != nil {
			w.Log(log.Fields{
				"error": err,
			}).Error("JobPutWorker catch error")
		}
	}()

	c := w.client()
	if c == nil {
		time.Sleep(time.Second)
		return
	}

	msg, ok := w.queue.Pop(true)
	if !ok {
		return
	}

	c.Tube.Name = msg.Service
	_, err := c.Put([]byte(msg.Data), 1, 0, time.Minute)
	if err != nil {
		w.Log(log.Fields{
			"tube":  msg.Service,
			"error": err,
		}).Error("JobPutWorker put fail")

		w.closeClient()
		return
	}

	_, tid, _ := msg.btrIDS()
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

func (w *JobPutWorker) client() *beanstalk.Conn {
	if w.c == nil {
		c, err := beanstalk.Dial("tcp", AddrJobServerDefault)
		if err != nil {
			w.Log(log.Fields{
				"error": err,
			}).Error("JobPutWorker connect beanstalk fail")
		} else {
			w.c = c
		}
	}
	return w.c
}

func (w *JobPutWorker) closeClient() {
	if w.c != nil {
		w.c.Close()
	}
	w.c = nil
}
