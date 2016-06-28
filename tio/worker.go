package tio

import (
	"github.com/chashu-code/micro-broker/log"
	"github.com/chashu-code/micro-broker/manage"
)

// WorkerProcessFunc 处理方法类型
type WorkerProcessFunc func()

// Worker 基类
type Worker struct {
	log.Able
	manager manage.IManager
	client  IJobClient
	process WorkerProcessFunc
}

func (w *Worker) init(manager manage.IManager, client IJobClient, process WorkerProcessFunc) {
	w.manager = manager
	w.client = client
	w.process = process
}

// Run 运行JobPutWorker
func (w *Worker) Run() {
	defer func() {
		w.client.Close()
	}()

	for !w.manager.IsShutdown() {
		w.process()
	}
}
