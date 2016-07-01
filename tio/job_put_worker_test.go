package tio

import (
	"testing"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/stretchr/testify/assert"
)

func Test_JobPutWorkerExec(t *testing.T) {
	jobClient := &MockJobClient{}
	jobQueue := NewMsgQueueWithSize(10, JobPutWorkerSizeDefault)
	resQueue := NewMsgQueueWithSize(10, JobPutWorkerSizeDefault)
	m := &manage.Manager{}
	m.Init("test")

	// need define job queue
	assert.Panics(t, func() {
		NewJobPutWorker(m, jobClient)
	})

	m.MapSet(manage.IDMapQueue, manage.KeyJobQueue, jobQueue)
	m.MapSet(manage.IDMapQueue, "test-put", resQueue)

	worker := NewJobPutWorker(m, jobClient)
	go worker.Run()

	// put fail
	msgFail := &Msg{
		Service: "error",
		Data:    []byte("hello"),
		From:    "@unfound@",
	}
	jobQueue.Push(msgFail, true)

	// unfound res queue
	msg := msgFail.Clone()
	msg.Service = "success"
	jobQueue.Push(msg, true)

	//found res queue
	msgOK := msg.Clone()
	msgOK.UpdateFrom("bid", "test-put", "1")
	jobQueue.Push(msgOK, true)

	_, ok := resQueue.Pop(true)
	assert.True(t, ok)
	_, ok = resQueue.Pop(true)
	assert.False(t, ok)
	assert.Equal(t, 3, jobClient.putTimes)
}
