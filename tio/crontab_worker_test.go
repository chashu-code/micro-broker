package tio

import (
	"errors"
	"testing"
	"time"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/stretchr/testify/assert"
)

type MockJobClient struct {
	putTimes   int
	statsTimes int
	statsReady chan bool
}

func (c *MockJobClient) Close() {
}

func (c *MockJobClient) Put(tube string, body []byte, pri uint32, delay, ttr time.Duration) (uint64, error) {
	c.putTimes++
	if tube == "error" {
		return 0, errors.New("sorry")
	}
	return 1, nil
}

func (c *MockJobClient) Stats(tube string) (map[string]string, error) {
	c.statsReady <- true
	c.statsTimes++
	stats := map[string]string{
		"current-jobs-ready":    "0",
		"current-jobs-reserved": "0",
	}
	if tube == "busy" {
		stats["current-jobs-ready"] = "1"
		return stats, nil
	}
	return stats, nil
}

func Test_CrontabWorkerExec(t *testing.T) {
	jobClient := &MockJobClient{
		statsReady: make(chan bool, 0),
	}
	m := &manage.Manager{}
	m.Init("test")

	worker := NewCrontabWroker(m, jobClient, 1000)
	go worker.Run()

	jobs := CrontabJobList{
		&CrontabJob{Tube: "busy", Interval: 1},
		&CrontabJob{Tube: "success", Interval: 1},
	}
	worker.UpdateJobs(jobs)

	for i := 0; i < 5; i++ {
		<-jobClient.statsReady
	}

	assert.True(t, 2 <= jobClient.putTimes)
	assert.True(t, 4 <= jobClient.statsTimes)
}
