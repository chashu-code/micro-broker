package work

import (
	"fmt"
	"testing"
	"time"

	"github.com/chashu-code/micro-broker/pool"
	"github.com/stretchr/testify/assert"
)

func newCrontabWorker() *CrontabWorker {
	w := &CrontabWorker{}
	w.mgr = newManager()
	return w
}

func Test_CrontabWorker_hmstoi(t *testing.T) {
	// w := newCrontabWorker()
	// wrong format
	for _, s := range []string{"", "1:2", "1:2:e"} {
		v := hmstoi(s)
		assert.Equal(t, 0, v)
	}

	// right format
	checks := map[string]int{
		"1:2:03":   60*60 + 2*60 + 3,
		"02:00:00": 2 * 60 * 60,
	}

	for s, c := range checks {
		v := hmstoi(s)
		assert.Equal(t, c, v)
	}
}

func Test_CrontabWorker_dslToJob(t *testing.T) {
	checks := map[string]*CrontabJob{
		"":    nil,
		"12k": nil,
		"12m": &CrontabJob{
			Tube:     "test",
			Interval: int((time.Duration(12*60) * time.Second).Seconds()),
		},
		"12m|22": nil,
		"333s|00:01:00,00:02:01": &CrontabJob{
			Tube:       "test",
			Interval:   int((333 * time.Second).Seconds()),
			LimitStart: hmstoi("00:01:00"),
			LimitEnd:   hmstoi("00:02:01"),
		},
	}

	for dsl, c := range checks {
		job := dslToJob("test", dsl)
		if c == nil {
			assert.Nil(t, job)
		} else {
			assert.Equal(t, c.Tube, job.Tube)
			assert.Equal(t, c.Interval, job.Interval)
			assert.Equal(t, c.LimitStart, job.LimitStart)
			assert.Equal(t, c.LimitEnd, job.LimitEnd)
		}
	}
}

func Test_CrontabWorker_updateJobs(t *testing.T) {
	w := newCrontabWorker()
	w.V = "1"
	// v no change
	sink := w.newSinkLog()
	w.mgr.Conf.CrontabJobDslMap = map[string]string{
		"v":     "1",
		"test1": "10m",
		"test2": "12s|12:00:01,13:00:03",
	}
	assert.False(t, w.updateJobs())
	logNotHas(t, sink, "update crontab jobs")

	// v change
	w.V = "0"
	sink = w.newSinkLog()
	assert.True(t, w.updateJobs())
	assert.Equal(t, "1", w.V)
	logHas(t, sink, "update crontab jobs")
	logHas(t, sink, "add CrontabJob", "test1", "test2")

	w.mgr.Conf.CrontabJobDslMap = map[string]string{
		"v":     "0",
		"test1": "10m|",
		"test2": "12s|12:00:01,13:00",
	}
	sink = w.newSinkLog()
	assert.True(t, w.updateJobs())
	assert.Equal(t, "0", w.V)
	logHas(t, sink, "wrong CrontabJob DSL", "test1", "test2")
	logNotHas(t, sink, "add CrontabJob")

	// clear
	w.mgr.Conf.CrontabJobDslMap = map[string]string{}
	sink = w.newSinkLog()
	assert.True(t, w.updateJobs())
	assert.Equal(t, "", w.V)
	logHas(t, sink, "update crontab jobs")

}

func Test_CrontabWorker_logPuts(t *testing.T) {
	w := newCrontabWorker()
	sink := w.newSinkLog()

	now := time.Unix(1257897600, 0).UTC() // 2009-11-11 00:00:00 +0000 UTC

	w.mgr.Conf.CrontabJobDslMap = map[string]string{
		"v":     "1",
		"test1": "10s",
		"test2": "5s|0:1:00,0:1:30",
	}

	w.updateJobs()
	assert.NotEmpty(t, w.jobs)

	// all job PutTimes => 0
	w.logPuts(now)
	logNotHas(t, sink, "statistics")

	w.jobs[0].PutTimes = 1

	// <10s
	w.logPuts(now)
	logNotHas(t, sink, "statistics")

	// 10s
	w.logPuts(now.Add(10 * time.Second))
	logHas(t, sink, "statistics", "test1")

	assert.Equal(t, 0, w.jobs[0].PutTimes)
}

func Test_CrontabWorker_getWrkJobs(t *testing.T) {
	w := newCrontabWorker()
	w.newSinkLog()

	now := time.Unix(1257897600, 0).UTC() // 2009-11-11 00:00:00 +0000 UTC

	// len = 0
	assert.Empty(t, w.getWrkJobs(now))

	w.mgr.Conf.CrontabJobDslMap = map[string]string{
		"v":     "1",
		"test1": "10s",
		"test2": "5s|0:1:00,0:1:30",
	}
	assert.True(t, w.updateJobs())

	// part
	jobs := w.getWrkJobs(now)
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, "test1", jobs[0].Tube)

	// 5s pass, no jobs
	jobs = w.getWrkJobs(now.Add(5 * time.Second))
	assert.Empty(t, jobs)
	// 10s pass, 1 jobs
	jobs = w.getWrkJobs(now.Add(10 * time.Second))
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, "test1", jobs[0].Tube)
	// pass 1 minute
	jobs = w.getWrkJobs(now.Add(1 * time.Minute))
	assert.Equal(t, 2, len(jobs))
	assert.Equal(t, "test2", jobs[0].Tube)
	assert.Equal(t, "test1", jobs[1].Tube)
	// pass 1 minute 3 seconds
	jobs = w.getWrkJobs(now.Add(1 * time.Minute).Add(3 * time.Second))
	assert.Empty(t, jobs)
	// pass 1 minute 5 seconds
	jobs = w.getWrkJobs(now.Add(1 * time.Minute).Add(5 * time.Second))
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, "test2", jobs[0].Tube)
	// pass 1 minute 10 seconds
	jobs = w.getWrkJobs(now.Add(1 * time.Minute).Add(10 * time.Second))
	assert.Equal(t, 2, len(jobs))
	assert.Equal(t, "test2", jobs[0].Tube)
	assert.Equal(t, "test1", jobs[1].Tube)
	// pass 2 minute
	jobs = w.getWrkJobs(now.Add(2 * time.Minute))
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, "test1", jobs[0].Tube)
}

func Test_CrontabWorker_processWithTime(t *testing.T) {
	w := newCrontabWorker()
	w.beanPoolMap = pool.NewBeanPoolMap()
	sink := w.newSinkLog()
	now := time.Now()

	// tube 无job，推送成功
	w.mgr.Conf.CrontabJobDslMap = map[string]string{
		"v": "1",
	}

	key := fmt.Sprintf("test-%v", now.Unix())
	w.mgr.Conf.CrontabJobDslMap[key] = "10s"

	w.processWithTime(now)
	logHas(t, sink, "put statistics")

	// tube 有job，不推送
	sink = w.newSinkLog()
	w.processWithTime(now.Add(11 * time.Second))
	logNotHas(t, sink, "put statistics")
}
