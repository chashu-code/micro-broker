package tio

import (
	"sort"
	"time"

	"github.com/chashu-code/micro-broker/manage"
)

// CrontabJob 定时任务
type CrontabJob struct {
	Tube       string
	Interval   int
	WillWorkAt int64
}

type CrontabJobList []*CrontabJob

func (l CrontabJobList) Len() int           { return len(l) }
func (l CrontabJobList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l CrontabJobList) Less(i, j int) bool { return l[i].WillWorkAt < l[j].WillWorkAt }

// CrontabWroker 定时推送工作器
type CrontabWroker struct {
	Worker

	jobs        CrontabJobList
	durInterval time.Duration
	data        []byte
}

// NewCrontabWroker 构造新的定时推送工作器
func NewCrontabWroker(manager manage.IManager, client IJobClient, msInterval int) *CrontabWroker {
	w := &CrontabWroker{
		durInterval: time.Duration(msInterval) * time.Millisecond,
	}

	protocol := &MPProtocol{}
	if data, err := protocol.Marshal("broker-crontab"); err != nil {
		panic(err)
	} else {
		w.data = data
	}

	w.init(manager, client, w.process)
	return w
}

func (w *CrontabWroker) UpdateJobs(jobs CrontabJobList) {
	w.jobs = jobs
	sort.Sort(w.jobs)
}

func (w *CrontabWroker) process() {
	time.Sleep(w.durInterval)

	jobs := w.jobs
	if len(jobs) < 1 {
		return
	}

	now := time.Now().Unix()

	for _, item := range jobs {
		if item.WillWorkAt > now {
			break
		}
		// 设置下次工作时间
		item.WillWorkAt = now + (int64)(item.Interval)
		// 检测是否无待处理或正在处理的任务？
		if stats, err := w.client.Stats(item.Tube); err != nil {
			continue
		} else {
			// 仅在为空时，才推送任务
			if stats["current-jobs-ready"] == "0" && stats["current-jobs-reserved"] == "0" {
				w.client.Put(item.Tube, w.data, 1, 0, time.Minute)
			}
		}
	}

	// 重新排序
	sort.Sort(w.jobs)
}
