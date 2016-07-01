package tio

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chashu-code/micro-broker/log"
	"github.com/chashu-code/micro-broker/manage"
)

// CrontabJob 定时任务
type CrontabJob struct {
	Tube         string
	Interval     int
	WillWorkAt   int64
	HMLimitStart int // hour * 60 + minute
	HMLimitEnd   int
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

	w.manager.RegConfigSyncCallback(w.confSyncCallback)

	return w
}

func (w *CrontabWroker) hmtoi(s string) int {
	arr := strings.SplitN(s, ":", 2)
	if len(arr) == 2 {
		h, _ := strconv.Atoi(arr[0])
		m, _ := strconv.Atoi(arr[1])
		return h*60 + m
	}
	return 0
}

func (w *CrontabWroker) confSyncCallback() {
	if v, ok := w.manager.MapGet(manage.IDMapConf, manage.KeyCronJob); ok {
		if vMap, ok := v.(map[string]interface{}); ok {
			jobs := make(CrontabJobList, 0)

			for name, vStr := range vMap {
				if vDuration, ok := vStr.(string); ok {
					hmStart := 0
					hmEnd := 0

					if strings.Contains(vDuration, "|") {
						arr := strings.SplitN(vDuration, "|", 2)
						if len(arr) == 2 {
							vDuration = arr[0]
							// hm start and end
							arr = strings.SplitN(arr[1], ",", 2)
							hmStart = w.hmtoi(arr[0])
							hmEnd = w.hmtoi(arr[1])
						}
					}

					if duration, err := time.ParseDuration(vDuration); err == nil {
						job := &CrontabJob{
							Tube:         name,
							Interval:     int(duration.Seconds()),
							HMLimitStart: hmStart,
							HMLimitEnd:   hmEnd,
						}

						jobs = append(jobs, job)

						w.Log(log.Fields{
							"name":         name,
							"value":        vStr,
							"duration":     duration,
							"hmLimitStart": hmStart,
							"hmLimitEnd":   hmEnd,
						}).Info("Cron job parse ok")

						continue
					}
				}

				w.Log(log.Fields{
					"name":  name,
					"value": vStr,
				}).Error("Cron job parse fail")
			}

			w.UpdateJobs(jobs)
		}
	}
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
	hmNow := time.Now().Hour()*60 + time.Now().Minute()

	for _, item := range jobs {
		if item.WillWorkAt > now {
			break
		}

		// 时间限制均不为 0 才生效，若当前时间不在许可范围内，则跳过
		if (item.HMLimitStart != 0 && item.HMLimitEnd != 0) &&
			(item.HMLimitStart > hmNow || item.HMLimitEnd < hmNow) {
			continue
		}

		// 设置下次工作时间
		item.WillWorkAt = now + (int64)(item.Interval)

		// 检测是否无待处理或正在处理的任务？
		if stats, err := w.client.Stats(item.Tube); err != nil {
			continue
		} else {
			// 仅在为空时，才推送任务
			if stats == nil || (stats["current-jobs-ready"] == "0" && stats["current-jobs-reserved"] == "0") {
				w.client.Put(item.Tube, w.data, 1, 0, time.Minute)
				w.Log(log.Fields{
					"name": item.Tube,
				}).Info("Cron job added")
			}
		}
	}

	// 重新排序
	sort.Sort(w.jobs)
}
