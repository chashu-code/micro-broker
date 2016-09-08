package work

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/pool"
	"github.com/uber-go/zap"
)

// CrontabJob 定时任务
type CrontabJob struct {
	// Tube 推送的管道
	Tube string
	// Interval 间隔秒数
	Interval int
	// WillWorkAt 将处理于 timestamp
	WillWorkAt int64
	// LimitStart  限制开始时间（ hour * 60 * 60 + minute * 60 + second）
	LimitStart int
	// LimitEnd  限制结束时间（ hour * 60 * 60 + minute * 60 + second）
	LimitEnd int

	// PutTimes 推送次数
	PutTimes int
}

// CrontabJobList 有序定时任务列表
type CrontabJobList []*CrontabJob

func (l CrontabJobList) Len() int           { return len(l) }
func (l CrontabJobList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l CrontabJobList) Less(i, j int) bool { return l[i].WillWorkAt < l[j].WillWorkAt }

// CrontabWorker 定时工作器
type CrontabWorker struct {
	Worker
	V string

	jobs      CrontabJobList
	re        *regexp.Regexp
	lastLogAt int64 // 上次记录时间
}

// CrontabWorkerRun 运行1个CrontabWorker
func CrontabWorkerRun(mgr *manage.Manager, ip string, count int) {
	w := &CrontabWorker{}
	go w.Run(mgr, "crontab", w.process)
}

func hmstoi(hms string) int {
	result := 0
	arr := strings.SplitN(hms, ":", 3)
	if len(arr) == 3 {
		h := 0
		m := 0
		s := 0
		v, err := strconv.Atoi(arr[0])
		if err != nil {
			return result
		}
		h = v * 60 * 60
		v, err = strconv.Atoi(arr[1])
		if err != nil {
			return result
		}
		m = v * 60
		v, err = strconv.Atoi(arr[2])
		if err != nil {
			return result
		}
		s = v
		result = h + m + s
	}

	return result
}

var reDSL = regexp.MustCompile(`^(\d+[hms])(\|(\d{1,2}:\d{1,2}:\d{1,2}),(\d{1,2}:\d{1,2}:\d{1,2}))?$`)

func dslToJob(name, dsl string) *CrontabJob {
	// dsl 规则
	// 12m  => 每12分钟执行一次
	// 12m|13:00:00,14:00:00 => 在 > 13点 && < 14点 范围内，每12m 执行一次
	ss := reDSL.FindStringSubmatch(dsl)
	if len(ss) == 5 {
		if dur, err := time.ParseDuration(ss[1]); err == nil {
			return &CrontabJob{
				Tube:       name,
				Interval:   int(dur.Seconds()),
				LimitStart: hmstoi(ss[3]),
				LimitEnd:   hmstoi(ss[4]),
			}
		}
	}
	return nil
}

// updateJobs 更新任务列表
func (w *CrontabWorker) updateJobs() bool {

	dslMap := w.mgr.Conf.CrontabJobDslMap
	v := dslMap["v"]

	// 如果 v 没有更新，则退出
	if w.V == v {
		return false
	}

	w.V = v

	w.Log.Info("update crontab jobs", zap.String("v", v))

	jobs := make(CrontabJobList, 0)

	for name, dsl := range dslMap {
		if name == "v" {
			continue
		}

		job := dslToJob(name, dsl)

		if job != nil {
			jobs = append(jobs, job)
			w.Log.Info("add CrontabJob", zap.Object("job", job))
		} else {
			w.Log.Warn("wrong CrontabJob DSL", zap.String("name", name), zap.String("dsl", dsl))
		}
	}

	w.jobs = jobs
	return true
}

func (w *CrontabWorker) getWrkJobs(now time.Time) []*CrontabJob {
	jobs := w.jobs
	wrkJobs := []*CrontabJob{}

	if len(jobs) == 0 {
		return wrkJobs
	}

	// 结束时，重新排序
	defer sort.Sort(jobs)

	nowST := now.Unix()
	h, m, s := now.Clock()
	hmsNow := h*60*60 + m*60 + s

	for _, item := range jobs {
		if item.WillWorkAt > nowST {
			break
		}
		// 时间限制均不为 0 才生效，若当前时间不在许可范围内，则跳过
		if (item.LimitStart != 0 && item.LimitEnd != 0) &&
			(item.LimitStart > hmsNow || item.LimitEnd < hmsNow) {
			continue
		}

		// 设置下次工作时间
		item.WillWorkAt = nowST + (int64)(item.Interval)
		wrkJobs = append(wrkJobs, item)
	}

	return wrkJobs
}

func (w *CrontabWorker) logPuts(now time.Time) {
	nowUnix := now.Unix()
	// 10s记录一次；未足10s 或 jobs为空 退出
	if w.lastLogAt+10 > nowUnix || len(w.jobs) == 0 {
		return
	}
	w.lastLogAt = nowUnix

	var fields []zap.Field

	for _, job := range w.jobs {
		if job.PutTimes > 0 {
			fields = append(fields, zap.Int(job.Tube, job.PutTimes))
			job.PutTimes = 0
		}
	}

	if len(fields) > 0 {
		w.Log.Info("put statistics", fields...)
	}
}

func (w *CrontabWorker) process() {
	// 每秒触发一次
	time.Sleep(time.Second)
	w.processWithTime(time.Now())
}

func (w *CrontabWorker) processWithTime(now time.Time) {
	w.updateJobs()

	jobs := w.getWrkJobs(now)
	if len(jobs) < 1 {
		// 无任务，不做特殊处理
		return
	}

	p, _, err := w.beanPoolMap.FetchOrNew(defaults.IPLocal, w.mgr.Conf.JobPoolSize)
	if err != nil {
		w.Log.Error("fetch local job pool fail", zap.Error(err))
		return
	}

	var btsMsg []byte
	nowST := now.Unix()
	msg := &manage.Msg{
		Action: manage.ActJob,
		Data:   nowST,
		V:      1,
	}
	msg.FillWithReq(w.mgr)

	btsMsg, err = w.mgr.Pack(msg)
	if err != nil {
		w.Log.Error("pack crontab job fail", zap.Error(err))
		return
	}

	err = p.With(func(c *pool.BeanClient) error {
		for _, item := range jobs {
			stats, errPut := c.Stats(item.Tube)
			if errPut != nil {
				s := errPut.Error()
				// 如果不是tube 不存在错误，则退出
				if !strings.Contains(s, "stats-tube: not found") {
					return errPut
				}
				stats = nil
			}

			if stats == nil || (stats["current-jobs-ready"] == "0" && stats["current-jobs-reserved"] == "0") {
				pri, delay, ttr, _ := msg.CodeToPutArgs()
				_, errPut = c.Put(item.Tube, btsMsg, pri, delay, ttr)
				if errPut != nil {
					return errPut
				}
				item.PutTimes++
			}
		}

		return nil
	})

	if err != nil {
		w.Log.Error("put crontjob fail", zap.Error(err))
	} else {
		w.logPuts(now)
	}
}
