package work

import (
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
}

// CrontabJobList 有序定时任务列表
type CrontabJobList []*CrontabJob

func (l CrontabJobList) Len() int           { return len(l) }
func (l CrontabJobList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l CrontabJobList) Less(i, j int) bool { return l[i].WillWorkAt < l[j].WillWorkAt }

// CrontabWorker 定时工作器
type CrontabWorker struct {
	Worker

	V    string
	jobs CrontabJobList
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

func hms2toi(hms2 string) (int, int) {
	arr := strings.SplitN(hms2, ",", 2)
	if len(arr) == 2 {
		return hmstoi(arr[0]), hmstoi(arr[1])
	}
	return 0, 0
}

// updateJobs 更新任务列表
func (w *CrontabWorker) updateJobs() bool {
	// dsl Map 规则
	// name => 12m  => 每12分钟执行一次
	// name => 12m|13:00:00,14:00:00 => 在 > 13点 && < 14点 范围内，每12m 执行一次

	dslMap := w.mgr.Conf.CrontabJobDslMap
	v := dslMap["v"]

	// 如果 v 无效，或者没有更新，则退出
	if v == "" || w.V == v {
		return false
	}

	w.V = v

	w.Log.Info("update crontab jobs", zap.String("v", v))

	jobs := make(CrontabJobList, 0)

	for name, dsl := range dslMap {
		if name == "v" {
			continue
		}

		hmsStart := 0
		hmsEnd := 0
		durS := dsl

		if strings.Contains(dsl, "|") {
			arr := strings.SplitN(dsl, "|", 2)
			if len(arr) == 2 {
				durS = arr[0]
				hmsStart, hmsEnd = hms2toi(arr[1])
			}
		}

		if dur, err := time.ParseDuration(durS); err == nil {
			job := &CrontabJob{
				Tube:       name,
				Interval:   int(dur.Seconds()),
				LimitStart: hmsStart,
				LimitEnd:   hmsEnd,
			}

			jobs = append(jobs, job)

			w.Log.Info("add CrontabJob", zap.Object("job", job))
		}
	}

	w.jobs = jobs
	return true
}

func (w *CrontabWorker) getWrkJobs() []*CrontabJob {
	jobs := w.jobs
	wrkJobs := []*CrontabJob{}

	if len(jobs) == 0 {
		return wrkJobs
	}

	// 结束时，重新排序
	defer sort.Sort(jobs)

	now := time.Now()
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

func (w *CrontabWorker) process() error {
	// 每秒触发一次
	time.Sleep(time.Second)

	w.updateJobs()

	jobs := w.getWrkJobs()
	if len(jobs) < 1 {
		// 无任务，不做特殊处理
		return nil
	}

	p, _, err := w.beanPoolMap.Fetch(defaults.IPLocal, w.mgr.Conf.JobPoolSize)
	if err != nil {
		w.Log.Error("fetch local job pool fail", zap.Error(err))
		return err
	}

	var btsMsg []byte
	nowST := time.Now().Unix()
	msg := &manage.Msg{
		Action: "ActJob",
		Data:   nowST,
		V:      1,
	}
	btsMsg, err = w.mgr.Pack(msg)
	if err != nil {
		w.Log.Error("pack crontab job fail", zap.Error(err))
		return err
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
				pri, delay, ttr, _ := c.PutArgsWithCode("")
				_, errPut = c.Put(item.Tube, btsMsg, pri, delay, ttr)
				if errPut != nil {
					return errPut
				}
			}
		}

		return nil
	})

	if err != nil {
		w.Log.Error("put crontjob fail", zap.Error(err))
		return err
	}

	return nil
}
