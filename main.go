package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/pool"
	"github.com/chashu-code/micro-broker/protocol"
	"github.com/chashu-code/micro-broker/work"
	"github.com/uber-go/zap"
)

var ipConf = flag.String("ipconf", "127.0.0.1:6379", "指定可链接到配置redis，多个可以用,隔开")
var logPath = flag.String("log", "", "指定日志文件路径，若不指定，则直接输出到终端")

// var brokerName = flag.String("n", "", "指定 Broker 名称（ 默认采用 os.Hostname ）")
// var pathConf = flag.String("c", "", "JSON 配置文件路径（ 通常仅用于开发环境 ）")

var pathPID = flag.String("p", "", "pid file path")

// var isMonitor = flag.Bool("monitor", false, "若指定，则以 Monitor 的方式运行")
// var verbose = flag.Bool("verbose", false, "若指定，则以 Monitor 的方式运行")

// var nojob = flag.Bool("nojob", false, "若指定，则不对job进行处理")

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	conf := manage.NewConfig()
	conf.RedisPoolMap = pool.NewRedisPoolMap()
	conf.BeanPoolMap = pool.NewBeanPoolMap()
	conf.LogLevel = zap.InfoLevel

	// 设定配置Redis Url, 默认为本地 redis
	if *ipConf != "" {
		if ip := strings.Split(*ipConf, ",")[0]; ip != "" {
			conf.IPConf = ip
		}
	}

	if *logPath != "" {
		conf.LogPath = *logPath
	}

	mgr := manage.NewManager(conf)
	mgr.SubWrkRun = work.SubWorkerRun
	mgr.CarryWrkRun = work.CarryWorkerRun
	mgr.ConfWrkRun = work.ConfWorkerRun
	mgr.CrontabWrkRun = work.CrontabWorkerRun
	mgr.AddProtocolGenFn(1, protocol.NewV1Protocol)

	// pid file
	if *pathPID != "" {
		pid := fmt.Sprintf("%v", os.Getpid())
		ioutil.WriteFile(*pathPID, ([]byte)(pid), 0666)
	}

	mgr.Start()
}
