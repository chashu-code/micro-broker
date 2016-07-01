package main

import (
	"flag"
	"os"
	"runtime"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/tio"
)

var gateWayURI = flag.String("g", "redis://127.0.0.1:6379", "指定可链接到 Monitor 的网关")
var brokerName = flag.String("n", "", "指定 Broker 名称（ 默认采用 os.Hostname ）")
var pathConf = flag.String("c", "", "JSON 配置文件路径（ 通常仅用于开发环境 ）")
var isMonitor = flag.Bool("monitor", false, "若指定，则以 Monitor 的方式运行")
var verbose = flag.Bool("verbose", false, "若指定，则以 Monitor 的方式运行")
var nojob = flag.Bool("nojob", false, "若指定，则不对job进行处理")

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	name := *brokerName
	var err error
	if name == "" {
		name, err = os.Hostname()
		if err != nil {
			panic(err)
		}
	}

	// manager
	m := manage.NewBroker(name)
	m.SetVerbose(*verbose)
	m.MapSet(manage.IDMapQueue, manage.KeyJobQueue,
		tio.NewMsgQueueWithSize(tio.JobQueueOpTimeoutDefault, tio.JobPutWorkerSizeDefault))

	// job worker
	if !*nojob {
		for i := 0; i < tio.JobPutWorkerSizeDefault; i++ {
			jobClient := tio.NewBeanJobClient(tio.AddrJobServerDefault)
			w := tio.NewJobPutWorker(m, jobClient)
			go w.Run()
		}
		// TODO: add reg config sync callback
		// change the CrontabJobList
		jobClient := tio.NewBeanJobClient(tio.AddrJobServerDefault)
		w := tio.NewCrontabWroker(m, jobClient, 1000)
		go w.Run()
	}

	m.UpdateWithConfFile(*pathConf)

	// server
	server := new(tio.TCPServer)
	server.Listen(m, nil)
}
