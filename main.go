package main

import (
	"flag"
	"runtime"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/tio"
)

var gateWayURI = flag.String("g", "redis://127.0.0.1:6379", "指定可链接到 Monitor 的网关")
var brokerName = flag.String("n", "", "指定 Broker 名称（ 默认采用 os.Hostname ）")
var pathConf = flag.String("c", "", "JSON 配置文件路径（ 通常仅用于开发环境 ）")
var isMonitor = flag.Bool("monitor", false, "若指定，则以 Monitor 的方式运行")
var verbose = flag.Bool("verbose", false, "若指定，则以 Monitor 的方式运行")

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	broker := manage.NewBroker("dev")
	broker.SetVerbose(*verbose)

	server := new(tio.TCPServer)
	server.Listen(broker, ":6636")
}
