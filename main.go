package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/chashu-code/micro-broker/fsm"
	"github.com/chashu-code/micro-broker/sprite"
)

var gateWayURI = flag.String("g", "redis://127.0.0.1:6379", "指定可链接到 Monitor 的网关")
var brokerName = flag.String("n", "", "指定 Broker 名称（ 默认采用 os.Hostname ）")
var pathConf = flag.String("c", "", "JSON 配置文件路径（ 通常仅用于开发环境 ）")
var isMonitor = flag.Bool("monitor", false, "若指定，则以 Monitor 的方式运行")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	work()
}

func work() {
	flag.Parse()

	name := *brokerName
	path := *pathConf

	if name == "" {
		var err error
		if name, err = os.Hostname(); err != nil {
			fmt.Printf("获取 os.Hostname 失败: %s\n", err)
			os.Exit(1)
		}
	}

	stopSignal := make(chan bool)

	cbDesc := fsm.CallbackDesc{
		"stop": func(_ fsm.CallbackKey, _ *fsm.Event) error {
			stopSignal <- true
			return nil
		},
	}

	if *isMonitor {
		fmt.Println("将作为 Monitor 启动")
	} else {
		fmt.Println("将作为 Broker 启动")
		sprite.BrokerRun(map[string]interface{}{
			"name":         name,
			"pathConf":     path,
			"gatewayURI":   *gateWayURI,
			"callbackDesc": cbDesc,
		})
	}

	<-stopSignal

}
