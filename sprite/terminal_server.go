package sprite

import (
	"fmt"
	"net"

	"github.com/chashu-code/micro-broker/log"
)

// TerminalServer 终端监听服务
type TerminalServer struct {
	tid    uint
	stoped bool
}

// TIDNext 返回下一个有效tid
func (s *TerminalServer) TIDNext() string {
	s.tid++
	return fmt.Sprintf("#T%v", s.tid)
}

// Listen 开始监听
func (s *TerminalServer) Listen(broker *Broker, addr string) {
	server, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	s.stoped = false

	broker.Log(log.Fields{
		"addr": addr,
	}).Info("terminal server start")

	for !s.stoped {
		conn, err := server.Accept()
		if err != nil {
			panic(err)
		}

		// new terminal
		options := map[string]interface{}{
			"name":         s.TIDNext(),
			"conn":         conn,
			"mapQueueOp":   broker.MapQueueOp,
			"mapOutRouter": broker.MapOutRouter,
			"mapConfig":    broker.MapConfig,
		}

		TerminalRun(options)
	}

	broker.Log(log.Fields{}).Info("terminal server stop")
}

// Stop 停止监听
func (s *TerminalServer) Stop() {
	s.stoped = true
}
