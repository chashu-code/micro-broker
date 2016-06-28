package tio

import (
	"fmt"
	"net"
	"strings"

	"github.com/chashu-code/micro-broker/log"
	"github.com/chashu-code/micro-broker/manage"
)

// IConnCallback 链接事件回调接口
type IConnCallback interface {
	// OnConnect 建立连接时回调，返回false则关闭链接
	OnConnect(*Terminal) bool
	// OnData 接收到数据，准备处理时回调，返回false则关闭链接
	OnData(*Terminal, IPacket) bool
	// OnClose 关闭链接时回调
	OnClose(*Terminal)
	// OnError 发生意外错误时回调
	OnError(*Terminal, interface{})
}

// Server 服务基本实现
type Server struct {
	log.Able
	tid     uint
	manager manage.IManager
	config  *Config
}

// TIDNext 返回下一个有效tid
func (s *Server) TIDNext() string {
	s.tid++
	return fmt.Sprintf("#T%v", s.tid)
}

// Start 初始化
func (s *Server) Start(manager manage.IManager, config *Config) {
	if config == nil {
		s.config = ConfigDefault()
	} else {
		s.config = config
	}

	s.FixFields = log.Fields{
		"listen": s.config.AddrListen,
	}

	s.manager = manager
	s.manager.WaitAdd()
}

// Stop 停止
func (s *Server) Stop() {
	s.manager.WaitDone()
}

// TerminalStart 终端开启
func (s *Server) TerminalStart() {
	s.manager.WaitAdd()
}

// TerminalClose 终端关闭
func (s *Server) TerminalClose() {
	s.manager.WaitDone()
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func isTimeout(err error) bool {
	if t, ok := err.(net.Error); ok {
		return t.Timeout()
	}
	return strings.Contains(err.Error(), "i/o timeout")
}
