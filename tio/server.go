package tio

import (
	"fmt"
	"net"
	"sync"

	"github.com/chashu-code/micro-broker/log"
	"github.com/chashu-code/micro-broker/manage"
	cmap "github.com/streamrail/concurrent-map"
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

// IServer 服务接口
type IServer interface {
	Listen(manager manage.IManager, addr string)
	IsShutdown() bool
	Protocol() IProtocol
	ConnCallback() IConnCallback
}

const (
	// KeyBrokerName  配置中，当前borker name
	KeyBrokerName = "#NAME"
	// KeyBrokersOnline 配置中，在线brokers
	KeyBrokersOnline = "#ON-BRKS"

	// KeyGateWaysOnline 配置中，在线的gateway
	KeyGateWaysOnline = "#ON-GWS"

	// KeyJobQueue 任务队列名
	KeyJobQueue = "#QUEUE-JOB"

	// KeyBrokerQueue 代理消息队列名
	KeyBrokerQueue = "#QUEUE-BRK"

	// KeyPubQueue 推送消息队列名
	KeyPubQueue = "#QUEUE-PUB"

	// KeyVerConf 配置版本名
	KeyVerConf = "#VER-CONF"

	// KeyVerGateWaysOnline 在线gateway版本
	KeyVerGateWaysOnline = "#VER-ON-GWS"

	// KeyVerBrokersOnline 在线broker版本
	KeyVerBrokersOnline = "#VER-ON-BRKS"

	// KeyVerServiceRoute 服务路由版本
	KeyVerServiceRoute = "#VER-SROUTE"

	// KeyServiceRoute 服务路由
	KeyServiceRoute = "#SROUTE"

	// AddrListenDefault 默认监听地址
	AddrListenDefault = "127.0.0.1:6636"
	// SpriteLBSizeDefault 精灵负载最大数量
	SpriteLBSizeDefault = 2
	// MsgQueueSizeDefault 消息队列最大缓冲
	MsgQueueSizeDefault = 30
	// MsgQueueOpTimeoutDefault 消息队列操作超时毫秒
	MsgQueueOpTimeoutDefault = 100
	// NetTimeoutDefault 网络操作超时毫秒
	NetTimeoutDefault = 1000
)

// Server 服务基本实现
type Server struct {
	log.Able

	protocol IProtocol
	callback IConnCallback

	tid           uint
	chanStop      chan struct{}
	waitGroupStop *sync.WaitGroup
	mapCatalog    map[uint8]cmap.ConcurrentMap
}

// AddMap 添加表
func (s *Server) AddMap(mapID uint8, mapItem cmap.ConcurrentMap) {
	s.mapCatalog[mapID] = mapItem
}

// Map 返回指定id的表
func (s *Server) Map(mapID uint8) cmap.ConcurrentMap {
	if item, ok := s.mapCatalog[mapID]; ok {
		return item
	}
	return nil
}

// MapGet 从表中读取，找不到返回 nil, false
func (s *Server) MapGet(mapID uint8, key string) (interface{}, bool) {
	if mapItem, ok := s.mapCatalog[mapID]; ok {
		return mapItem.Get(key)
	}
	return nil, false
}

// MapSet 存到表中，若找不到该表，则返回false
func (s *Server) MapSet(mapID uint8, key string, val interface{}) bool {
	if mapItem, ok := s.mapCatalog[mapID]; ok {
		mapItem.Set(key, val)
		return true
	}
	return false
}

// TIDNext 返回下一个有效tid
func (s *Server) TIDNext() string {
	s.tid++
	return fmt.Sprintf("#T%v", s.tid)
}

// Protocol 返回服务协议
func (s *Server) Protocol() IProtocol {
	return s.protocol
}

// ConnCallback 返回服务事件回调处理
func (s *Server) ConnCallback() IConnCallback {
	return s.callback
}

// IsShutdown 是否已关闭
func (s *Server) IsShutdown() bool {
	select {
	case <-s.chanStop:
		return true
	default:
		return false
	}
}

// Start 初始化
func (s *Server) Start(addr string, protocol IProtocol, callback IConnCallback) {
	s.protocol = protocol
	s.callback = callback
	s.mapCatalog = make(map[uint8]cmap.ConcurrentMap)
	s.waitGroupStop = &sync.WaitGroup{}
	s.FixFields = log.Fields{
		"listen": addr,
	}

	// s.SetLogLevel(log.LevelError)
	s.waitGroupStop.Add(1)
}

// Shutdown 停止
func (s *Server) Shutdown() {
	s.waitGroupStop.Done()
}

// TerminalStart 终端开启
func (s *Server) TerminalStart() {
	s.waitGroupStop.Add(1)
}

// TerminalClose 终端关闭
func (s *Server) TerminalClose() {
	s.waitGroupStop.Done()
}

// ShutdownWait 等待停止
func (s *Server) ShutdownWait() {
	close(s.chanStop)
	s.waitGroupStop.Wait()
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func isTimeout(err error) bool {
	t, ok := err.(*net.OpError)
	return ok && t.Timeout()
}
