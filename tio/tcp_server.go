package tio

import (
	"net"
	"time"

	"github.com/chashu-code/micro-broker/log"
	"github.com/chashu-code/micro-broker/manage"
)

// TCPServer Tcp 协议链接监听实现
type TCPServer struct {
	Server
}

// Listen 开始监听
func (s *TCPServer) Listen(manager manage.IManager, addr string) {
	var listener *net.TCPListener

	protocol := &SSDBProtocol{
		timeoutDeadLine: time.Second,
	}
	callback := &TerminalCallback{}

	s.Server.Start(addr, protocol, callback)

	defer func() {
		s.Log(log.Fields{
			"error": recover(),
		}).Info("TCPServer stop")

		if listener != nil {
			listener.Close()
		}

		s.Server.Shutdown()
	}()

	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	checkError(err)
	listener, err = net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	s.LogDirect().Info("TCPServer start")

	timeoutAccept := time.Second

	for !s.IsShutdown() {

		listener.SetDeadline(time.Now().Add(timeoutAccept))

		conn, err := listener.AcceptTCP()
		if err != nil {
			if isTimeout(err) {
				continue
			} else {
				panic(err)
			}
		}

		s.TerminalStart()
		go func() {
			defer s.TerminalClose()
			TerminalRun(s.TIDNext(), manager, s, conn)
		}()
	}
}

// Shutdown 关闭
func (s *TCPServer) Shutdown() {
	s.Server.ShutdownWait()
}
