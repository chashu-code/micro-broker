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
func (s *TCPServer) Listen(manager manage.IManager, config *Config) {
	var listener *net.TCPListener

	s.Server.Start(manager, config)

	defer func() {
		s.Log(log.Fields{
			"error": recover(),
		}).Info("TCPServer stop")

		if listener != nil {
			listener.Close()
		}
		s.Server.Stop()
	}()

	tcpAddr, err := net.ResolveTCPAddr("tcp4", s.config.AddrListen)
	checkError(err)
	listener, err = net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	s.LogDirect().Info("TCPServer start")

	timeoutAccept := time.Duration(s.config.NetTimeout) * time.Millisecond

	s.config.Protocol = &MPProtocol{}
	s.config.ConnCallback = &TerminalCallback{}

	for !s.manager.IsShutdown() {

		listener.SetDeadline(time.Now().Add(timeoutAccept))

		conn, err := listener.AcceptTCP()
		if err != nil {
			if isTimeout(err) {
				continue
			} else {
				panic(err)
			}
		}

		// if err = conn.SetNoDelay(true); err != nil {
		// 	panic(err)
		// }

		s.TerminalStart()
		go func() {
			defer s.TerminalClose()
			TerminalRun(s.TIDNext(), manager, s.config, conn)
		}()
	}
}
