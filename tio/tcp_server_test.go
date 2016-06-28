package tio

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/stretchr/testify/assert"
)

func Test_TcpServerShutdown(t *testing.T) {
	manager := &manage.Manager{}
	manager.Init("test")
	server := new(TCPServer)
	config := ConfigDefault()
	config.AddrListen = ":7654"
	config.TerminalTimeout = 10
	go func() {
		server.Listen(manager, config)
	}()

	// wait for server init
	time.Sleep(time.Millisecond * time.Duration(1))

	// connect to server
	conns := make([]net.Conn, 2)
	for i := range conns {
		c, err := net.Dial("tcp", config.AddrListen)
		assert.Nil(t, err)
		conns[i] = c
	}

	server.manager.Shutdown()

	b := make([]byte, 5)
	for _, c := range conns {
		_, err := c.Read(b)
		assert.Equal(t, io.EOF, err)
	}
}
