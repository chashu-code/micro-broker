package tio

import (
	"testing"

	"github.com/chashu-code/micro-broker/manage"
	mconn "github.com/jordwest/mock-conn"
	"github.com/stretchr/testify/assert"
)

type MockConnCallback struct {
	connectTimes int
	dataTimes    int
	closeTimes   int
	errorTimes   int
}

func (c *MockConnCallback) OnConnect(t *Terminal) bool {
	c.connectTimes++
	return true
}

func (c *MockConnCallback) OnData(t *Terminal, p IPacket) bool {
	c.dataTimes++
	msg := p.ToMsg()
	if msg.Code == "1" {
		panic("sorry")
	}
	t.SendPacket(p)
	return true
}

func (c *MockConnCallback) OnClose(t *Terminal) {
	c.closeTimes++
}

func (c *MockConnCallback) OnError(t *Terminal, v interface{}) {
	c.errorTimes++
}

func mockArgs() (manage.IManager, IProtocol) {
	manager := &manage.Manager{}
	manager.Init("test")
	protocol := &MPProtocol{}
	return manager, protocol
}

func Test_TerminalNormal(t *testing.T) {
	cb := &MockConnCallback{}
	manager, protocol := mockArgs()

	mc := mconn.NewConn()
	// protocol := &MPProtocol{}
	go func() {
		p := NewMPPacket()
		action := CmdResRecv
		code := "0"
		data := []byte{1}
		p.UpdateCmds(action, code)
		p.UpdateData(data)
		bytes := p.Bytes()
		mc.Client.Write(bytes)
		mc.Client.Read(bytes)
		manager.Shutdown()
	}()

	conf := ConfigDefault()
	conf.ConnCallback = cb
	conf.Protocol = protocol
	conf.TerminalTimeout = 10

	TerminalRun("test", manager, conf, mc.Server)

	assert.Equal(t, 1, cb.connectTimes)
	assert.Equal(t, 1, cb.dataTimes)
	assert.Equal(t, 1, cb.closeTimes)
	assert.Equal(t, 0, cb.errorTimes)
}

func Test_TerminalError(t *testing.T) {
	cb := &MockConnCallback{}
	manager, protocol := mockArgs()

	mc := mconn.NewConn()
	// protocol := &MPProtocol{}
	go func() {
		p := NewMPPacket()
		action := CmdResRecv
		code := "1"
		data := []byte{1}
		p.UpdateCmds(action, code)
		p.UpdateData(data)
		bytes := p.Bytes()
		mc.Client.Write(bytes)
		mc.Client.Read(bytes)
	}()

	conf := ConfigDefault()
	conf.ConnCallback = cb
	conf.Protocol = protocol
	conf.TerminalTimeout = 10

	TerminalRun("test", manager, conf, mc.Server)

	assert.Equal(t, 1, cb.connectTimes)
	assert.Equal(t, 1, cb.dataTimes)
	assert.Equal(t, 1, cb.closeTimes)
	assert.Equal(t, 1, cb.errorTimes)
}

func Test_TerminalRID(t *testing.T) {
	term := &Terminal{}

	assert.Equal(t, "0", term.RID())
	assert.Equal(t, "1", term.RIDNext())
	assert.Equal(t, "2", term.RIDNext())
	assert.Equal(t, "2", term.RID())
}
