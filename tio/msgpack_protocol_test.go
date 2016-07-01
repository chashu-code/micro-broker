package tio

import (
	"testing"

	mconn "github.com/jordwest/mock-conn"

	"github.com/stretchr/testify/assert"
)

func Test_MsgPackProtocol_ToMsg(t *testing.T) {
	p := NewMPPacket()
	data := []byte{0}
	service := "service/send"
	nav := "nav/send"
	for _, action := range []string{CmdReqSend, CmdJobSend} {
		p.UpdateCmds(action, service, nav)
		p.UpdateData(data)
		msg := p.ToMsg()
		assert.Equal(t, action, msg.Action)
		assert.Equal(t, service, msg.Service)
		assert.Equal(t, nav, msg.Nav)
		assert.Equal(t, data, msg.Data)
	}

	service = "service/req"
	nav = "nav/req"
	from := "b1@t1@r1"
	for _, action := range []string{CmdReqRemote, CmdReqRecv, CmdJobRemote} {
		p.UpdateCmds(action, service, nav, from)
		p.UpdateData(data)
		msg := p.ToMsg()
		assert.Equal(t, action, msg.Action)
		assert.Equal(t, service, msg.Service)
		assert.Equal(t, nav, msg.Nav)
		assert.Equal(t, from, msg.From)
		assert.Equal(t, data, msg.Data)
	}

	code := "1"
	from = "b2@t1@r1"
	for _, action := range []string{CmdResSend, CmdResRemote} {
		p.UpdateCmds(action, code, from)
		p.UpdateData(data)
		msg := p.ToMsg()
		assert.Equal(t, action, msg.Action)
		assert.Equal(t, code, msg.Code)
		assert.Equal(t, from, msg.From)
		assert.Equal(t, data, msg.Data)
	}

	code = "2"
	for _, action := range []string{CmdResRecv} {
		p.UpdateCmds(action, code)
		p.UpdateData(data)
		msg := p.ToMsg()
		assert.Equal(t, action, msg.Action)
		assert.Equal(t, code, msg.Code)
		assert.Equal(t, data, msg.Data)
	}

	action := "unknow"
	p.UpdateCmds(action, code)
	p.UpdateData(data)
	msg := p.ToMsg()

	assert.Nil(t, msg)

}

func Test_MsgPackProtocol_ReadPacket(t *testing.T) {
	p := NewMPPacket()
	action := CmdResRecv
	code := "2"
	data := []byte{1}
	p.UpdateCmds(action, code)
	p.UpdateData(data)

	bytes := p.Bytes()
	mc := mconn.NewConn()

	go func() {
		mc.Client.Write(bytes)
	}()

	protocol := &MPProtocol{}
	pRead, err := protocol.ReadPacket(mc.Server)

	assert.Nil(t, err)
	assert.Equal(t, p.Cmds(), pRead.Cmds())
	assert.Equal(t, p.Data(), pRead.Data())
}
