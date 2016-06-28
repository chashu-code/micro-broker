package tio

import (
	"testing"

	"github.com/chashu-code/micro-broker/manage"
	"github.com/stretchr/testify/assert"
)

func mockTerminal() *Terminal {
	manager := &manage.Manager{}
	manager.Init("test")
	config := ConfigDefault()
	config.TerminalTimeout = 10
	config.Protocol = &MPProtocol{}
	config.ConnCallback = &TerminalCallback{}

	term := TerminalBuild("t1", manager, config)
	return term
}

func Test_TerminalCallback_Reg(t *testing.T) {
	term := mockTerminal()
	cb := term.Conf.ConnCallback.(*TerminalCallback)
	packet := NewMPPacket()
	packet.UpdateCmds(CmdReg, "a,b")
	assert.Nil(t, term.SubMsgPoper)
	cb.processReg(term, packet)
	cmds := packet.Cmds()
	assert.Equal(t, "ok", cmds[0])
	assert.NotNil(t, term.SubMsgPoper)
}

func Test_TerminalCallback_Pull(t *testing.T) {
	term := mockTerminal()
	cb := term.Conf.ConnCallback.(*TerminalCallback)
	packet := NewMPPacket()
	packet.UpdateCmds(CmdPull, "unfound")
	cb.processPull(term, packet)
	cmds := packet.Cmds()
	assert.Equal(t, "err", cmds[0])

	// reg
	term.Conf.MultiMsgQueuePoperTimeout = 10
	packet.UpdateCmds(CmdReg, "a,b")
	cb.processReg(term, packet)
	token := packet.Cmds()[1]
	packet.UpdateCmds(CmdPull, token)

	// empty
	cb.processPull(term, packet)
	cmds = packet.Cmds()
	assert.Equal(t, "empty", cmds[0])

	// pull one
	msg := &Msg{
		Action: CmdResRecv,
		Code:   "0",
	}
	queue := term.MsgQueue("a")
	queue.Push(msg, true)

	packet.UpdateCmds(CmdPull, token)
	cb.processPull(term, packet)

	msgPull := packet.ToMsg()
	assert.Equal(t, msg.Action, msgPull.Action)
	assert.Equal(t, msg.Code, msgPull.Code)
}

func Test_TerminalCallback_Sync(t *testing.T) {
	term := mockTerminal()
	cb := term.Conf.ConnCallback.(*TerminalCallback)
	packet := NewMPPacket()
	// 无配置
	packet.UpdateCmds(CmdSync, "test", "")
	cb.processSync(term, packet)
	cmds := packet.Cmds()
	assert.Equal(t, "newest", cmds[0])

	// 添加配置
	term.Manager.MapSet(manage.IDMapConf, manage.KeyVerConf, "1.0")
	term.Manager.MapSet(manage.IDMapConf, "test", "hello")
	packet.UpdateCmds(CmdSync, "test", "")
	cb.processSync(term, packet)
	cmds = packet.Cmds()
	assert.Equal(t, "ok", cmds[0])
	assert.Equal(t, "1.0", cmds[1])
	v, _ := term.Protocol().Marshal("hello")
	assert.Equal(t, v, packet.Data())

	// 已是最新
	packet.UpdateCmds(CmdSync, "test", "1.0")
	cb.processSync(term, packet)
	cmds = packet.Cmds()
	assert.Equal(t, "newest", cmds[0])
}

func Test_TerminalCallback_ResSend(t *testing.T) {
	term := mockTerminal()
	cb := term.Conf.ConnCallback.(*TerminalCallback)
	packet := NewMPPacket()

	msg := &Msg{
		Action: CmdResSend,
		Code:   "0",
	}
	msg.UpdateFrom("bid", "unfound", "1")
	msg.UpdatePacket(packet)

	cb.processResSend(term, packet)
	cmds := packet.Cmds()
	assert.Equal(t, "err", cmds[0])
	assert.Contains(t, cmds[1], "unfound")

	msg.UpdateFrom("bid", term.Name, "2")
	msg.UpdatePacket(packet)
	cb.processResSend(term, packet)
	cmds = packet.Cmds()
	assert.Equal(t, "ok", cmds[0])
	msgRes, ok := term.ResQueue.Pop(true)
	assert.True(t, ok)
	assert.Equal(t, CmdResRecv, msgRes.Action)
	assert.Equal(t, "0", msgRes.Code)
}

func Test_TerminalCallback_ReqSendLocal(t *testing.T) {
	term := mockTerminal()
	cb := term.Conf.ConnCallback.(*TerminalCallback)
	packet := NewMPPacket()
	msg := &Msg{
		Action:  CmdReqSend,
		Service: "a",
	}
	// unfound queue
	msg.UpdatePacket(packet)
	cb.processReqSend(term, CmdReqSend, packet)
	cmds := packet.Cmds()
	assert.Equal(t, "err", cmds[0])
	assert.Contains(t, cmds[1], "unfound")

	// has queue, but res timeout
	term.Manager.MapSet(manage.IDMapQueue, "a", NewMsgQueue(1))

	msg.UpdatePacket(packet)
	cb.processReqSend(term, CmdReqSend, packet)
	cmds = packet.Cmds()
	assert.Equal(t, "err", cmds[0])
	assert.Contains(t, cmds[1], "res timeout")
	term.MsgQueue("a").Pop(true)

	// reply res
	msgRes := &Msg{
		Action: CmdResRecv,
		Code:   "0",
	}
	msgRes.UpdateFrom("bid", term.Name, term.RIDNext())
	term.ResQueue.Push(msgRes, true)
	term.rid--
	msg.UpdatePacket(packet)
	cb.processReqSend(term, CmdReqSend, packet)
	cmds = packet.Cmds()
	assert.Equal(t, CmdResRecv, cmds[0])
}
