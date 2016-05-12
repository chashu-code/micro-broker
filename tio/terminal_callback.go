package tio

import (
	"crypto/md5"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/chashu-code/micro-broker/log"
	"github.com/chashu-code/micro-broker/manage"
)

// TerminalCallback 终端回调处理
type TerminalCallback struct {
	log.Able
}

// OnConnect 终端链接回调处理
func (c *TerminalCallback) OnConnect(t *Terminal) bool {
	c.Log(log.Fields{
		"tid": t.Name,
	}).Debug("Terminal connect")
	return true
}

// OnData 终端接收数据回调处理
func (c *TerminalCallback) OnData(t *Terminal, packet IPacket) bool {
	p, ok := packet.(*MPPacket)
	if !ok {
		c.Log(log.Fields{
			"tid": t.Name,
		}).Error("Terminal recv a error packet")
		return false
	}

	cmds := p.Cmds()
	cmd := cmds[0]

	switch cmd {
	case CmdReg: // 注册监听
		c.processReg(t, p)
	case CmdPull: // 拉取指令
		c.processPull(t, p)
	case CmdJobSend, CmdReqSend:
		c.processReqSend(t, cmd, p)
	case CmdResSend: // 发送应答
		c.processResSend(t, p)
	case CmdSync: // 处理同步
		c.processSync(t, p)
	default:
		p.UpdateCmds("err", fmt.Sprintf("wrong cmds %v", cmds))
	}

	cmdsRes := p.Cmds()

	// 若为错误，则不该携带Data
	if cmdsRes[0] == "err" {
		p.UpdateData(nil)
	}

	if t.Manager.Verbose() && (cmd != "pull" || cmdsRes[0] != "empty") {
		c.Log(log.Fields{
			"tid":      t.Name,
			"recvCmds": cmds,
			"sendCmds": cmdsRes,
		}).Debug("Terminal data")
	}

	if err := t.SendPacket(p); err != nil {
		c.Log(log.Fields{
			"tid":   t.Name,
			"error": err,
		}).Error("Terminal send packet fail")
		return false
	}

	return true
}

// OnClose 终端关闭回调处理
func (c *TerminalCallback) OnClose(t *Terminal) {
	c.Log(log.Fields{
		"tid": t.Name,
	}).Debug("Terminal close")
}

// OnError 终端发生错误时回调处理
func (c TerminalCallback) OnError(t *Terminal, err interface{}) {
	c.Log(log.Fields{
		"tid":   t.Name,
		"error": err,
		"stack": (string)(debug.Stack()),
	}).Error("Terminal error")
}

func (c *TerminalCallback) processReqSend(t *Terminal, cmd string, p IPacket) {
	// TODO job req_remote
	msg := p.ToMsg()
	if msg == nil {
		p.UpdateCmds("err", fmt.Sprintf("%s msg decode fail", cmd))
		return
	}

	dest := t.Manager.RouteNextDest(msg.Service)
	if dest == "" {
		p.UpdateCmds("err", fmt.Sprintf("unfound router of service %q", msg.Service))
		return
	}

	addr := t.Manager.DestAddr(dest)
	if addr != manage.AddrLocal {
		p.UpdateCmds("err", fmt.Sprintf("unsupported remote request to %q(%s)", dest, addr))
		return
	}

	var queue *MsgQueue

	if cmd == CmdJobSend {
		queue = t.MsgQueue(manage.KeyJobQueue)
	} else {
		queue = t.MsgQueue(msg.Service)
		msg.Action = CmdReqRecv
	}

	if queue == nil {
		p.UpdateCmds("err", fmt.Sprintf("unfound service queue %q", msg.Service))
		return
	}

	msg.updateFrom(t.Manager.Name(), t.Name, t.RIDNext())

	if !queue.Push(msg, false) {
		p.UpdateCmds("err", fmt.Sprintf("push service queue %q timeout", msg.Service))
		return
	}

	var ok bool
	var msgRes *Msg

	for {
		msgRes, ok = t.ResQueue.Pop(true)
		if !ok {
			p.UpdateCmds("err", "wait res timeout")
			return
		}
		_, _, rid := msgRes.btrIDS()
		if rid != t.RID() {
			c.Log(log.Fields{
				"tid":     t.Name,
				"rid":     t.RID(),
				"ridRecv": rid,
			}).Warning("Terminal recv a overdue res msg")
			continue
		}
		msgRes.UpdatePacket(p)
		return
	}
}

func (c *TerminalCallback) processResSend(t *Terminal, p IPacket) {
	// TODO res_remote
	msg := p.ToMsg()
	if msg == nil {
		p.UpdateCmds("err", "res_send msg decode fail")
		return
	}

	dest, tid, _ := msg.btrIDS()

	addr := t.Manager.DestAddr(dest)
	if addr != manage.AddrLocal {
		p.UpdateCmds("err", fmt.Sprintf("unsupported remote response to %q(%s)", dest, addr))
		return
	}

	queue := t.MsgQueue(tid)
	if queue == nil {
		p.UpdateCmds("err", fmt.Sprintf("unfound res queue(%s), client has closed maybe", tid))
		return
	}

	msg.Action = CmdResRecv
	if queue.Push(msg, false) {
		p.UpdateCmds("ok")
		p.UpdateData(nil) // 不需要携带Data
	} else {
		p.UpdateCmds("err", fmt.Sprintf("push res queue(%s) timeout", tid))
	}
}

func (c *TerminalCallback) processReg(t *Terminal, p IPacket) {
	args := p.Cmds()
	if len(args) < 2 {
		p.UpdateCmds("err", "cmd reg need args: services_str")
		return
	}

	subToken := fmt.Sprintf("%x", md5.Sum([]byte(args[1])))
	if v, ok := t.Manager.MapGet(manage.IDMapConf, subToken); ok {
		if _, ok = v.(*MultiMsgQueuePoper); ok {
			p.UpdateCmds("ok", subToken)
			return
		}
	}

	mapQueue := t.Manager.Map(manage.IDMapQueue)
	if mapQueue == nil {
		panic(errors.New("unfound mapQueue with server"))
	}

	services := strings.Split(args[1], ",")
	poper := NewMultiMsgQueuePoper(mapQueue, services)

	t.Manager.MapSet(manage.IDMapConf, subToken, poper)

	p.UpdateCmds("ok", subToken)
}

func (c *TerminalCallback) processPull(t *Terminal, p IPacket) {
	args := p.Cmds()
	if len(args) < 2 {
		p.UpdateCmds("err", "cmd pull need args: sub_token")
		return
	}

	subToken := args[1]

	var poper *MultiMsgQueuePoper

	if v, ok := t.Manager.MapGet(manage.IDMapConf, subToken); ok {
		poper, _ = v.(*MultiMsgQueuePoper)
	}

	if poper == nil {
		p.UpdateCmds("err", "unregistered sub_token")
		return
	}

	msg, ok := poper.Pop()
	if !ok {
		p.UpdateCmds("empty")
		return
	}

	// c.Log(log.Fields{
	// 	"tid": t.Name,
	// 	"msg": msg,
	// }).Warn("Terminal pull a msg")

	msg.UpdatePacket(p)
}

func (c *TerminalCallback) processSync(t *Terminal, p IPacket) {
	args := p.Cmds()

	if len(args) < 3 {
		p.UpdateCmds("err", "cmd sync need args: name, version")
		return
	}

	name, ver := args[1], args[2]
	if v, ok := t.Manager.MapGet(manage.IDMapConf, manage.KeyVerConf); ok {
		var verConf string
		if verConf, ok = v.(string); ok {
			if verConf != ver { // 版本有差异
				if v, ok = t.Manager.MapGet(manage.IDMapConf, name); ok {
					data, err := t.Protocol().Marshal(v)
					if err != nil {
						p.UpdateCmds("err", fmt.Sprintf("config encode error: %s", err))
						return
					}
					p.UpdateCmds("ok", verConf)
					p.UpdateData(data)
					return
				}
			}
		}
	}
	p.UpdateCmds("newest")
}
