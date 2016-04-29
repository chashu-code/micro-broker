package tio

import (
	"crypto/md5"
	"encoding/json"
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
	p, ok := packet.(*SSDBPacket)
	if !ok {
		c.Log(log.Fields{
			"tid": t.Name,
		}).Error("Terminal recv a error packet")
		return false
	}

	cmd := p.CMD()
	args := p.StrArgs()

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
		p.Update("err", fmt.Sprintf("wrong cmd %s %v", cmd, args))
	}

	if t.Manager.Verbose() && cmd != "pull" {
		c.Log(log.Fields{
			"tid":      t.Name,
			"recvCmd":  cmd,
			"recvArgs": args,
			"sendCmd":  p.CMD(),
			"sendArgs": p.StrArgs(),
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
	args := p.StrArgs()

	if len(args) < 1 {
		p.Update("err", fmt.Sprintf("cmd %s need args: service nav data", cmd))
		return
	}

	msg := p.ToMsg()
	if msg == nil {
		p.Update("err", fmt.Sprintf("%s msg decode fail", cmd))
		return
	}

	dest := t.Manager.RouteNextDest(msg.Service)
	if dest == "" {
		p.Update("err", fmt.Sprintf("unfound router of service %q", msg.Service))
		return
	}

	addr := t.Manager.DestAddr(dest)
	if addr != manage.AddrLocal {
		p.Update("err", fmt.Sprintf("unsupported remote request to %q(%s)", dest, addr))
		return
	}

	var queue *MsgQueue

	if cmd == CmdJobSend {
		queue = t.MsgQueue(KeyJobQueue)
	} else {
		queue = t.MsgQueue(msg.Service)
		msg.Action = CmdReqRecv
	}

	if queue == nil {
		p.Update("err", fmt.Sprintf("unfound service queue %q", msg.Service))
		return
	}

	msg.updateFrom(t.Manager.Name(), t.Name, t.RIDNext())

	if !queue.Push(msg, false) {
		p.Update("err", fmt.Sprintf("push service queue %q timeout", msg.Service))
		return
	}

	var ok bool
	var msgRes *Msg

	for {
		msgRes, ok = t.ResQueue.Pop(true)
		if !ok {
			p.Update("err", "wait res timeout")
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
	args := p.StrArgs()

	if len(args) < 1 {
		p.Update("err", "cmd res_send need args: code data from")
		return
	}

	msg := p.ToMsg()
	if msg == nil {
		p.Update("err", "res_send msg decode fail")
		return
	}

	dest, tid, _ := msg.btrIDS()

	addr := t.Manager.DestAddr(dest)
	if addr != manage.AddrLocal {
		p.Update("err", fmt.Sprintf("unsupported remote response to %q(%s)", dest, addr))
		return
	}

	queue := t.MsgQueue(tid)
	if queue == nil {
		p.Update("err", fmt.Sprintf("unfound res queue(%s), client has closed maybe", tid))
		return
	}

	msg.Action = CmdResRecv
	if queue.Push(msg, false) {
		p.Update("ok")
	} else {
		p.Update("err", fmt.Sprintf("push res queue(%s) timeout", tid))
	}
}

func (c *TerminalCallback) processReg(t *Terminal, p IPacket) {
	args := p.StrArgs()
	if len(args) < 1 {
		p.Update("err", "cmd reg need args: services_str")
		return
	}

	subToken := fmt.Sprintf("%x", md5.Sum([]byte(args[0])))
	if v, ok := t.Manager.MapGet(manage.IDMapConf, subToken); ok {
		if _, ok = v.(*MultiMsgQueuePoper); ok {
			p.Update("ok", subToken)
			return
		}
	}

	mapQueue := t.Manager.Map(manage.IDMapQueue)
	if mapQueue == nil {
		panic(errors.New("unfound mapQueue with server"))
	}

	services := strings.Split(args[0], ",")
	poper := NewMultiMsgQueuePoper(mapQueue, services)

	t.Manager.MapSet(manage.IDMapConf, subToken, poper)

	p.Update("ok", subToken)
}

func (c *TerminalCallback) processPull(t *Terminal, p IPacket) {
	args := p.StrArgs()
	if len(args) < 1 {
		p.Update("err", "cmd pull need args: sub_token")
		return
	}

	subToken := args[0]

	var poper *MultiMsgQueuePoper

	if v, ok := t.Manager.MapGet(manage.IDMapConf, subToken); ok {
		poper, _ = v.(*MultiMsgQueuePoper)
	}

	if poper == nil {
		p.Update("err", "unregistered sub_token")
		return
	}

	msg, ok := poper.Pop()
	if !ok {
		p.Update("empty")
		return
	}

	msg.UpdatePacket(p)
}

func (c *TerminalCallback) processSync(t *Terminal, p IPacket) {
	args := p.StrArgs()

	if len(args) < 2 {
		p.Update("err", "cmd sync need args: name, version")
		return
	}

	name, ver := args[0], args[1]
	if v, ok := t.Manager.MapGet(manage.IDMapConf, KeyVerConf); ok {
		var verConf string
		if verConf, ok = v.(string); ok {
			if verConf != ver { // 版本有差异
				if v, ok = t.Manager.MapGet(manage.IDMapConf, name); ok {
					data, err := json.Marshal(v)
					if err != nil {
						p.Update("err", fmt.Sprintf("config encode error: %s", err))
						return
					}
					p.Update("ok", verConf, data)
					return
				}
			}
		}
	}
	p.Update("newest")
}
