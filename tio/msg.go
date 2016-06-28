package tio

import (
	"fmt"
	"strings"
)

const (
	// CmdReqSend 请求发送指令
	CmdReqSend = "req_send"
	// CmdReqRemote 远端请求指令
	CmdReqRemote = "req_remote"
	// CmdReqRecv 请求接收指令
	CmdReqRecv = "req_recv"

	// CmdJobSend 任务发送指令
	CmdJobSend = "job_send"
	// CmdJobRemote 远端任务指令
	CmdJobRemote = "job_remote"

	// CmdResSend 应答发送指令
	CmdResSend = "res_send"
	// CmdResRemote 远端应答指令
	CmdResRemote = "res_remote"
	// CmdResRecv 应答接收指令
	CmdResRecv = "res_recv"

	// CmdPull 拉取指令
	CmdPull = "pull"
	// CmdSub 订阅指令
	CmdSub = "sub"
	// CmdReg 注册指令
	CmdReg = "reg"
	// CmdSync 配置同步指令
	CmdSync = "sync"
)

// Msg 通讯消息结构
type Msg struct {
	Action  string
	From    string
	Service string
	Nav     string
	Data    []byte
	Code    string
}

// Clone 克隆一个新的msg
func (msg *Msg) Clone() *Msg {
	return &Msg{
		Action:  msg.Action,
		From:    msg.From,
		Service: msg.Service,
		Nav:     msg.Nav,
		Data:    msg.Data,
		Code:    msg.Code,
	}
}

// BTRids 返回 bid,tid,rid
func (msg *Msg) BTRids() (bid, tid, rid string) {
	ids := strings.Split(msg.From, "@")
	switch len(ids) {
	case 3:
		bid, tid, rid = ids[0], ids[1], ids[2]
	case 2:
		tid, rid = ids[0], ids[1]
	case 1:
		rid = ids[0]
	}

	return
}

// UpdateFrom 依据传入的 bid tid rid 更新 From
func (msg *Msg) UpdateFrom(bid, tid, rid string) {
	msg.From = strings.Join([]string{bid, tid, rid}, "@")
}

// UpdatePacket 以Msg属性，更新Packet
func (msg *Msg) UpdatePacket(p IPacket) {
	switch msg.Action {
	case CmdReqSend, CmdJobSend:
		p.UpdateCmds(msg.Action, msg.Service, msg.Nav)
		p.UpdateData(msg.Data)
	case CmdReqRemote, CmdReqRecv, CmdJobRemote:
		p.UpdateCmds(msg.Action, msg.Service, msg.Nav, msg.From)
		p.UpdateData(msg.Data)
	case CmdResSend, CmdResRemote:
		p.UpdateCmds(msg.Action, msg.Code, msg.From)
		p.UpdateData(msg.Data)
	case CmdResRecv:
		p.UpdateCmds(msg.Action, msg.Code)
		p.UpdateData(msg.Data)
	default:
		p.UpdateCmds("err", fmt.Sprintf("wrong msg(%s) update packet", msg.Action))
	}
}
