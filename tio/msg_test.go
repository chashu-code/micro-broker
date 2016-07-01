package tio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Msg_BTRids(t *testing.T) {
	msg := &Msg{
		From: "rid1",
	}

	bid, tid, rid := msg.BTRids()
	assert.Equal(t, "", bid)
	assert.Equal(t, "", tid)
	assert.Equal(t, "rid1", rid)

	msg.From = "tid1@rid1"
	bid, tid, rid = msg.BTRids()
	assert.Equal(t, "", bid)
	assert.Equal(t, "tid1", tid)
	assert.Equal(t, "rid1", rid)

	msg.From = "bid1@tid1@rid1"
	bid, tid, rid = msg.BTRids()
	assert.Equal(t, "bid1", bid)
	assert.Equal(t, "tid1", tid)
	assert.Equal(t, "rid1", rid)
}

func Test_Msg_UpdateFrom(t *testing.T) {
	msg := &Msg{
		From: "rid1",
	}

	msg.UpdateFrom("", "", "")
	assert.Equal(t, "@@", msg.From)

	msg.UpdateFrom("bid2", "tid2", "rid2")
	assert.Equal(t, "bid2@tid2@rid2", msg.From)
}

func Test_Msg_UpdatePacket(t *testing.T) {
	p := NewMPPacket()
	data := []byte("hello")
	msg := &Msg{
		From:    "b1@t1@r1",
		Nav:     "nav",
		Code:    "0",
		Service: "s1",
		Data:    data,
	}

	// req_send and job send
	for _, act := range []string{CmdReqSend, CmdJobSend} {
		msg.Action = act
		msg.UpdatePacket(p)
		assert.Equal(t, []string{msg.Action, msg.Service, msg.Nav}, p.Cmds())
		assert.Equal(t, data, p.Data())
	}

	// req_remote req_recv job_remote
	for _, act := range []string{CmdReqRemote, CmdReqRecv, CmdJobRemote} {
		msg.Action = act
		msg.UpdatePacket(p)
		assert.Equal(t, []string{msg.Action, msg.Service, msg.Nav, msg.From}, p.Cmds())
		assert.Equal(t, data, p.Data())
	}
	// res_send res_remote
	for _, act := range []string{CmdResSend, CmdResRemote} {
		msg.Action = act
		msg.UpdatePacket(p)
		assert.Equal(t, []string{msg.Action, msg.Code, msg.From}, p.Cmds())
		assert.Equal(t, data, p.Data())
	}

	// res_recv
	for _, act := range []string{CmdResRecv} {
		msg.Action = act
		msg.UpdatePacket(p)
		assert.Equal(t, []string{msg.Action, msg.Code}, p.Cmds())
		assert.Equal(t, data, p.Data())
	}

	// other error
	msg.Action = "unkonw"
	msg.UpdatePacket(p)
	assert.Equal(t, "err", p.Cmds()[0])
}
