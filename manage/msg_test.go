package manage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Msg_PidOfRID(t *testing.T) {
	msg := &Msg{}

	pid, err := msg.PidOfRID()
	assert.Error(t, err)

	msg.RID = "123|xxxxx"
	pid, err = msg.PidOfRID()
	assert.NoError(t, err)
	assert.Equal(t, "123", pid)
}

func Test_Msg_FillWithReq(t *testing.T) {
	mgr := NewManager(NewConfig())
	msg := &Msg{}

	assert.Empty(t, msg.BID)
	assert.Empty(t, msg.TID)

	msg.FillWithReq(mgr)
	assert.Equal(t, mgr.IP(), msg.BID)
	tid := mgr.NextTID()
	assert.NotEqual(t, tid, msg.TID)
	assert.Contains(t, msg.TID, mgr.IP())
}

func Test_Msg_IsDead(t *testing.T) {
	msg := &Msg{}

	assert.True(t, msg.IsDead())

	now := time.Now().Unix()
	msg.DeadLine = now - 1
	assert.True(t, msg.IsDead())
	msg.DeadLine = now
	assert.False(t, msg.IsDead())
	msg.DeadLine = now + 1
	assert.False(t, msg.IsDead())

}

func Test_Msg_TubeName(t *testing.T) {
	msg := &Msg{}
	assert.Empty(t, msg.TubeName())
	msg.Topic = "abc"
	assert.Equal(t, msg.Topic, msg.TubeName())
	msg.Channel = "say"
	assert.Equal(t, msg.Topic+"-"+msg.Channel, msg.TubeName())
}

func Test_Msg_ServiceName(t *testing.T) {
	msg := &Msg{}
	assert.Empty(t, msg.ServiceName())
	msg.Topic = "abc"
	assert.Equal(t, msg.Topic, msg.ServiceName())
	msg.Channel = "say"
	assert.Equal(t, msg.Topic+"/"+msg.Channel, msg.ServiceName())
}

func Test_Msg_CodeToPutArgs(t *testing.T) {
	msg := &Msg{}
	for _, code := range []string{"", "aaa", "11|11"} {
		msg.Code = code
		pri, delay, ttr, err := msg.CodeToPutArgs()
		assert.Nil(t, err)
		assert.Equal(t, uint32(100), pri)
		assert.Equal(t, time.Duration(0), delay)
		assert.Equal(t, 5*time.Minute, ttr)
	}
	msg.Code = "2|x|4"
	pri, delay, ttr, err := msg.CodeToPutArgs()
	assert.Error(t, err)

	msg.Code = "2|3|4"
	pri, delay, ttr, err = msg.CodeToPutArgs()
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), pri)
	assert.Equal(t, 3*time.Second, delay)
	assert.Equal(t, 4*time.Second, ttr)
}

func Test_Msg_Clone(t *testing.T) {
	now := time.Now().Unix()

	msg := &Msg{
		Topic:    "topic",
		Channel:  "channel",
		Nav:      "nav",
		Action:   "req",
		BID:      "b",
		RID:      "r",
		TID:      "t",
		Data:     "data",
		SendTime: now,
		DeadLine: now + 1,
		V:        1,
	}

	msgRes := msg.Clone(ActRes)
	assert.Equal(t, ActRes, msgRes.Action)
	assert.Empty(t, msgRes.Topic)
	assert.Empty(t, msgRes.Channel)
	assert.Empty(t, msgRes.Nav)
	assert.Equal(t, "0", msgRes.Code)
	assert.Equal(t, msg.Data, msgRes.Data)
	assert.Equal(t, msg.BID, msgRes.BID)
	assert.Equal(t, msg.RID, msgRes.RID)
	assert.Equal(t, msg.TID, msgRes.TID)
	assert.Equal(t, msg.SendTime, msgRes.SendTime)
	assert.Equal(t, msg.DeadLine, msgRes.DeadLine)
	assert.Equal(t, msg.V, msgRes.V)

	msgReq := msg.Clone(ActReq)

	assert.Equal(t, ActReq, msgReq.Action)
	assert.Equal(t, msg.Topic, msgReq.Topic)
	assert.Equal(t, msg.Channel, msgReq.Channel)
	assert.Equal(t, msg.Nav, msgReq.Nav)
	assert.Empty(t, msg.Code)
	assert.Equal(t, msg.Data, msgRes.Data)
	assert.Equal(t, msg.BID, msgRes.BID)
	assert.Equal(t, msg.RID, msgRes.RID)
	assert.Equal(t, msg.TID, msgRes.TID)
	assert.Equal(t, msg.SendTime, msgRes.SendTime)
	assert.Equal(t, msg.DeadLine, msgRes.DeadLine)
	assert.Equal(t, msg.V, msgRes.V)
}
