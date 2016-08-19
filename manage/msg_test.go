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
	assert.NotEmpty(t, msg.BID)
	assert.NotEmpty(t, msg.TID)
}

func Test_Msg_IsDead(t *testing.T) {
	msg := &Msg{}

	assert.True(t, msg.IsDead())

	msg.DeadLine = uint(time.Now().Unix() + 1)
	assert.False(t, msg.IsDead())
}

func Test_Msg_Clone(t *testing.T) {
	now := uint(time.Now().Unix())

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
