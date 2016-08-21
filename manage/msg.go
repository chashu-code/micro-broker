package manage

import (
	"fmt"
	"strings"
	"time"

	"github.com/uber-go/zap"
)

const (
	// ActReq 请求指令
	ActReq = "req"
	// ActRes 应答指令
	ActRes = "res"
	// ActJob Job推送
	ActJob = "job"
)

// Msg 消息结构
type Msg struct {
	Action   string
	BID      string
	RID      string
	TID      string
	Topic    string
	Channel  string
	Nav      string
	SendTime int64
	DeadLine int64

	Data interface{}
	Code string

	V uint
}

// MarshalLog zap log 序列化接口方法
func (msg *Msg) MarshalLog(kv zap.KeyValue) error {
	kv.AddString("act", msg.Action)
	kv.AddString("bid", msg.BID)
	kv.AddString("rid", msg.RID)
	kv.AddString("tid", msg.TID)
	kv.AddString("topic", msg.Topic)
	kv.AddString("chan", msg.Channel)
	kv.AddString("nav", msg.Nav)
	kv.AddInt64("st", msg.SendTime)
	kv.AddInt64("dl", msg.DeadLine)
	kv.AddString("code", msg.Code)
	kv.AddObject("data", msg.Data)

	return nil
}

// Clone 克隆一个新的msg
func (msg *Msg) Clone(action string) *Msg {
	msgNew := &Msg{
		Action:   action,
		BID:      msg.BID,
		RID:      msg.RID,
		TID:      msg.TID,
		Data:     msg.Data,
		SendTime: msg.SendTime,
		DeadLine: msg.DeadLine,
		V:        msg.V,
	}

	if action == ActReq {
		msgNew.Topic = msg.Topic
		msgNew.Channel = msg.Channel
		msgNew.Nav = msg.Nav
	} else {
		if msg.Code == "" {
			msgNew.Code = "0"
		} else {
			msgNew.Code = msg.Code
		}
	}

	return msgNew
}

// TubeName 返回Job TubeName
func (msg *Msg) TubeName() string {
	return msg.comboName("-")
}

// ServiceName 返回 ServiceName
func (msg *Msg) ServiceName() string {
	return msg.comboName("/")
}

func (msg *Msg) comboName(join string) string {
	if msg.Channel == "" {
		return msg.Topic
	}
	return msg.Topic + join + msg.Channel
}

// PidOfRID 从RID中分析Pid
func (msg *Msg) PidOfRID() (string, error) {
	strs := strings.SplitN(msg.RID, "|", 2)
	if len(strs) == 2 {
		return strs[0], nil
	}
	return "", fmt.Errorf("Error Msg RID: %s", msg.RID)
}

// FillWithReq 填充 req msg 的一些属性
func (msg *Msg) FillWithReq(mgr *Manager) {
	if msg.BID == "" {
		msg.BID = mgr.IP()
	}

	if msg.TID == "" {
		msg.TID = mgr.NextTID()
	}
}

// IsDead 是否已过期？
func (msg *Msg) IsDead() bool {
	return msg.DeadLine < time.Now().Unix()
}
