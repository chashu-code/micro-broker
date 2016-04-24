package sprite

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	// ActREQ 消息行为 request
	ActREQ = "req"
	// ActRES 消息行为 response
	ActRES = "res"
	// ActREG 消息行为 register
	ActREG = "reg"
	// ActSYNC 消息行为 更新config
	ActSYNC = "sync"
	// ActRPT 消息系统为 汇报 broker状态信息
	ActRPT = "report"
	// ActJOB 消息行为 job 推送任务
	ActJOB = "job"
	// ActStart 消息行为 启动服务
	ActStart = "start"

	// ActSub 消息行为 监听服务
	ActSub = "sub"

	msgByteMax = 4 * 1024 * 1024
)

var (
	// ErrMsgToBig 消息体超过限定大小
	ErrMsgToBig = fmt.Errorf("msg json byes size to big (> %v)", msgByteMax)
	// ErrNeedRegMsg 消息Action需为reg
	ErrNeedRegMsg = fmt.Errorf("msg action must be reg")
	// ErrInvalidRegMsg 消息不是有效reg
	ErrInvalidRegMsg = fmt.Errorf("reg msg service can't be empty")
)

// Msg 通讯消息结构
type Msg struct {
	Action  string
	From    string
	Channel string
	Service string
	Nav     string
	Data    map[string]interface{}
}

// MsgFromJSON 反序列化 Msg
func MsgFromJSON(data []byte) (msg *Msg, err error) {
	msg = &Msg{
		Data: make(map[string]interface{}),
	}

	err = json.Unmarshal(data, &msg.Data)

	if err == nil {
		ok := true
		if msg.Action, ok = msg.Data["action"].(string); !ok {
			err = errors.New("'action' must be a string")
			return
		}

		if msg.From, ok = msg.Data["from"].(string); !ok {
			err = errors.New("'from' must be a string")
			return
		}

		if msg.Service, ok = msg.Data["service"].(string); !ok {
			err = errors.New("'service' must be a string")
			return
		}
		msg.Nav, _ = msg.Data["nav"].(string)
	}

	return
}

// ToJSON 序列化 Msg
func (msg *Msg) ToJSON() ([]byte, error) {
	if msg.Data == nil {
		msg.Data = map[string]interface{}{}
	}
	msg.Data["from"] = msg.From
	msg.Data["action"] = msg.Action
	msg.Data["service"] = msg.Service
	msg.Data["nav"] = msg.Nav

	data, err := json.Marshal(msg.Data)
	if err == nil {
		if len(data) > msgByteMax {
			data = nil
			err = ErrMsgToBig
		}
	}
	return data, err
}

// BTR_IDS 返回 bid,tid,rid
func (msg *Msg) btrIDS() (bid, tid, rid string) {
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

func (msg *Msg) updateFrom(bid, tid, rid string) {
	msg.From = strings.Join([]string{bid, tid, rid}, "@")
}
