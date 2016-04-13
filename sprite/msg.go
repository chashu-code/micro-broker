package sprite

import "encoding/json"

// Msg 通讯消息结构
type Msg struct {
	Action  string
	From    string
	Channel string
	Data    map[string]interface{}
}

// MsgFromJSON 反序列化 Msg
func MsgFromJSON(data []byte) (*Msg, error) {
	msg := &Msg{
		Data: make(map[string]interface{}),
	}

	err := json.Unmarshal(data, &msg.Data)

	if err == nil {
		msg.From = msg.Data["from"].(string)
		msg.Action = msg.Data["action"].(string)
	}

	return msg, err
}

// ToJSON 序列化 Msg
func (msg *Msg) ToJSON() ([]byte, error) {
	msg.Data["from"] = msg.From
	return json.Marshal(msg.Data)
}
