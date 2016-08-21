package protocol

import "github.com/chashu-code/micro-broker/manage"

//go:generate msgp

type V1Protocol struct {
	Action   string `msg:"act"`
	BID      string `msg:"bid"`
	RID      string `msg:"rid"`
	TID      string `msg:"tid"`
	Topic    string `msg:"topic"`
	Channel  string `msg:"chan"`
	Nav      string `msg:"nav"`
	SendTime int64  `msg:"st"`
	DeadLine int64  `msg:"dl"`

	Data interface{} `msg:"data"`
	Code string      `msg:"code"`
}

func NewV1Protocol() manage.IProtocol {
	return &V1Protocol{}
}

func (p *V1Protocol) BytesToMsg(bts []byte) (*manage.Msg, error) {
	_, err := p.UnmarshalMsg(bts)

	if err != nil {
		return nil, err
	}

	msg := &manage.Msg{
		Action:   p.Action,
		BID:      p.BID,
		RID:      p.RID,
		TID:      p.TID,
		Topic:    p.Topic,
		Channel:  p.Channel,
		Nav:      p.Nav,
		SendTime: p.SendTime,
		DeadLine: p.DeadLine,
		Data:     p.Data,
		Code:     p.Code,
		V:        1,
	}

	return msg, nil
}

func (p *V1Protocol) MsgToBytes(msg *manage.Msg) ([]byte, error) {
	p.Action = msg.Action
	p.BID = msg.BID
	p.RID = msg.RID
	p.TID = msg.TID
	p.Topic = msg.Topic
	p.Channel = msg.Channel
	p.Nav = msg.Nav
	p.Code = msg.Code
	p.Data = msg.Data
	p.SendTime = msg.SendTime
	p.DeadLine = msg.DeadLine

	return p.MarshalMsg(nil)

}
