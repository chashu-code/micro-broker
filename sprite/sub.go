package sprite

import (
	"time"

	"github.com/chashu-code/micro-broker/adapter"
	"github.com/chashu-code/micro-broker/fsm"
	"github.com/chashu-code/micro-broker/log"
	"github.com/imdario/mergo"
)

// Sub 服务结构
type Sub struct {
	Sprite
	redis            adapter.IRedis
	redisBuilder     adapter.RediserBuilder
	gatewayURI       string
	intervalOverhaul int
	msPutTimeout     int
	msNetTimeout     int

	msgQueueOp *MsgQueueOp

	Channels []string

	timesOverhaul   uint
	timesPutTimeout uint
}

// SubRun 构造并运行 Sub 精灵
func SubRun(options map[string]interface{}) ISprite {
	s := &Sub{
		gatewayURI:   options["gatewayURI"].(string),
		redisBuilder: options["redisBuilder"].(adapter.RediserBuilder),
		Channels:     options["channels"].([]string),
	}

	s.addFlowDesc(options)
	s.addCallbackDesc(options)
	s.fillIntervals(options)

	msgQueue := options["msgQueue"].(chan *Msg)
	s.msgQueueOp = NewMsgQueueOp(msgQueue, s.msPutTimeout)

	s.Run(options)
	return s
}

// Report 汇报Sub信息
func (s *Sub) Report() map[string]interface{} {
	s.report["timesOverhaul"] = s.timesOverhaul
	s.report["timesPutTimeout"] = s.timesPutTimeout
	return s.report
}

func (s *Sub) fillIntervals(options map[string]interface{}) {
	opDefault := map[string]interface{}{
		"intervalOverhaul": 1000,
		"msPutTimeout":     1000,
		"msNetTimeout":     10000,
	}
	mergo.Merge(&options, opDefault)

	s.intervalOverhaul = options["intervalOverhaul"].(int)
	s.msPutTimeout = options["msPutTimeout"].(int)
	s.msNetTimeout = options["msNetTimeout"].(int)
}

func (s *Sub) addFlowDesc(options map[string]interface{}) {
	options["initState"] = "initial"
	options["flowDesc"] = fsm.FlowDescList{
		{Evt: "Overhaul", SrcList: []string{"initial", "overhaul-fail"}, Dst: "overhaul"},
		{Evt: "Fail", SrcList: []string{"overhaul", "wait-msg"}, Dst: "overhaul-fail"},
		{Evt: "Success", SrcList: []string{"overhaul", "put-msg"}, Dst: "wait-msg"},
		{Evt: "Success", SrcList: []string{"wait-msg"}, Dst: "put-msg"},
		{Evt: "Timeout", SrcList: []string{"wait-msg"}, Dst: "wait-msg-timeout"},
		{Evt: "Retry", SrcList: []string{"wait-msg-timeout"}, Dst: "wait-msg"},
	}
}

func (s *Sub) addCallbackDesc(options map[string]interface{}) {
	desc := fsm.CallbackDesc{
		"enter-initial":          s.enterInitial,
		"enter-overhaul":         s.enterOverHaul,
		"enter-overhaul-fail":    s.enterOverHaulFail,
		"enter-wait-msg":         s.enterWaitMsg,
		"enter-wait-msg-timeout": s.enterWaitMsgTimeout,
		"enter-put-msg":          s.enterPutMsg,
	}

	if opDesc, ok := options["callbackDesc"]; ok {
		for k, v := range opDesc.(fsm.CallbackDesc) {
			desc[k] = v
		}
	}

	// 尝试资源回收
	var cbStop fsm.CallbackFunc
	if cb, ok := desc["stop"]; ok {
		cbStop = cb
	}

	desc["stop"] = func(key fsm.CallbackKey, evt *fsm.Event) error {
		if s.redis != nil {
			s.redis.Close()
		}

		if cbStop != nil {
			return cbStop(key, evt)
		}
		return nil
	}

	options["callbackDesc"] = desc
}

func (s *Sub) enterInitial(_ fsm.CallbackKey, evt *fsm.Event) error {
	return fsm.EvtNext(evt, "Overhaul")
}

func (s *Sub) enterOverHaul(_ fsm.CallbackKey, evt *fsm.Event) error {
	var err error

	s.timesOverhaul++

	if s.redis == nil || s.redis.LastCritical() != nil {
		if s.redis != nil {
			s.redis.Close()
		}
		if s.redis, err = s.redisBuilder(s.gatewayURI, s.msNetTimeout); err != nil {
			s.Log(log.Fields{
				"uri":   s.gatewayURI,
				"error": err,
			}).Error("build redis fail")

			return fsm.EvtNext(evt, "Fail")
		}
	}

	channels := make([]interface{}, len(s.Channels))
	for i, c := range s.Channels {
		channels[i] = c
	}

	resp := s.redis.Subscribe(channels...)

	if resp.Err != nil {
		s.Log(log.Fields{
			"error": err,
		}).Error("subscribe redis fail")
		return fsm.EvtNext(evt, "Fail")
	}

	return fsm.EvtNext(evt, "Success")
}

func (s *Sub) enterOverHaulFail(_ fsm.CallbackKey, evt *fsm.Event) error {
	time.Sleep(time.Duration(s.intervalOverhaul) * time.Millisecond)
	return fsm.EvtNext(evt, "Overhaul")
}

func (s *Sub) enterWaitMsg(_ fsm.CallbackKey, evt *fsm.Event) error {
	data, err := s.redis.Receive()
	switch err {
	case nil:
		evt.Next = &fsm.Event{
			Name: "Success",
			Args: []interface{}{[]byte(data)},
		}
	case adapter.ErrReceiveTimeout:
		return fsm.EvtNext(evt, "Timeout")
	default:
		s.Log(log.Fields{
			"error": err,
		}).Error("wait sub msg fail")
		return fsm.EvtNext(evt, "Fail")
	}

	return nil
}

func (s *Sub) enterWaitMsgTimeout(_ fsm.CallbackKey, evt *fsm.Event) error {
	return fsm.EvtNext(evt, "Retry")
}

func (s *Sub) enterPutMsg(_ fsm.CallbackKey, evt *fsm.Event) error {
	if len(evt.Args) == 1 {
		if data, ok := evt.Args[0].([]byte); ok {
			if msg, err := MsgFromJSON(data); err == nil {
				// 若置入队列超时，则直接丢弃该消息
				// 客户端则作为超时响应
				if !s.msgQueueOp.Push(msg, true) {
					s.timesPutTimeout++
				}

			} else {
				s.Log(log.Fields{
					"error": err,
				}).Error("sub msg decode json fail")
			}
		}
	}
	return fsm.EvtNext(evt, "Success")
}
