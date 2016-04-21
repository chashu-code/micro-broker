package sprite

import "time"

import "github.com/chashu-code/micro-broker/fsm"

import "github.com/chashu-code/micro-broker/adapter"
import "github.com/chashu-code/micro-broker/log"
import "github.com/imdario/mergo"

// Pub 服务结构
type Pub struct {
	Sprite
	redis            adapter.IRedis
	redisBuilder     adapter.RediserBuilder
	gatewayURI       string
	intervalOverhaul int
	intervalWaitMsg  int
	msNetTimeout     int

	timesOverhaul uint
	mqOp          *MsgQueueOp
}

// PubRun 构造并运行 Pub 精灵
func PubRun(options map[string]interface{}) ISprite {
	msgQueue := options["msgQueue"].(chan *Msg)

	s := &Pub{
		gatewayURI:   options["gatewayURI"].(string),
		redisBuilder: options["redisBuilder"].(adapter.RediserBuilder),
	}

	s.addFlowDesc(options)
	s.addCallbackDesc(options)
	s.fillIntervals(options)

	s.mqOp = NewMsgQueueOp(msgQueue, s.intervalWaitMsg)

	s.Run(options)
	return s
}

// Report 汇报Pub信息
func (s *Pub) Report() map[string]interface{} {
	s.report["timesOverhaul"] = s.timesOverhaul
	return s.report
}

func (s *Pub) fillIntervals(options map[string]interface{}) {
	opDefault := map[string]interface{}{
		"intervalOverhaul": 1000,
		"intervalWaitMsg":  1000,
		"msNetTimeout":     10000,
	}
	mergo.Merge(&options, opDefault)

	s.intervalOverhaul = options["intervalOverhaul"].(int)
	s.intervalWaitMsg = options["intervalWaitMsg"].(int)
	s.msNetTimeout = options["msNetTimeout"].(int)
}

func (s *Pub) addFlowDesc(options map[string]interface{}) {
	options["initState"] = SpriteInitState
	options["flowDesc"] = fsm.FlowDescList{
		{Evt: "Overhaul", SrcList: []string{SpriteInitState, "overhaul-fail"}, Dst: "overhaul"},
		{Evt: "Fail", SrcList: []string{"overhaul", "pub-msg"}, Dst: "overhaul-fail"},
		{Evt: "Success", SrcList: []string{"overhaul", "pub-msg"}, Dst: "wait-msg"},
		{Evt: "Pub", SrcList: []string{"wait-msg"}, Dst: "pub-msg"},
		{Evt: "Timeout", SrcList: []string{"wait-msg"}, Dst: "wait-msg-timeout"},
		{Evt: "Retry", SrcList: []string{"wait-msg-timeout"}, Dst: "wait-msg"},
	}
}

func (s *Pub) addCallbackDesc(options map[string]interface{}) {
	desc := fsm.CallbackDesc{
		"enter-initial":          s.enterInitial,
		"enter-overhaul":         s.enterOverHaul,
		"enter-overhaul-fail":    s.enterOverHaulFail,
		"enter-wait-msg":         s.enterWaitMsg,
		"enter-wait-msg-timeout": s.enterWaitMsgTimeout,
		"enter-pub-msg":          s.enterPubMsg,
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

func (s *Pub) enterInitial(_ fsm.CallbackKey, evt *fsm.Event) error {
	return fsm.EvtNext(evt, "Overhaul")
}

func (s *Pub) enterOverHaul(_ fsm.CallbackKey, evt *fsm.Event) error {
	var err error

	s.timesOverhaul++

	if s.redis == nil || s.redis.LastCritical() != nil {
		// 虽然目前使用的Redis库，在报错时，会自动关闭链接
		// 但还是加一层确保
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

	resp := s.redis.Cmd("PING")
	if pong, err := resp.Str(); err != nil || pong != "PONG" {
		s.Log(log.Fields{
			"error": err,
			"pong":  pong,
		}).Error("ping redis fail")

		return fsm.EvtNext(evt, "Fail")
	}

	return fsm.EvtNext(evt, "Success")
}

func (s *Pub) enterOverHaulFail(_ fsm.CallbackKey, evt *fsm.Event) error {
	time.Sleep(time.Duration(s.intervalOverhaul) * time.Millisecond)
	return fsm.EvtNext(evt, "Overhaul")
}

func (s *Pub) enterWaitMsg(_ fsm.CallbackKey, evt *fsm.Event) error {
	if msg, ok := s.mqOp.Pop(true); ok {
		evt.Next = &fsm.Event{
			Name: "Pub",
			Args: []interface{}{msg},
		}
		return nil
	}
	return fsm.EvtNext(evt, "Timeout")
}

func (s *Pub) enterWaitMsgTimeout(_ fsm.CallbackKey, evt *fsm.Event) error {
	return fsm.EvtNext(evt, "Retry")
}

func (s *Pub) enterPubMsg(_ fsm.CallbackKey, evt *fsm.Event) error {
	if len(evt.Args) == 1 {
		if msg, ok := evt.Args[0].(*Msg); ok {
			if !s.pubMsg(msg) {
				return fsm.EvtNext(evt, "Fail")
			}
		}
	}
	return fsm.EvtNext(evt, "Success")
}

func (s *Pub) pubMsg(msg *Msg) bool {
	data, err := msg.ToJSON()

	if err != nil {
		s.Log(log.Fields{
			"msg":   msg,
			"error": err,
		}).Error("encode msg fail")
		return true // 编码出错，直接丢弃该信息，不必检修
	}

	resp := s.redis.Cmd("PUBLISH", msg.Channel, data)
	if resp.Err != nil {
		s.Log(log.Fields{
			"error": resp.Err,
		}).Error("pub msg fail")
		return false
	}

	return true
}
