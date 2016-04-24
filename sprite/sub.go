package sprite

import (
	"time"

	"github.com/chashu-code/micro-broker/adapter"
	"github.com/chashu-code/micro-broker/fsm"
	"github.com/chashu-code/micro-broker/log"
	"github.com/imdario/mergo"
	cmap "github.com/streamrail/concurrent-map"
)

// Sub 服务结构
type Sub struct {
	Sprite
	redis        adapter.IRedis
	redisBuilder adapter.RediserBuilder
	gatewayURI   string

	msOverhaulInterval int
	msQueueOpTimeout   int
	msNetTimeout       int

	mapQueueOp cmap.ConcurrentMap
	msgWillPut *Msg
	opExecer   *MsgQueueOpExecer

	Channels []string

	timesOverhaul       uint
	timesQueueOpTimeout uint
	timesSubed          uint
}

// SubRun 构造并运行 Sub 精灵
func SubRun(options map[string]interface{}) ISprite {
	s := &Sub{
		gatewayURI:   options["gatewayURI"].(string),
		redisBuilder: options["redisBuilder"].(adapter.RediserBuilder),
		Channels:     options["channels"].([]string),
	}

	s.opExecer = NewMsgQueueOpExecer()

	s.addFlowDesc(options)
	s.addCallbackDesc(options)
	s.fillMsTimeoutOpts(options)

	s.mapQueueOp = options["mapQueueOp"].(cmap.ConcurrentMap)
	s.Run(options)
	return s
}

// Report 汇报Sub信息
func (s *Sub) Report() map[string]interface{} {
	s.report["timesOverhaul"] = s.timesOverhaul
	s.report["timesQueueOpTimeout"] = s.timesQueueOpTimeout
	s.report["timesSubed"] = s.timesSubed
	return s.report
}

func (s *Sub) fillMsTimeoutOpts(options map[string]interface{}) {
	opDefault := map[string]interface{}{
		"msOverhaulInterval": NetTimeoutDefault,
		"msNetTimeout":       NetTimeoutDefault,
	}
	mergo.Merge(&options, opDefault)

	s.msOverhaulInterval = options["msOverhaulInterval"].(int)
	s.msNetTimeout = options["msNetTimeout"].(int)
}

func (s *Sub) addFlowDesc(options map[string]interface{}) {
	options["initState"] = SpriteInitState
	options["flowDesc"] = fsm.FlowDescList{
		{Evt: "Overhaul", SrcList: []string{SpriteInitState, "overhaul-fail"}, Dst: "overhaul"},
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
	time.Sleep(time.Duration(s.msOverhaulInterval) * time.Millisecond)
	return fsm.EvtNext(evt, "Overhaul")
}

func (s *Sub) enterWaitMsg(_ fsm.CallbackKey, evt *fsm.Event) error {
	data, err := s.redis.Receive()
	switch err {
	case nil:
		if msg, errDecode := MsgFromJSON([]byte(data)); errDecode == nil {
			s.msgWillPut = msg
		} else {
			s.Log(log.Fields{
				"error": errDecode,
			}).Info("sub msg decode json fail")
			s.msgWillPut = nil
		}
		return fsm.EvtNext(evt, "Success")
	case adapter.ErrReceiveTimeout:
		return fsm.EvtNext(evt, "Timeout")
	default:
		s.Log(log.Fields{
			"error": err,
		}).Error("wait sub msg fail")
		return fsm.EvtNext(evt, "Fail")
	}
}

func (s *Sub) enterWaitMsgTimeout(_ fsm.CallbackKey, evt *fsm.Event) error {
	return fsm.EvtNext(evt, "Retry")
}

func (s *Sub) enterPutMsg(_ fsm.CallbackKey, evt *fsm.Event) error {
	msg := s.msgWillPut

	if msg != nil {
		var key string

		switch msg.Action {
		case ActREQ:
			// 推入相关的 Service Queue
			key = msg.Service
		case ActRES:
			// 推入相关的 Terminal Queue
			_, tid, _ := msg.btrIDS()
			key = tid
		case ActJOB:
			// 推入相关的 Job Queue
			key = KeyJobQueue
		case ActSYNC:
			// 推入 Broker Queue
			key = KeyBrokerQueue
		default:
			// 意料之外的消息，跳过
			s.Log(log.Fields{
				"action": msg.Action,
			}).Info("error sub msg action")
		}

		if key == "" { // 未能解析出适合的key，跳过
			return fsm.EvtNext(evt, "Success")
		}

		// 仅推入存在的 Msg Queue
		if v, ok := s.mapQueueOp.Get(key); ok {
			if mqOp, ok := v.(*MsgQueueOp); ok {
				if !s.opExecer.Push(mqOp, msg, true) {
					s.timesQueueOpTimeout++
				} else {
					s.timesSubed++
				}
			}
		}
	}
	return fsm.EvtNext(evt, "Success")
}
