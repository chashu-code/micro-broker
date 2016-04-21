package sprite

import (
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/chashu-code/micro-broker/adapter"
	"github.com/chashu-code/micro-broker/fsm"
	"github.com/chashu-code/micro-broker/log"
	"github.com/imdario/mergo"
	cmap "github.com/streamrail/concurrent-map"
)

var (
	// ErrInvalidNextCmd 错误的Next指令
	ErrInvalidNextCmd = errors.New("invalid next cmd")
)

// Terminal 终端处理精灵
type Terminal struct {
	Sprite

	MQInOp  *MsgQueueOp
	MQOutOp *MsgQueueOp

	msQueueOpTimeout int
	msNetTimeout     int
	conn             *adapter.SSDBOper
	mapInRouter      cmap.ConcurrentMap
	mapOutRouter     cmap.ConcurrentMap
	mapConfig        cmap.ConcurrentMap
	services         []string
	countNext        int
	msgsRouting      []*Msg
	bid              string

	timesRouteFail    uint
	timesOutOpTimeout uint
}

// Report 汇报信息
func (s *Terminal) Report() map[string]interface{} {
	s.report["timesRouteFail"] = s.timesRouteFail
	s.report["timesOutOpTimeout"] = s.timesOutOpTimeout

	return s.report
}

// RegServices 返回注册的服务列表
func (s *Terminal) RegServices() []string {
	return s.services
}

// TerminalRun 启动Terminal精灵
func TerminalRun(options map[string]interface{}) *Terminal {
	s := &Terminal{}

	s.fillIntervals(options)
	s.addFlowDesc(options)
	s.addCallbackDesc(options)

	s.conn = adapter.NewSSDBOper(options["conn"].(net.Conn), s.msNetTimeout)
	s.mapInRouter = options["mapInRouter"].(cmap.ConcurrentMap)
	s.mapOutRouter = options["mapOutRouter"].(cmap.ConcurrentMap)
	s.mapConfig = options["mapConfig"].(cmap.ConcurrentMap)

	mq := options["msgOutQueue"].(chan *Msg)
	s.MQOutOp = NewMsgQueueOp(mq, s.msQueueOpTimeout)

	mq = make(chan *Msg, 10)
	s.MQInOp = NewMsgQueueOp(mq, s.msQueueOpTimeout)

	if v, ok := s.mapConfig.Get("brokerName"); ok {
		s.bid = v.(string)
	}

	s.Run(options)

	return s
}

func (s *Terminal) fillIntervals(options map[string]interface{}) {
	opDefault := map[string]interface{}{
		"msQueueOpTimeout": 1000,
		"msNetTimeout":     1000,
	}
	mergo.Merge(&options, opDefault)

	s.msQueueOpTimeout = options["msQueueOpTimeout"].(int)
	s.msNetTimeout = options["msNetTimeout"].(int)
}

func (s *Terminal) addFlowDesc(options map[string]interface{}) {
	options["initState"] = SpriteInitState
	options["flowDesc"] = fsm.FlowDescList{
		{Evt: "Reg", SrcList: []string{SpriteInitState}, Dst: "registering"},
		{Evt: "Fail", SrcList: []string{"receiving"}, Dst: "failed"},
		{Evt: "Route", SrcList: []string{"receiving"}, Dst: "routing"},
		{Evt: "Push", SrcList: []string{"routing"}, Dst: "pushing"},
		{Evt: "Success", SrcList: []string{"registering", "pushing"}, Dst: "receiving"},
		{Evt: "Retry", SrcList: []string{"failed"}, Dst: "receiving"},
	}
}

func (s *Terminal) addCallbackDesc(options map[string]interface{}) {
	desc := fsm.CallbackDesc{
		"enter-initial":     s.enterInitial,
		"enter-registering": s.enterRegistering,
		"enter-receiving":   s.enterReceiving,
		"enter-routing":     s.enterRouteing,
		"enter-pushing":     s.enterPushing,
		"enter-failed":      s.enterFailed,
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
		s.conn.Close()

		// 取消注册
		for _, service := range s.services {
			s.mapInRouter.Remove(service)
		}

		if cbStop != nil {
			return cbStop(key, evt)
		}
		return nil
	}

	options["callbackDesc"] = desc
}

func (s *Terminal) enterFailed(_ fsm.CallbackKey, evt *fsm.Event) error {
	return fsm.EvtNext(evt, "Retry")
}

func (s *Terminal) enterInitial(_ fsm.CallbackKey, evt *fsm.Event) error {
	return fsm.EvtNext(evt, "Reg")
}

func (s *Terminal) enterRegistering(_ fsm.CallbackKey, evt *fsm.Event) error {
	_, msgs, err := s.nextFromSocket()
	if err != nil {
		panic(err)
	}

	msg := msgs[0]

	if msg.Action != "reg" {
		panic(ErrNeedRegMsg)
	}

	if msg.Service == "" {
		panic(ErrInvalidRegMsg)
	}

	s.services = strings.Split(msg.Service, ",")

	// 注册 terminal 监听相关服务
	for _, service := range s.services {
		s.mapInRouter.Set(service, s)
	}

	// 推送配置信息
	msg = &Msg{
		Action: ActCFG,
	}
	s.fillConfigWithMsg(msg)

	if data, err := msg.ToJSON(); err != nil {
		panic(err)
	} else {
		err = s.conn.Send("msgs", string(data))
		if err != nil {
			panic(err)
		}
	}

	return fsm.EvtNext(evt, "Success")
}

func (s *Terminal) enterReceiving(_ fsm.CallbackKey, evt *fsm.Event) error {
	count, msgs, err := s.nextFromSocket()

	if err != nil {
		// 仅超时，可做失败重试
		if isTimeout(err) {
			return fsm.EvtNext(evt, "Fail")
		}
		panic(err)
	}

	s.countNext = count
	s.msgsRouting = msgs

	return fsm.EvtNext(evt, "Route")
}

func (s *Terminal) enterRouteing(_ fsm.CallbackKey, evt *fsm.Event) error {
	var canRoute bool

	for _, msg := range s.msgsRouting {
		switch msg.Action {
		case ActREQ, ActJOB:
			// update msg.from
			_, _, rid := msg.btrIDS()
			msg.updateFrom(s.bid, s.Srv.Name, rid)

			canRoute = s.routeReq(msg)
		case ActRES:
			canRoute = s.routeRes(msg)
		default:
			canRoute = false
		}

		if canRoute {
			if ok := s.MQOutOp.Push(msg, true); !ok {
				s.timesOutOpTimeout++
			}
		} else {
			s.timesRouteFail++
		}
	}
	return fsm.EvtNext(evt, "Push")
}

func (s *Terminal) enterPushing(_ fsm.CallbackKey, evt *fsm.Event) error {
	args := []interface{}{"msgs"}

	for i := 0; i < s.countNext; i++ {
		msg, ok := s.MQInOp.Pop(true)
		if !ok { // 超时
			break
		}
		// 若为更新配置，则填充相关配置信息
		if msg.Action == ActCFG {
			s.fillConfigWithMsg(msg)
		}

		if data, err := msg.ToJSON(); err == nil {
			args = append(args, string(data))
		}
	}

	if len(args) == 1 {
		args[0] = "empty"
	}

	// 发送失败，则直接停止
	if err := s.conn.Send(args...); err != nil {
		panic(err)
	}

	return fsm.EvtNext(evt, "Success")
}

func (s *Terminal) routeReq(msg *Msg) bool {
	if v, ok := s.mapOutRouter.Get(msg.Service); ok {
		if router, ok := v.(IServiceRouter); ok {
			router.Route(msg, []string{})
			return true
		}
	}
	return false
}

func (s *Terminal) routeRes(msg *Msg) bool {
	bid, _, _ := msg.btrIDS()
	if bid == "" {
		return false
	}
	msg.Channel = bid
	return true
}

func (s *Terminal) fillConfigWithMsg(msg *Msg) {
	if msg.Data == nil {
		msg.Data = make(map[string]interface{})
	}

	if msg.From == "" {
		msg.From = s.Srv.Name
	}

	config := make(map[string]interface{})

	for _, service := range s.services {
		if v, ok := s.mapConfig.Get(service); ok {
			config[service] = v
		}
	}

	msg.Data["config"] = config
}

func (s *Terminal) nextFromSocket() (count int, msgs []*Msg, err error) {
	var datas []string
	datas, err = s.conn.Recv()
	if err != nil {
		return
	}

	if len(datas) < 2 || datas[0] != "next" {
		err = ErrInvalidNextCmd
		return
	}

	count, err = strconv.Atoi(datas[1])
	if err != nil {
		return
	}

	if len(datas) > 2 {
		for _, data := range datas[2:] {
			msg, errDecode := MsgFromJSON([]byte(data))
			if errDecode != nil {
				s.Log(log.Fields{
					"error": errDecode,
					"json":  data,
				}).Info("terminal receive invalid msg json")
				continue
			}
			msgs = append(msgs, msg)
		}
	}
	return
}

func isTimeout(err error) bool {
	t, ok := err.(*net.OpError)
	return ok && t.Timeout()
}
