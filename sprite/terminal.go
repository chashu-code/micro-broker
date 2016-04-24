package sprite

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/chashu-code/micro-broker/adapter"
	"github.com/chashu-code/micro-broker/fsm"
	"github.com/chashu-code/micro-broker/log"
	"github.com/imdario/mergo"
	cmap "github.com/streamrail/concurrent-map"
)

// Terminal 终端处理精灵
type Terminal struct {
	Sprite

	mqInOp  *MsgQueueOp
	mqOutOp *MsgQueueOp

	msInQueueOpTimeout int
	msNetTimeout       int
	conn               *adapter.SSDBOper

	mapOutRouter cmap.ConcurrentMap
	mapQueueOp   cmap.ConcurrentMap
	mapConfig    cmap.ConcurrentMap

	services  []string
	countNext int
	cmds      []string
	bid       string
	rid       uint

	timesProcessTimeout uint
	timesOutOpTimeout   uint

	opExecer *MsgQueueOpExecer
}

// Report 汇报信息
func (s *Terminal) Report() map[string]interface{} {
	s.report["timesProcessTimeout"] = s.timesProcessTimeout
	s.report["timesOutOpTimeout"] = s.timesOutOpTimeout

	return s.report
}

// RID 返回当前 rid 的字符串
func (s *Terminal) RID() string {
	return strconv.Itoa(int(s.rid))
}

// RegServices 返回注册的服务列表
func (s *Terminal) RegServices() []string {
	return s.services
}

// TerminalRun 启动Terminal精灵
func TerminalRun(options map[string]interface{}) *Terminal {
	s := &Terminal{}

	s.opExecer = NewMsgQueueOpExecer()

	s.fillMsTimeoutOpts(options)
	s.addFlowDesc(options)
	s.addCallbackDesc(options)

	s.conn = adapter.NewSSDBOper(options["conn"].(net.Conn), s.msNetTimeout)
	s.mapOutRouter = options["mapOutRouter"].(cmap.ConcurrentMap)
	s.mapQueueOp = options["mapQueueOp"].(cmap.ConcurrentMap)
	s.mapConfig = options["mapConfig"].(cmap.ConcurrentMap)

	if v, ok := s.mapConfig.Get(KeyBrokerName); ok {
		s.bid = v.(string)
	} else {
		panic(errors.New("terminal need brokerName with config"))
	}

	if v, ok := s.mapQueueOp.Get(KeyPubQueue); ok {
		mq := v.(*MsgQueueOp)
		s.mqOutOp = mq
	} else {
		panic(errors.New("terminal need pub MsgQueueOp"))
	}

	mq := make(chan *Msg, MsgQueueSizeDefault)
	s.mqInOp = NewMsgQueueOp(mq, s.msInQueueOpTimeout)

	s.Run(options)
	return s
}

func (s *Terminal) fillMsTimeoutOpts(options map[string]interface{}) {
	opDefault := map[string]interface{}{
		"msInQueueOpTimeout": NetTimeoutDefault,
		"msNetTimeout":       NetTimeoutDefault,
	}
	mergo.Merge(&options, opDefault)

	s.msInQueueOpTimeout = options["msInQueueOpTimeout"].(int)
	s.msNetTimeout = options["msNetTimeout"].(int)
}

func (s *Terminal) addFlowDesc(options map[string]interface{}) {
	options["initState"] = SpriteInitState
	options["flowDesc"] = fsm.FlowDescList{
		{Evt: "Timeout", SrcList: []string{"receiving"}, Dst: "timeout"},
		{Evt: "Process", SrcList: []string{"receiving"}, Dst: "prccessing"},
		{Evt: "Success", SrcList: []string{SpriteInitState, "prccessing"}, Dst: "receiving"},
		{Evt: "Retry", SrcList: []string{"timeout"}, Dst: "receiving"},
	}
}

func (s *Terminal) addCallbackDesc(options map[string]interface{}) {
	desc := fsm.CallbackDesc{
		"enter-initial":    s.enterInitial,
		"enter-receiving":  s.enterReceiving,
		"enter-prccessing": s.enterProcessing,
		"enter-timeout":    s.enterTimeout,
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
		s.mapQueueOp.Remove(s.Srv.Name)

		s.Log(log.Fields{}).Info("terminal stop")

		if cbStop != nil {
			return cbStop(key, evt)
		}
		return nil
	}

	options["callbackDesc"] = desc
}

func (s *Terminal) enterTimeout(_ fsm.CallbackKey, evt *fsm.Event) error {
	return fsm.EvtNext(evt, "Retry")
}

func (s *Terminal) enterInitial(_ fsm.CallbackKey, evt *fsm.Event) error {
	s.Log(log.Fields{}).Info("terminal start")

	// 创建监听应答的 msg queue op
	s.mapQueueOp.Set(s.Srv.Name, s.mqInOp)

	return fsm.EvtNext(evt, "Success")
}

func (s *Terminal) enterReceiving(_ fsm.CallbackKey, evt *fsm.Event) error {
	cmds, err := s.conn.Recv()

	if err != nil {
		// 仅超时，可做失败重试
		if isTimeout(err) {
			return fsm.EvtNext(evt, "Timeout")
		}
		panic(err)
	}

	s.cmds = cmds

	return fsm.EvtNext(evt, "Process")
}

func (s *Terminal) enterProcessing(_ fsm.CallbackKey, evt *fsm.Event) error {
	var cmdsRes []interface{}

	cmds := s.cmds

	// 收到的指令，必须有2个元素以上
	if len(cmds) > 1 {
		switch cmds[0] {
		case ActREG: // 注册监听
			cmdsRes = s.processReg(cmds[1:])
		case ActSub: // 监听服务
			cmdsRes = s.processSub(cmds[1:])
		case ActREQ, ActJOB: // 发送请求
			cmdsRes = s.processReq(cmds[0], cmds[1:])
		case ActRES: // 发送应答
			cmdsRes = s.processRes(cmds[1:])
		case ActSYNC: // 处理同步
			cmdsRes = s.processSync(cmds[1:])
		default:
			cmdsRes = []interface{}{"err", fmt.Sprintf("wrong cmd %s", cmds[0])}
		}
	} else {
		cmdsRes = []interface{}{"err", fmt.Sprintf("wrong cmds: %s", cmds)}
	}

	// if "empty" != cmdsRes[0] {
	// 	s.Log(log.Fields{
	// 		"cmds": cmds,
	// 		"res":  cmdsRes,
	// 	}).Info("terminal receiving cmds")
	// }

	if err := s.conn.Send(cmdsRes...); err != nil {
		panic(err)
	}

	return fsm.EvtNext(evt, "Success")
}

func (s *Terminal) processReq(cmd string, args []string) []interface{} {
	var res []interface{}

	if len(args) < 1 {
		return append(res, "err", fmt.Sprintf("cmd %s need args: msg", cmd))
	}

	msg, err := MsgFromJSON([]byte(args[0]))
	if err != nil {
		return append(res, "err", fmt.Sprintf("%s msg decode error: %s", cmd, err))
	}

	if v, ok := s.mapOutRouter.Get(msg.Service); ok {
		// 获取服务路由
		var router IServiceRouter
		if router, ok = v.(IServiceRouter); ok {
			// 获取在线broker列表，并设置msg.Channel为适合的broker
			var brokersOnline []string
			if v, ok = s.mapConfig.Get(KeyBrokersOnline); ok {
				brokersOnline, _ = v.([]string)
			}
			router.Route(msg, brokersOnline)

			// 设置适合的 msg.From
			s.rid++
			msg.updateFrom(s.bid, s.Srv.Name, s.RID())

			// 推送消息
			if !s.opExecer.Push(s.mqOutOp, msg, true) {
				// 推送队列超时，队列已满
				return append(res, "err", "pub queue is full")
			}

			// 监听应答
			for {
				msg, ok = s.opExecer.Pop(s.mqInOp, true)
				if !ok {
					var keys []string
					for item := range s.mapQueueOp.IterBuffered() {
						keys = append(keys, item.Key)
					}
					s.Log(log.Fields{
						"mapQueueOp": keys,
					}).Info("timeout??")
					return append(res, "err", "wait res timeout")
				}

				_, _, rid := msg.btrIDS()
				if rid != s.RID() {
					continue
				}

				data, err := msg.ToJSON()
				if err != nil {
					return append(res, "err", fmt.Sprintf("res msg json encode error: %s", err))
				}

				return append(res, "ok", data)
			}
		}
	}

	return append(res, "err", fmt.Sprintf("unfound service router of %s", msg.Service))

}

func (s *Terminal) processRes(args []string) []interface{} {
	var res []interface{}

	if len(args) < 1 {
		return append(res, "err", "cmd res need args: msg")
	}

	msg, err := MsgFromJSON([]byte(args[0]))
	if err != nil {
		return append(res, "err", fmt.Sprintf("res msg decode error: %s", err))
	}

	bid, _, _ := msg.btrIDS()
	if bid == "" {
		return append(res, "err", "res msg's From need set bid")
	}
	msg.Channel = bid

	// 推送消息
	if !s.opExecer.Push(s.mqOutOp, msg, true) {
		// 推送队列超时，队列已满
		return append(res, "err", "pub queue is full")
	}

	return append(res, "ok")
}

func (s *Terminal) processReg(args []string) []interface{} {
	var res []interface{}

	if len(args) < 1 {
		return append(res, "err", "cmd reg need args: services_str")
	}

	subToken := fmt.Sprintf("%x", md5.Sum([]byte(args[0])))
	if v, ok := s.mapConfig.Get(subToken); ok {
		if _, ok = v.(*MultiMsgQueueOpPoper); ok {
			return append(res, "ok", subToken)
		}
	}

	services := strings.Split(args[0], ",")
	poper := NewMultiMsgQueueOpPoper(s.mapQueueOp, services)

	s.mapConfig.Set(subToken, poper)

	return append(res, "ok", subToken)
}

func (s *Terminal) processSub(args []string) []interface{} {
	var res []interface{}

	if len(args) < 1 {
		return append(res, "err", "cmd sub need args: sub_token")
	}

	subToken := args[0]

	var poper *MultiMsgQueueOpPoper

	if v, ok := s.mapConfig.Get(subToken); ok {
		poper, _ = v.(*MultiMsgQueueOpPoper)
	}

	if poper == nil {
		return append(res, "err", "unregistered sub_token")
	}

	msg, ok := poper.Pop()
	if !ok {
		return append(res, "empty")
	}

	data, err := msg.ToJSON()
	if err != nil {
		return append(res, "err", fmt.Sprintf("req msg json encode error: %s", err))
	}

	return append(res, "ok", data)
}

func (s *Terminal) processSync(args []string) []interface{} {
	var res []interface{}

	if len(args) < 2 {
		return append(res, "err", "cmd sync need args: name, version")
	}

	name, ver := args[0], args[1]
	if v, ok := s.mapConfig.Get(KeyVerConf); ok {
		var verConf string
		if verConf, ok = v.(string); ok {
			if verConf != ver { // 版本有差异
				if v, ok = s.mapConfig.Get(name); ok {
					data, err := json.Marshal(v)
					if err != nil {
						return append(res, "err", fmt.Sprintf("config encode error: %s", err))
					}
					return append(res, "ok", verConf, data)
				}
			}
		}
	}
	return append(res, "newest")
}

func isTimeout(err error) bool {
	t, ok := err.(*net.OpError)
	return ok && t.Timeout()
}
