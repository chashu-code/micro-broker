package sprite

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/chashu-code/micro-broker/adapter"
	"github.com/chashu-code/micro-broker/fsm"
	"github.com/chashu-code/micro-broker/log"
	cmap "github.com/streamrail/concurrent-map"
)

const (
	// KeyBrokerName  配置中，当前borker name
	KeyBrokerName = "#NAME"
	// KeyBrokersOnline 配置中，在线brokers
	KeyBrokersOnline = "#ON-BRKS"

	// KeyGateWaysOnline 配置中，在线的gateway
	KeyGateWaysOnline = "#ON-GWS"

	// KeyJobQueue 任务队列名
	KeyJobQueue = "#QUEUE-JOB"

	// KeyBrokerQueue 代理消息队列名
	KeyBrokerQueue = "#QUEUE-BRK"

	// KeyPubQueue 推送消息队列名
	KeyPubQueue = "#QUEUE-PUB"

	// KeyVerConf 配置版本名
	KeyVerConf = "#VER-CONF"

	// KeyVerGateWaysOnline 在线gateway版本
	KeyVerGateWaysOnline = "#VER-ON-GWS"

	// KeyVerBrokersOnline 在线broker版本
	KeyVerBrokersOnline = "#VER-ON-BRKS"

	// KeyVerServiceRoute 服务路由版本
	KeyVerServiceRoute = "#VER-SROUTE"

	// KeyServiceRoute 服务路由
	KeyServiceRoute = "#SROUTE"

	// AddrListenDefault 默认监听地址
	AddrListenDefault = "127.0.0.1:6636"
	// SpriteLBSizeDefault 精灵负载最大数量
	SpriteLBSizeDefault = 2
	// MsgQueueSizeDefault 消息队列最大缓冲
	MsgQueueSizeDefault = 30
	// MsgQueueOpTimeoutDefault 消息队列操作超时毫秒
	MsgQueueOpTimeoutDefault = 100
	// NetTimeoutDefault 网络操作超时毫秒
	NetTimeoutDefault = 1000
)

// Broker 代理精灵
type Broker struct {
	Sprite

	MapOutRouter cmap.ConcurrentMap
	MapConfig    cmap.ConcurrentMap
	MapSprite    cmap.ConcurrentMap
	MapQueueOp   cmap.ConcurrentMap

	MQPubOp    *MsgQueueOp
	MQJobOp    *MsgQueueOp
	MQBrokerOp *MsgQueueOp

	opExecer *MsgQueueOpExecer

	spriteID       uint
	terminalServer *TerminalServer

	mapPub cmap.ConcurrentMap
	mapSub cmap.ConcurrentMap
	mapJob cmap.ConcurrentMap
}

// BrokerRun 构建并启动Broker
func BrokerRun(options map[string]interface{}) *Broker {
	s := &Broker{
		MapOutRouter: cmap.New(),
		MapConfig:    cmap.New(),
		MapQueueOp:   cmap.New(),

		mapPub: cmap.New(),
		mapSub: cmap.New(),
		mapJob: cmap.New(),

		MQPubOp:    NewMsgQueueOp(make(chan *Msg, MsgQueueSizeDefault*SpriteLBSizeDefault), MsgQueueOpTimeoutDefault),
		MQJobOp:    NewMsgQueueOp(make(chan *Msg, MsgQueueSizeDefault), MsgQueueOpTimeoutDefault),
		MQBrokerOp: NewMsgQueueOp(make(chan *Msg, MsgQueueSizeDefault), MsgQueueOpTimeoutDefault),
	}

	s.opExecer = NewMsgQueueOpExecer()
	s.terminalServer = &TerminalServer{}

	// 如有设置，则更新配置
	if v, ok := options["gatewayURI"]; ok {
		if uri, _ := v.(string); uri != "" {
			s.MapConfig.Set(KeyGateWaysOnline, uri)
		}
	}

	s.addFlowDesc(options)
	s.addCallbackDesc(options)
	s.updateWithConfFile(options)
	s.Run(options)
	return s
}

func (s *Broker) addFlowDesc(options map[string]interface{}) {
	options["initState"] = SpriteInitState
	options["flowDesc"] = fsm.FlowDescList{
		{Evt: "Success", SrcList: []string{SpriteInitState, "reporting"}, Dst: "processing"},
		{Evt: "Report", SrcList: []string{"processing"}, Dst: "reporting"},
	}
}

func (s *Broker) addCallbackDesc(options map[string]interface{}) {
	desc := fsm.CallbackDesc{
		"enter-initial":    s.enterInitial,
		"enter-reporting":  s.enterReporting,
		"enter-processing": s.enterProcessing,
	}

	if opDesc, ok := options["callbackDesc"]; ok {
		for k, v := range opDesc.(fsm.CallbackDesc) {
			desc[k] = v
		}
	}

	options["callbackDesc"] = desc
}

func (s *Broker) updateWithConfFile(options map[string]interface{}) {
	if v, ok := options["pathConf"]; ok {
		var path string
		if path, ok = v.(string); ok && path != "" {
			data, err := ioutil.ReadFile(path)
			if err != nil {
				panic(fmt.Errorf("read config fail: %v", err))
			}

			conf := make(map[string]interface{})
			if err = json.Unmarshal(data, &conf); err != nil {
				panic(fmt.Errorf("config decode fail: %v", err))
			}

			s.updateWithSyncInfo(conf)
		}
	}

}

func (s *Broker) updateWithSyncInfo(info map[string]interface{}) {
	var ok bool
	var vStr string
	var vMap map[string]interface{}
	var routeMap map[string]interface{}

	for k, v := range info {
		fmt.Printf("update %s-%v\n", k, v)
		switch k {
		case KeyBrokersOnline, KeyGateWaysOnline:
			if vStr, ok = v.(string); ok {
				s.MapConfig.Set(k, PickBrokersFromStr(vStr))
			}
		case KeyServiceRoute:
			if vMap, ok = v.(map[string]interface{}); ok {
				for service, rv := range vMap {
					if routeMap, ok = rv.(map[string]interface{}); !ok {
						continue
					}
					if router, err := PickRouterFromMap(routeMap); err != nil {
						s.Log(log.Fields{
							"service": service,
							"error":   err,
						}).Error("PickRouterFromMap fail")
					} else {
						s.MapOutRouter.Set(service, router)
					}
				}
			}
		default:
			s.MapConfig.Set(k, v)
		}
	}

	data, err := s.MapConfig.MarshalJSON()
	fmt.Printf("config: %v\n%s\n", err, data)
}

func (s *Broker) enterInitial(_ fsm.CallbackKey, evt *fsm.Event) error {
	s.MapConfig.Set(KeyBrokerName, s.Srv.Name)
	s.MapQueueOp.Set(KeyPubQueue, s.MQPubOp)
	s.MapQueueOp.Set(KeyJobQueue, s.MQJobOp)
	s.MapQueueOp.Set(KeyBrokerQueue, s.MQBrokerOp)

	go s.terminalServer.Listen(s, AddrListenDefault)

	s.startSprites()

	return fsm.EvtNext(evt, "Success")
}

func (s *Broker) enterProcessing(_ fsm.CallbackKey, evt *fsm.Event) error {
	for {
		msg, ok := s.opExecer.Pop(s.MQBrokerOp, true)
		if !ok {
			break
		}

		switch msg.Action {
		case ActSYNC:
			s.updateConfig(msg)
		case ActStart:
			switch msg.Service {
			case "pub":
				s.startPubSubSprite("pub", s.mapPub)
			case "sub":
				s.startPubSubSprite("sub", s.mapSub)
			case "job":
				s.startJobSprite()
			}
		}
	}
	return fsm.EvtNext(evt, "Report")
}

func (s *Broker) enterReporting(_ fsm.CallbackKey, evt *fsm.Event) error {
	// s.Log(log.Fields{
	// 	"report": s.Report(),
	// }).Info("broker report!")

	return fsm.EvtNext(evt, "Success")
}

func (s *Broker) updateConfig(msg *Msg) {
	s.updateWithSyncInfo(msg.Data)
}

func (s *Broker) startSprites() {
	s.startJobSprite()
	s.startPubSubSprite("pub", s.mapPub)
	s.startPubSubSprite("sub", s.mapSub)
}

func (s *Broker) startPubSubSprite(which string, mapSprite cmap.ConcurrentMap) {
	if v, ok := s.MapConfig.Get(KeyGateWaysOnline); ok {
		if urlsOn, ok := v.([]string); ok {
			// 启动未启动的
			for _, url := range urlsOn {
				spriteName := url
				// 若服务还存活，则跳过
				if mapSprite.Has(url) {
					continue
				}

				cbDesc := fsm.CallbackDesc{
					"stop": func(_ fsm.CallbackKey, evt *fsm.Event) error {
						mapSprite.Remove(evt.Service)

						msg := &Msg{
							Action:  ActStart,
							Service: which,
						}

						times := 5
						for {
							if s.opExecer.Push(s.MQBrokerOp, msg, true) {
								break
							}
							times--
							if times < 0 {
								panic(errors.New("sprite stop push MQBrokerOp timeout"))
							}
						}
						return nil
					},
				}

				// 启动 SubSprite
				options := map[string]interface{}{
					"name":         spriteName,
					"redisBuilder": (adapter.RediserBuilder)(adapter.BuildRedister),
					"callbackDesc": cbDesc,
					"gatewayURI":   url,

					"msgQueue": s.MQPubOp.C, // pub

					"mapQueueOp": s.MapQueueOp,         // sub
					"channels":   []string{s.Srv.Name}, // sub
				}

				if which == "sub" {
					mapSprite.Set(spriteName, SubRun(options))
				} else {
					mapSprite.Set(spriteName, PubRun(options))
				}
			}

			// 停止不在线的
			var spriteStop ISprite
			for item := range mapSprite.IterBuffered() {
				isExist := false
				urlRun := item.Key

				for _, url := range urlsOn {
					if url == urlRun {
						isExist = true
						break
					}
				}

				if !isExist {
					if spriteStop, ok = item.Val.(ISprite); ok {
						spriteStop.Stop()
					}
				}
			}
		}
	}
}

func (s *Broker) startJobSprite() {

}
