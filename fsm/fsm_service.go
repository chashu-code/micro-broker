// Package fsm 实现有限状态机的服务
package fsm

import "strings"
import "errors"
import "github.com/chashu-code/micro-broker/log"
import "github.com/chashu-code/micro-broker/queue"
import "runtime/debug"

const (
	// LeaveState 离开状态回调类型
	LeaveState CallbackType = iota
	// EnterState 进入状态回调类型
	EnterState
	// StopFSM 停止状态机回调类型
	StopFSM
)

const (
	// EvtNameStartFSM 启动状态机事件名
	EvtNameStartFSM = "START-FSM"
	// EvtNameStopFSM 停止状态机事件名
	EvtNameStopFSM = "STOP-FSM"
)

var (
	// ErrStopWithEvt 当收到Event.Name = EvtNameStopFSM 的事件，抛出该错误
	ErrStopWithEvt = errors.New("stop fsm with stop event")
)

var (
	stopCallbackKey = CallbackKey{
		Type: StopFSM,
	}
)

type flowKey struct {
	Src     string
	EvtName string
}

// CallbackType 回调类型
type CallbackType uint8

// CallbackKey 回调映射表 Key结构
type CallbackKey struct {
	State string
	Type  CallbackType
}

// CallbackFunc 回调方法类型
type CallbackFunc func(CallbackKey, *Event) error

// Event 流转事件结构
type Event struct {
	// Name 名称
	Name string
	// Args 参数（可选）
	Args []interface{}
	// Service 流转事件的服务名称（自动绑定）
	Service string
	// Next 回调可通过设置该属性，指定状态机接下来流转的事件
	Next *Event
}

// EvtNext 设置 evt.Next， 并返回 nil
func EvtNext(evt *Event, nameNext string) error {
	evt.Next = &Event{Name: nameNext}
	return nil
}

// Service 有限状态机服务结构
type Service struct {
	log.Able
	// Name 状态机名称
	Name            string
	state           string
	evtQueue        *queue.Queue
	flowMap         map[flowKey]string
	flowCallbackMap map[CallbackKey]CallbackFunc
}

// FlowDesc 流转描述，用于初始化DSL
type FlowDesc struct {
	// Evt 事件名称
	Evt string
	// SrcList 可流转 Evt 的源状态列表
	SrcList []string
	// Dst 流转目标状态
	Dst string
}

// FlowDescList FlowDesc 列表
type FlowDescList []FlowDesc

// CallbackDesc 回调描述，用于初始化DSL
type CallbackDesc map[string]CallbackFunc

// Current 返回当前状态
func (s *Service) Current() string {
	return s.state
}

// Flow 流转事件，在处理完上一个事件前，会阻塞
func (s *Service) Flow(evt Event) {
	s.evtQueue.Push(evt)
}

// Serve 开启服务
func (s *Service) Serve() {
	defer func() {
		if err := recover(); err != nil {
			s.Log(log.Fields{
				"state": s.state,
				"error": err,
				"stack": (string)(debug.Stack()),
			}).Error("stop serve")

			evt := Event{
				Name: EvtNameStopFSM,
				Args: []interface{}{err},
			}
			s.invokeStopCallback(&evt)
		}
	}()

	// 调用 初始状态 enter callback
	var evt = Event{
		Name: EvtNameStartFSM,
	}
	err := s.invokeCallback(EnterState, s.state, &evt)

	s.Log(log.Fields{
		"state": s.state,
		"error": err,
	}).Info("start serve")

	for {
		if evt.Next != nil {
			s.Flow(*evt.Next)
		}

		evt = s.evtQueue.Pop().(Event)

		if evt.Name == EvtNameStopFSM {
			panic(ErrStopWithEvt)
		}

		dst, ok := s.hasDst(&evt)

		if !ok {
			s.Log(log.Fields{
				"event": evt.Name,
				"state": s.state,
			}).Info("flow error")
			continue
		}

		err := s.invokeCallbacks(&evt, dst)

		if err == nil {
			s.state = dst
		} else {
			s.Log(log.Fields{
				"event": evt.Name,
				"state": s.state,
				"error": err,
			}).Info("callback error")
		}
	}
}

// New 构造一个有限状态机服务
func New(options map[string]interface{}) *Service {
	name := options["name"].(string)
	initState := options["initState"].(string)
	flowDescList := options["flowDesc"].(FlowDescList)
	cbDesc := options["callbackDesc"].(CallbackDesc)

	s := &Service{
		Name:            name,
		state:           initState,
		flowCallbackMap: make(map[CallbackKey]CallbackFunc),
		flowMap:         make(map[flowKey]string),
		evtQueue:        queue.New(),
	}

	for _, desc := range flowDescList {
		for _, src := range desc.SrcList {
			key := flowKey{
				Src:     src,
				EvtName: desc.Evt,
			}

			s.flowMap[key] = desc.Dst
		}
	}

	for on, cb := range cbDesc {
		key, ok := onToCbKey(on)
		if ok && cb != nil {
			s.flowCallbackMap[key] = cb
		}
	}

	s.FixFields = log.Fields{
		"service": name,
	}

	return s
}

func onToCbKey(on string) (CallbackKey, bool) {
	var key CallbackKey

	if on == "stop" {
		return stopCallbackKey, true
	}

	result := strings.SplitN(on, "-", 2)

	if len(result) != 2 {
		return key, false
	}

	switch result[0] {
	case "enter":
		key = CallbackKey{
			Type:  EnterState,
			State: result[1],
		}
	case "leave":
		key = CallbackKey{
			Type:  LeaveState,
			State: result[1],
		}
	default:
		return key, false
	}

	return key, true
}

func (s *Service) invokeCallbacks(evt *Event, dst string) error {
	// 调用 当前状态的 leave callback
	err := s.invokeCallback(LeaveState, s.state, evt)
	if err != nil {
		return err
	}

	// 调用 dst 的 enter callback
	err = s.invokeCallback(EnterState, dst, evt)
	if err != nil {
		return err
	}

	return nil

}

func (s *Service) invokeCallback(typeCallback CallbackType, state string, evt *Event) error {
	keyAll := CallbackKey{
		State: "*",
		Type:  typeCallback,
	}

	key := CallbackKey{
		State: state,
		Type:  typeCallback,
	}

	evt.Service = s.Name

	for _, keyCb := range []CallbackKey{keyAll, key} {
		if fn, ok := s.flowCallbackMap[keyCb]; ok {
			if err := fn(key, evt); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Service) invokeStopCallback(evt *Event) {
	if fn, ok := s.flowCallbackMap[stopCallbackKey]; ok {
		evt.Service = s.Name
		fn(stopCallbackKey, evt)
	}
}

func (s *Service) hasDst(evt *Event) (string, bool) {
	key := flowKey{
		Src:     s.state,
		EvtName: evt.Name,
	}

	if dst, ok := s.flowMap[key]; ok {
		return dst, true
	}

	return "", false
}
