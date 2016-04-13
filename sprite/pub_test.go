package sprite

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chashu-code/micro-broker/adapter"
	"github.com/chashu-code/micro-broker/fsm"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type PubMockedRediser struct {
	mock.Mock
	buildFailTimes int
	cmdFailTimes   int
}

func (r *PubMockedRediser) LastCritical() error {
	return nil
}

func (r *PubMockedRediser) Cmd(cmd string, args ...interface{}) *redis.Resp {
	argsAll := []interface{}{cmd}
	if len(args) > 0 {
		argsAll = append(argsAll, args)
	}
	res := r.Called(argsAll...)
	return res.Get(0).(*redis.Resp)
}

func (r *PubMockedRediser) Close() error {
	r.Called()
	return nil
}

func (r *PubMockedRediser) Build(url string) (adapter.IRedis, error) {
	if r.buildFailTimes > 0 {
		r.buildFailTimes--
		return r, errors.New("build error")
	}
	return r, nil
}

// 循环检测2次，才成功
func Test_OverhaulTwice(t *testing.T) {
	mRedis := &PubMockedRediser{
		buildFailTimes: 1,
		cmdFailTimes:   1,
	}

	respErr := redis.NewRespSimple("PONG")

	mRedis.On(
		"Cmd",
		mock.MatchedBy(func(_ string) bool {
			if mRedis.cmdFailTimes > 0 {
				mRedis.cmdFailTimes--
				respErr.Err = errors.New("ping error")
			} else {
				respErr.Err = nil
			}
			return true
		}),
	).Return(respErr)

	signalOK := make(chan bool)
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			switch key.State {
			case "wait-msg":
				signalOK <- true
			}
			return nil
		},
	}

	msgQ := make(chan Msg)

	options := map[string]interface{}{
		"name":             "test",
		"redisBuilder":     (adapter.RediserBuilder)(mRedis.Build),
		"msgQueue":         msgQ,
		"callbackDesc":     cbDesc,
		"gatewayURI":       "OverhaulTwice",
		"intervalOverhaul": 1,    // Millisecond
		"intervalWaitMsg":  1000, // Millisecond
	}

	s := PubRun(options)
	<-signalOK

	expectedFlow := "initial/overhaul/overhaul-fail/overhaul/overhaul-fail/overhaul/wait-msg"
	stateFlow := strings.Join(states, "/")

	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
	assert.Equal(t, uint(3), s.Report()["timesOverhaul"].(uint))
}

// 停止，可自动关闭连接
func Test_AutoCloseWhenStop(t *testing.T) {
	mRedis := &PubMockedRediser{}

	resp := redis.NewRespSimple("PONG")

	mRedis.On("Cmd", "PING").Return(resp)
	mRedis.On("Close").Return()

	signalOK := make(chan bool)
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			if key.State == "wait-msg" {
				signalOK <- true
			}
			return nil
		},
		"stop": func(key fsm.CallbackKey, evt *fsm.Event) error {
			signalOK <- true
			return nil
		},
	}

	msgQ := make(chan Msg)

	options := map[string]interface{}{
		"name":             "test",
		"redisBuilder":     (adapter.RediserBuilder)(mRedis.Build),
		"msgQueue":         msgQ,
		"callbackDesc":     cbDesc,
		"gatewayURI":       "AutoCloseWhenStop",
		"intervalOverhaul": 1, // Millisecond
		"intervalWaitMsg":  1, // Millisecond
	}

	s := PubRun(options)

	// 等待进入 wait-msg
	<-signalOK
	s.Stop()

	// 等待 stop callback
	<-signalOK

	mRedis.AssertCalled(t, "Close")

	expectedFlow := "initial/overhaul/wait-msg"
	stateFlow := strings.Join(states, "/")

	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
}

// 等待消息超时
func Test_WaitMsgTimeout(t *testing.T) {
	mRedis := &PubMockedRediser{}

	resp := redis.NewRespSimple("PONG")

	mRedis.On("Cmd", "PING").Return(resp)
	mRedis.On("Close").Return()

	signalOK := make(chan bool)
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
		"leave-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			if key.State == "wait-msg-timeout" {
				signalOK <- true
			}
			return nil
		},
	}

	msgQ := make(chan Msg)

	options := map[string]interface{}{
		"name":             "test",
		"redisBuilder":     (adapter.RediserBuilder)(mRedis.Build),
		"msgQueue":         msgQ,
		"callbackDesc":     cbDesc,
		"gatewayURI":       "WaitMsgTimeout(",
		"intervalOverhaul": 1, // Millisecond
		"intervalWaitMsg":  1, // Millisecond
	}

	PubRun(options)

	// 等待 leave wait-msg-timeout 2 次
	<-signalOK
	<-signalOK

	time.Sleep(time.Duration(1) * time.Millisecond)

	expectedFlow := "initial/overhaul/wait-msg/wait-msg-timeout/wait-msg/wait-msg-timeout"
	stateFlow := strings.Join(states, "/")

	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
}

// 通过检测，并且发送收到的消息
func Test_SendMsgSuccess(t *testing.T) {
	msgs := []*Msg{}

	mRedis := &PubMockedRediser{}
	mRedis.On("Close").Return()
	mRedis.On("Cmd", "PING").Return(redis.NewRespSimple("PONG"))
	mRedis.On(
		"Cmd",
		"PUBLISH",
		mock.MatchedBy(func(args []interface{}) bool {
			if args[0].(string) == "test" {
				m, _ := MsgFromJSON(args[1].([]byte))
				msgs = append(msgs, m)
				return true
			}
			return false
		}),
	).Return(redis.NewRespSimple("OK"))

	msgQ := make(chan Msg)
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
	}

	options := map[string]interface{}{
		"name":             "test",
		"redisBuilder":     (adapter.RediserBuilder)(mRedis.Build),
		"msgQueue":         msgQ,
		"callbackDesc":     cbDesc,
		"gatewayURI":       "AutoCloseWhenStop",
		"intervalOverhaul": 1, // Millisecond
		"intervalWaitMsg":  1, // Millisecond
	}

	PubRun(options)

	var msg *Msg
	var err error

	msgJSON := `{
		"action": "req",
		"from": "1@2@3"
	}`

	msg, err = MsgFromJSON([]byte(msgJSON))
	msg.Channel = "test"
	assert.Nil(t, err)
	msgQ <- *msg

	msgJSON = `{
		"action": "res",
		"from": "1@2@3"
	}`

	msg, err = MsgFromJSON([]byte(msgJSON))
	msg.Channel = "test"
	assert.Nil(t, err)
	msgQ <- *msg

	msgQ <- *msg // 插入第三个，确保前两个msg被完全处理

	expectedFlow := "initial/overhaul/wait-msg/pub-msg/wait-msg/pub-msg/wait-msg"
	stateFlow := strings.Join(states, "/")

	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
	assert.Equal(t, "req", msgs[0].Action)
	assert.Equal(t, "res", msgs[1].Action)
}

// 发送消息失败（Redis），重新检测
func Test_OverhaulWhenSendError(t *testing.T) {
	mRedis := &PubMockedRediser{}
	mRedis.On("Close").Return()
	mRedis.On("Cmd", "PING").Return(redis.NewRespSimple("PONG"))

	respErr := redis.NewRespSimple("Fail")
	respErr.Err = errors.New("publish fail")

	mRedis.On(
		"Cmd",
		"PUBLISH",
		mock.MatchedBy(func(args []interface{}) bool {
			if args[0].(string) == "test" {
				return true
			}
			return false
		}),
	).Return(respErr)

	msgQ := make(chan Msg)
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
	}

	options := map[string]interface{}{
		"name":             "test",
		"redisBuilder":     (adapter.RediserBuilder)(mRedis.Build),
		"msgQueue":         msgQ,
		"callbackDesc":     cbDesc,
		"gatewayURI":       "AutoCloseWhenStop",
		"intervalOverhaul": 1, // Millisecond
		"intervalWaitMsg":  1, // Millisecond
	}

	PubRun(options)

	var msg *Msg
	var err error

	msgJSON := `{
		"action": "req",
		"from": "1@2@3"
	}`

	msg, err = MsgFromJSON([]byte(msgJSON))
	msg.Channel = "test"
	assert.Nil(t, err)
	msgQ <- *msg
	msgQ <- *msg

	expectedFlow := "initial/overhaul/wait-msg/pub-msg/overhaul-fail/overhaul/wait-msg"
	stateFlow := strings.Join(states, "/")
	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
}
