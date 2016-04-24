package sprite

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/chashu-code/micro-broker/adapter"
	"github.com/chashu-code/micro-broker/fsm"
	"github.com/mediocregopher/radix.v2/pubsub"
	"github.com/mediocregopher/radix.v2/redis"
	cmap "github.com/streamrail/concurrent-map"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockTimeoutErr struct {
	error
}

func (err *MockTimeoutErr) Timeout() bool {
	return true
}

type SubMockedRediser struct {
	mock.Mock

	gatewayURI string

	buildFailTimes int
	subFailTimes   int
	msgSN          int
}

func (r *SubMockedRediser) LastCritical() error {
	return nil
}

func (r *SubMockedRediser) Cmd(cmd string, args ...interface{}) *redis.Resp {
	argsAll := []interface{}{cmd}
	if len(args) > 0 {
		argsAll = append(argsAll, args)
	}
	res := r.Called(argsAll...)
	return res.Get(0).(*redis.Resp)
}

func (r *SubMockedRediser) Close() error {
	r.Called()
	return nil
}

func (r *SubMockedRediser) Subscribe(channels ...interface{}) *pubsub.SubResp {

	sr := &pubsub.SubResp{}

	if r.subFailTimes > 0 {
		r.subFailTimes--
		sr.Err = errors.New("sub fail")
	}
	return sr
}

func (r *SubMockedRediser) Receive() (string, error) {
	var data string
	var err error

	switch r.gatewayURI {
	case "ReceiveSuccess", "PutMsgTimeout":
		str := `{
      "action": "req",
      "from": "%v",
			"service": "hello"
    }`
		r.msgSN++
		data = fmt.Sprintf(str, r.msgSN)
	case "ReceiveDecodeFail":
		str := `{
      "action": "req",
      "from": "%v",
			"service": "hello"
    }`

		r.msgSN++
		if r.msgSN > 1 {
			data = fmt.Sprintf(str, r.msgSN)
		} else {
			data = "error format json"
		}

	default:
		// IO Timeout
		err = adapter.ErrReceiveTimeout
	}

	return data, err
}

func (r *SubMockedRediser) Build(url string, _ int) (adapter.IRedis, error) {
	r.gatewayURI = url

	if r.buildFailTimes > 0 {
		r.buildFailTimes--
		return r, errors.New("build error")
	}
	return r, nil
}

// 顺利从redis获取消息，但解码有误
func Test_SubRecvDecodeFail(t *testing.T) {
	mRedis := &SubMockedRediser{}

	mRedis.On("Close").Return()
	signalOK := make(chan bool)
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			switch key.State {
			case "put-msg":
				signalOK <- true
			}
			return nil
		},
	}

	msgQ := NewMsgQueueOp(make(chan *Msg), 1)
	mapQueueOp := cmap.New()
	mapQueueOp.Set("hello", msgQ)

	options := map[string]interface{}{
		"name":               "test",
		"redisBuilder":       (adapter.RediserBuilder)(mRedis.Build),
		"mapQueueOp":         mapQueueOp,
		"callbackDesc":       cbDesc,
		"gatewayURI":         "ReceiveDecodeFail",
		"msOverhaulInterval": 1, // Millisecond
		"channels":           []string{"test"},
	}

	SubRun(options)

	<-signalOK
	<-signalOK
	msg := <-msgQ.C
	assert.Equal(t, "2", msg.From)

	expectedFlow := "initial/overhaul/wait-msg/put-msg/wait-msg/put-msg"
	stateFlow := strings.Join(states, "/")

	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
}

// 顺利从redis获取消息，并及时推送至mapQueueOp
func Test_SubRecvSuccess(t *testing.T) {
	mRedis := &SubMockedRediser{}

	mRedis.On("Close").Return()
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
	}

	msgQ := NewMsgQueueOp(make(chan *Msg), 1)
	mapQueueOp := cmap.New()
	mapQueueOp.Set("hello", msgQ)

	options := map[string]interface{}{
		"name":               "test",
		"redisBuilder":       (adapter.RediserBuilder)(mRedis.Build),
		"mapQueueOp":         mapQueueOp,
		"callbackDesc":       cbDesc,
		"gatewayURI":         "ReceiveSuccess",
		"msOverhaulInterval": 1, // Millisecond
		"channels":           []string{"test"},
	}

	SubRun(options)

	msg := <-msgQ.C
	assert.Equal(t, "1", msg.From)
	msg = <-msgQ.C
	assert.Equal(t, "2", msg.From)

	expectedFlow := "initial/overhaul/wait-msg/put-msg/wait-msg/put-msg"
	stateFlow := strings.Join(states, "/")

	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
}

// 顺利从redis获取消息，推送至mapQueueOp超时
func Test_SubPutMsgTimeout(t *testing.T) {
	mRedis := &SubMockedRediser{}

	mRedis.On("Close").Return()
	signalOK := make(chan bool)
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			switch key.State {
			case "put-msg":
				signalOK <- true
			}
			return nil
		},
	}

	msgQ := NewMsgQueueOp(make(chan *Msg), 1)
	mapQueueOp := cmap.New()
	mapQueueOp.Set("hello", msgQ)

	options := map[string]interface{}{
		"name":               "test",
		"redisBuilder":       (adapter.RediserBuilder)(mRedis.Build),
		"mapQueueOp":         mapQueueOp,
		"callbackDesc":       cbDesc,
		"gatewayURI":         "PutMsgTimeout",
		"msOverhaulInterval": 1, // Millisecond
		"channels":           []string{"test"},
	}

	s := SubRun(options)

	<-signalOK
	<-signalOK
	<-signalOK

	assert.True(t, 1 < int(s.Report()["timesQueueOpTimeout"].(uint)), "timesQueueOpTimeout > 1")
	expectedFlow := "initial/overhaul/wait-msg/put-msg/wait-msg/put-msg"
	stateFlow := strings.Join(states, "/")
	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
}

// 从redis获取消息超时
func Test_SubRecvTimeout(t *testing.T) {
	mRedis := &SubMockedRediser{}

	mRedis.On("Close").Return()

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

	msgQ := NewMsgQueueOp(make(chan *Msg), 1)
	mapQueueOp := cmap.New()
	mapQueueOp.Set("hello", msgQ)

	options := map[string]interface{}{
		"name":               "test",
		"redisBuilder":       (adapter.RediserBuilder)(mRedis.Build),
		"mapQueueOp":         mapQueueOp,
		"callbackDesc":       cbDesc,
		"gatewayURI":         "ReceiveTimeout",
		"msOverhaulInterval": 1, // Millisecond
		"msQueueOpTimeout":   1, // Millisecond
		"channels":           []string{"test"},
	}

	SubRun(options)
	<-signalOK
	<-signalOK

	expectedFlow := "initial/overhaul/wait-msg/wait-msg-timeout/wait-msg"
	stateFlow := strings.Join(states, "/")

	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
}

// 订阅redis出问题
func Test_SubFail(t *testing.T) {
	mRedis := &SubMockedRediser{
		buildFailTimes: 1,
		subFailTimes:   1,
	}

	mRedis.On("Close").Return()

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

	msgQ := NewMsgQueueOp(make(chan *Msg), 1)
	mapQueueOp := cmap.New()
	mapQueueOp.Set("hello", msgQ)

	options := map[string]interface{}{
		"name":               "test",
		"redisBuilder":       (adapter.RediserBuilder)(mRedis.Build),
		"mapQueueOp":         mapQueueOp,
		"callbackDesc":       cbDesc,
		"gatewayURI":         "OverhaulTwice",
		"msOverhaulInterval": 1, // Millisecond
		"channels":           []string{"test"},
	}

	s := SubRun(options)
	<-signalOK

	expectedFlow := "initial/overhaul/overhaul-fail/overhaul/overhaul-fail/overhaul/wait-msg"
	stateFlow := strings.Join(states, "/")

	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
	assert.Equal(t, uint(3), s.Report()["timesOverhaul"].(uint))
}
