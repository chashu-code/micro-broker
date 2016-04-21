package sprite

import (
	"strings"
	"testing"
	"time"

	cmap "github.com/streamrail/concurrent-map"

	"github.com/chashu-code/micro-broker/adapter"
	"github.com/chashu-code/micro-broker/fsm"
	mconn "github.com/jordwest/mock-conn"
	"github.com/stretchr/testify/assert"
)

// 初始化部分 ===========
// 注册失败(错误的信息推送)，则自动关闭
func Test_TerminalRegFail(t *testing.T) {
	states := []string{}

	hasStop := false

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
		"stop": func(_ fsm.CallbackKey, evt *fsm.Event) error {
			hasStop = true
			return nil
		},
	}

	msgQ := make(chan *Msg)

	mc := mconn.NewConn()
	client := adapter.NewSSDBOper(mc.Client, 1)

	options := map[string]interface{}{
		"name":             "1",
		"msgOutQueue":      msgQ,
		"callbackDesc":     cbDesc,
		"msQueueOpTimeout": 1, // Millisecond
		"msNetTimeout":     1,
		"conn":             mc.Server,
		"mapInRouter":      cmap.New(),
		"mapOutRouter":     cmap.New(),
		"mapConfig":        cmap.New(),
	}

	TerminalRun(options)

	msgStr := `{
      "action": "res",
      "service": "hi",
      "from": "1"
  }`

	client.Send("next", "1", msgStr)
	time.Sleep(time.Millisecond * time.Duration(1))
	expectedFlow := "initial/registering"
	stateFlow := strings.Join(states, "/")
	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
	assert.True(t, hasStop, "terminal must be stop")
}

// 注册成功，推送配置信息
func Test_TerminalRegedSuccess(t *testing.T) {
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
	}

	msgQ := make(chan *Msg)

	mc := mconn.NewConn()
	client := adapter.NewSSDBOper(mc.Client, 1)

	mapInRouter := cmap.New()

	options := map[string]interface{}{
		"name":             "1",
		"msgOutQueue":      msgQ,
		"callbackDesc":     cbDesc,
		"msQueueOpTimeout": 1, // Millisecond
		"msNetTimeout":     1,
		"conn":             mc.Server,
		"mapInRouter":      mapInRouter,
		"mapOutRouter":     cmap.New(),
		"mapConfig":        cmap.New(),
	}

	TerminalRun(options)

	msgStr := `{
	    "action": "reg",
	    "service": "s1,s2",
	    "from": "1"
	}`

	client.Send("next", "1", msgStr)

	args, _ := client.Recv()

	assert.Equal(t, "msgs", args[0])

	msg, _ := MsgFromJSON([]byte(args[1]))

	assert.Equal(t, ActCFG, msg.Action)
	assert.Contains(t, msg.Data, "config")

	time.Sleep(time.Duration(1) * time.Millisecond)

	expectedFlow := "initial/registering/receiving"
	stateFlow := strings.Join(states, "/")
	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)

	// 检测注册服务是否成功
	_, ok := mapInRouter.Get("s1")
	assert.True(t, ok, "terminal has reg s1 service")

	_, ok = mapInRouter.Get("s2")
	assert.True(t, ok, "terminal has reg s2 service")
}

// 推送 ================
// 推送超时

// 推送配置
func Test_TerminalPushConfig(t *testing.T) {
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
	}

	msgQ := make(chan *Msg, 1)

	mc := mconn.NewConn()
	client := adapter.NewSSDBOper(mc.Client, 1)

	options := map[string]interface{}{
		"name":             "t1",
		"msgOutQueue":      msgQ,
		"callbackDesc":     cbDesc,
		"msQueueOpTimeout": 1, // Millisecond
		"msNetTimeout":     1,
		"conn":             mc.Server,
		"mapInRouter":      cmap.New(),
		"mapOutRouter":     cmap.New(),
		"mapConfig":        cmap.New(),
	}

	s := TerminalRun(options)

	msgConfg := &Msg{
		Action: ActCFG,
		From:   "center",
	}

	s.MQInOp.Push(msgConfg, true)

	msgStr := `{
	    "action": "reg",
	    "service": "s1,s2",
	    "from": "1"
	}`

	client.Send("next", "1", msgStr)

	args, _ := client.Recv()

	assert.Equal(t, "msgs", args[0])

	msg, _ := MsgFromJSON([]byte(args[1]))

	assert.Equal(t, ActCFG, msg.Action)
	assert.Equal(t, "t1", msg.From)
	assert.Contains(t, msg.Data, "config")

	// pass receving
	client.Send("next", "1")

	time.Sleep(time.Duration(1) * time.Millisecond)

	args, _ = client.Recv()

	assert.Equal(t, "msgs", args[0])

	msg, _ = MsgFromJSON([]byte(args[1]))

	assert.Equal(t, ActCFG, msg.Action)
	assert.Equal(t, "center", msg.From)
	assert.Contains(t, msg.Data, "config")

	expectedFlow := "initial/registering/receiving/routing/pushing"
	stateFlow := strings.Join(states, "/")
	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
}

// 推送其它
func Test_TerminalPushOther(t *testing.T) {
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
	}

	msgQ := make(chan *Msg, 1)

	mc := mconn.NewConn()
	client := adapter.NewSSDBOper(mc.Client, 1)

	options := map[string]interface{}{
		"name":             "t1",
		"msgOutQueue":      msgQ,
		"callbackDesc":     cbDesc,
		"msQueueOpTimeout": 1, // Millisecond
		"msNetTimeout":     1,
		"conn":             mc.Server,
		"mapInRouter":      cmap.New(),
		"mapOutRouter":     cmap.New(),
		"mapConfig":        cmap.New(),
	}

	s := TerminalRun(options)

	msgStr := `{
	    "action": "req",
	    "service": "hi",
	    "from": "1",
      "params": {
        "a": "abcde"
      }
	}`

	msgPush, _ := MsgFromJSON([]byte(msgStr))

	s.MQInOp.Push(msgPush, true)

	msgStr = `{
	    "action": "reg",
	    "service": "hi",
	    "from": "1"
	}`

	client.Send("next", "1", msgStr)
	// recv config
	client.Recv()
	// pass receving
	client.Send("next", "1")

	time.Sleep(time.Duration(1) * time.Millisecond)

	// recv push msg
	args, _ := client.Recv()
	assert.Equal(t, "msgs", args[0])

	msg, _ := MsgFromJSON([]byte(args[1]))

	assert.Equal(t, ActREQ, msg.Action)
	assert.Equal(t, "1", msg.From)
	assert.Contains(t, msg.Data, "params")

	_, ok := msg.Data["params"].(map[string]interface{})
	assert.True(t, ok, "params except  map[string]interface{}, but is %T", msg.Data["params"])

	expectedFlow := "initial/registering/receiving/routing/pushing"
	stateFlow := strings.Join(states, "/")
	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)
}

// 接收 ================
// 接收，请求路由、可、超时、不可
func Test_TerminalReqRoute(t *testing.T) {
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
	}

	msgQ := make(chan *Msg)
	mapConfig := cmap.New()
	mapConfig.Set("brokerName", "bfrom")

	mapOutRouter := cmap.New()
	routeMap := map[string]interface{}{
		"*": "btarget",
	}
	mapOutRouter.Set("hi", NewRandomSRouter(routeMap))

	mc := mconn.NewConn()
	client := adapter.NewSSDBOper(mc.Client, 1)

	options := map[string]interface{}{
		"name":             "t1",
		"msgOutQueue":      msgQ,
		"callbackDesc":     cbDesc,
		"msQueueOpTimeout": 1, // Millisecond
		"msNetTimeout":     1,
		"conn":             mc.Server,
		"mapInRouter":      cmap.New(),
		"mapOutRouter":     mapOutRouter,
		"mapConfig":        mapConfig,
	}

	s := TerminalRun(options)

	msgStr := `{
	    "action": "reg",
	    "service": "s1,s2",
	    "from": "1"
	}`

	client.Send("next", "1", msgStr)
	// recv config
	client.Recv()

	msgStr = `{
	    "action": "req",
	    "service": "hi",
	    "from": "1",
      "params": {
        "a": "abcde"
      }
	}`

	client.Send("next", "1", msgStr, msgStr)
	msg := <-msgQ
	assert.Equal(t, ActREQ, msg.Action)
	assert.Equal(t, "btarget", msg.Channel)
	assert.Equal(t, "bfrom@t1@1", msg.From)

	// route push MQOutOp timeout 1 times
	time.Sleep(time.Millisecond)

	// recv empty
	args, _ := client.Recv()
	assert.Equal(t, "empty", args[0])

	msgStr = `{
	    "action": "req",
	    "service": "unfound",
	    "from": "1",
      "params": {
        "a": "abcde"
      }
	}`

	client.Send("next", "1", msgStr)
	// recv empty
	client.Recv()

	r := s.Report()
	assert.Equal(t, 1, int(r["timesRouteFail"].(uint)))
	assert.Equal(t, 1, int(r["timesOutOpTimeout"].(uint)))

	expectedFlow := "initial/registering/receiving/routing/pushing/receiving/routing/pushing"
	stateFlow := strings.Join(states, "/")
	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)

}

// 接收，应答路由、可、超时、不可
func Test_TerminalResRoute(t *testing.T) {
	states := []string{}

	cbDesc := fsm.CallbackDesc{
		"enter-*": func(key fsm.CallbackKey, evt *fsm.Event) error {
			states = append(states, key.State)
			return nil
		},
	}

	msgQ := make(chan *Msg)
	mapConfig := cmap.New()
	mapConfig.Set("brokerName", "bfrom")
	mapOutRouter := cmap.New()

	mc := mconn.NewConn()
	client := adapter.NewSSDBOper(mc.Client, 1)

	options := map[string]interface{}{
		"name":             "t1",
		"msgOutQueue":      msgQ,
		"callbackDesc":     cbDesc,
		"msQueueOpTimeout": 1, // Millisecond
		"msNetTimeout":     1,
		"conn":             mc.Server,
		"mapInRouter":      cmap.New(),
		"mapOutRouter":     mapOutRouter,
		"mapConfig":        mapConfig,
	}

	s := TerminalRun(options)

	msgStr := `{
	    "action": "reg",
	    "service": "s1,s2",
	    "from": "1"
	}`

	client.Send("next", "1", msgStr)
	// recv config
	client.Recv()

	msgFailStr := `{
	    "action": "res",
      "service": "",
	    "code": 0,
	    "from": "1",
      "data": {
        "a": "abcde"
      }
	}`

	msgOkStr := `{
	    "action": "res",
      "service": "",
	    "code": 0,
	    "from": "b1@t1@1",
      "data": {
        "a": "abcde"
      }
	}`

	client.Send("next", "1", msgFailStr, msgOkStr, msgOkStr)
	msg := <-msgQ // only recv one msg, will timeout 1
	assert.Equal(t, ActRES, msg.Action)
	assert.Equal(t, "b1", msg.Channel)
	assert.Equal(t, "b1@t1@1", msg.From)

	// recv empty
	args, _ := client.Recv()
	assert.Equal(t, "empty", args[0])

	r := s.Report()
	assert.Equal(t, 1, int(r["timesRouteFail"].(uint)))
	assert.Equal(t, 1, int(r["timesOutOpTimeout"].(uint)))

	expectedFlow := "initial/registering/receiving/routing/pushing"
	stateFlow := strings.Join(states, "/")
	assert.True(t, strings.HasPrefix(stateFlow, expectedFlow), "wrong flow: %v", stateFlow)

}
