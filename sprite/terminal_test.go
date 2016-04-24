package sprite

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/chashu-code/micro-broker/adapter"
	cmap "github.com/streamrail/concurrent-map"

	mconn "github.com/jordwest/mock-conn"
	"github.com/stretchr/testify/assert"
)

// 初始化部分 ===========
func Test_TerminalInitSuccess(t *testing.T) {

	mapQueueOp := cmap.New()
	mapOutRouter := cmap.New()
	mapConfig := cmap.New()

	mapConfig.Set(KeyBrokerName, "brokerName")
	mapQueueOp.Set(KeyPubQueue, NewMsgQueueOp(make(chan *Msg), 1))

	mc := mconn.NewConn()
	tid := "#Ttest"
	options := map[string]interface{}{
		"name":         tid,
		"conn":         mc.Server,
		"mapQueueOp":   mapQueueOp,
		"mapOutRouter": mapOutRouter,
		"mapConfig":    mapConfig,
	}

	TerminalRun(options)

	time.Sleep(time.Millisecond)

	assert.True(t, mapQueueOp.Has(tid))
}

// Reg ===============
func Test_TerminalReg(t *testing.T) {
	mapQueueOp := cmap.New()
	mapOutRouter := cmap.New()
	mapConfig := cmap.New()

	mapConfig.Set(KeyBrokerName, "brokerName")
	mapQueueOp.Set(KeyPubQueue, NewMsgQueueOp(make(chan *Msg), 1))

	mc := mconn.NewConn()
	tid := "#Ttest"

	options := map[string]interface{}{
		"name":               tid,
		"conn":               mc.Server,
		"mapQueueOp":         mapQueueOp,
		"mapOutRouter":       mapOutRouter,
		"mapConfig":          mapConfig,
		"msInQueueOpTimeout": 1,
	}

	TerminalRun(options)

	client := adapter.NewSSDBOper(mc.Client, 1)

	assert.False(t, mapQueueOp.Has("one"))
	assert.False(t, mapQueueOp.Has("two"))

	res, _ := client.Cmd("reg", "one,two")
	assert.Equal(t, "ok", res[0])
	subToken := res[1]

	assert.True(t, mapConfig.Has(subToken))
	assert.True(t, mapQueueOp.Has("one"))
	assert.True(t, mapQueueOp.Has("two"))
}

// Sub ===============
func Test_TerminalSub(t *testing.T) {
	mapQueueOp := cmap.New()
	mapOutRouter := cmap.New()
	mapConfig := cmap.New()

	mapConfig.Set(KeyBrokerName, "brokerName")
	mapQueueOp.Set(KeyPubQueue, NewMsgQueueOp(make(chan *Msg), 1))

	mc := mconn.NewConn()
	tid := "#Ttest"

	options := map[string]interface{}{
		"name":               tid,
		"conn":               mc.Server,
		"mapQueueOp":         mapQueueOp,
		"mapOutRouter":       mapOutRouter,
		"mapConfig":          mapConfig,
		"msInQueueOpTimeout": 1,
	}

	TerminalRun(options)

	client := adapter.NewSSDBOper(mc.Client, 2)

	// 未注册
	res, _ := client.Cmd("sub", "error subToken")
	assert.Equal(t, "err", res[0])

	// 注册前，先加入一个mqOp
	mqTwo := NewMsgQueueOp(make(chan *Msg, 1), 1)
	mapQueueOp.Set("two", mqTwo)
	// 注册
	res, _ = client.Cmd("reg", "one,two")
	assert.Equal(t, "ok", res[0])
	subToken := res[1]

	// 未有消息
	res, _ = client.Cmd("sub", subToken)
	assert.Equal(t, "empty", res[0])

	// 有消息
	v, _ := mapQueueOp.Get("one")
	mqOne := v.(*MsgQueueOp)
	msg1 := &Msg{
		Action:  ActREQ,
		Service: "one",
		From:    "a@b@c",
	}

	msg2 := &Msg{
		Action:  ActREQ,
		Service: "two",
		From:    "a@b@c",
	}

	mqOne.C <- msg1
	mqTwo.C <- msg2

	for i := 0; i < 2; i++ {
		// 有缓冲的 channel send 后，未必能马上 recv 到
		for {
			res, _ = client.Cmd("sub", subToken)
			if res[0] == "empty" {
				continue
			}
			break
		}

		assert.Equal(t, "ok", res[0])
		msgRes, _ := MsgFromJSON([]byte(res[1]))
		assert.Contains(t, []string{"one", "two"}, msgRes.Service)
	}
}

// Res ===============
func Test_TerminalRes(t *testing.T) {
	mapQueueOp := cmap.New()
	mapOutRouter := cmap.New()
	mapConfig := cmap.New()

	mqOpPub := NewMsgQueueOp(make(chan *Msg, 1), 1)
	mapConfig.Set(KeyBrokerName, "brokerName")
	mapQueueOp.Set(KeyPubQueue, mqOpPub)

	mc := mconn.NewConn()
	tid := "#Ttest"

	options := map[string]interface{}{
		"name":               tid,
		"conn":               mc.Server,
		"mapQueueOp":         mapQueueOp,
		"mapOutRouter":       mapOutRouter,
		"mapConfig":          mapConfig,
		"msInQueueOpTimeout": 1,
	}

	TerminalRun(options)

	client := adapter.NewSSDBOper(mc.Client, 1)

	// 错误的from
	msg := &Msg{
		Action: ActRES,
	}
	data, _ := msg.ToJSON()
	res, _ := client.Cmd("res", data)
	assert.Equal(t, "err", res[0])
	assert.Contains(t, res[1], "bid")

	// fix From
	msg.From = "b1@t1@r1"
	data, _ = msg.ToJSON()

	// 已满
	mqOpPub.C <- msg // 满仓
	res, _ = client.Cmd("res", data)
	assert.Equal(t, "err", res[0])
	assert.Contains(t, res[1], "full")

	// 正常回应
	<-mqOpPub.C // 清仓
	res, _ = client.Cmd("res", data)
	assert.Equal(t, "ok", res[0])
	msgPub := <-mqOpPub.C
	assert.Equal(t, msg.From, msgPub.From)
}

// Req ===============
func Test_TerminalReq(t *testing.T) {
	mapOutRouter := cmap.New()
	router := NewRandomSRouter(map[string]interface{}{
		"*": "b1",
	})
	mapOutRouter.Set("hello", router)

	mapConfig := cmap.New()
	mapConfig.Set(KeyBrokerName, "brokerName")

	mapQueueOp := cmap.New()
	mqOpPub := NewMsgQueueOp(make(chan *Msg, 1), 1)
	mapQueueOp.Set(KeyPubQueue, mqOpPub)

	mc := mconn.NewConn()
	tid := "#Ttest"

	options := map[string]interface{}{
		"name":               tid,
		"conn":               mc.Server,
		"mapQueueOp":         mapQueueOp,
		"mapOutRouter":       mapOutRouter,
		"mapConfig":          mapConfig,
		"msInQueueOpTimeout": 1,
	}

	ts := TerminalRun(options)

	client := adapter.NewSSDBOper(mc.Client, 1)

	// 无相关处理路由
	msg := &Msg{
		Action:  ActREQ,
		Service: "unfound",
	}
	data, _ := msg.ToJSON()
	res, _ := client.Cmd("req", data)
	assert.Equal(t, "err", res[0])
	assert.Contains(t, res[1], "unfound service router")

	// fix Service
	msg.Service = "hello"

	// 推送队列满
	mqOpPub.C <- msg // 满仓
	data, _ = msg.ToJSON()
	res, _ = client.Cmd("req", data)
	assert.Equal(t, "err", res[0])
	assert.Contains(t, res[1], "full")

	// 获取应答超时
	<-mqOpPub.C // 清仓
	res, _ = client.Cmd("req", data)
	assert.Equal(t, "err", res[0])
	assert.Contains(t, res[1], "timeout")

	// 获取应答（哪怕有过期res）
	go func() {
		v, _ := mapQueueOp.Get(tid)
		mqOp := v.(*MsgQueueOp)

		// 错误
		mqOp.C <- &Msg{
			Action: ActRES,
			From:   "wrong_rid",
		}

		// 过期
		mqOp.C <- &Msg{
			Action: ActRES,
			From:   "b1@t1@" + ts.RID(),
		}

		// 有效
		rid, _ := strconv.Atoi(ts.RID())
		rid++
		mqOp.C <- &Msg{
			Action: ActRES,
			From:   fmt.Sprintf("b1@t2@%v", rid),
		}
	}()

	<-mqOpPub.C // 清仓
	res, _ = client.Cmd("req", data)
	assert.Equal(t, "ok", res[0])
	msgRes, _ := MsgFromJSON([]byte(res[1]))
	assert.Contains(t, msgRes.From, "b1@t2@")

	// job ~ 和 req 一样的处理逻辑
	msg.Action = ActJOB
	data, _ = msg.ToJSON()
	res, _ = client.Cmd("job", data)
	assert.Equal(t, "err", res[0])
	assert.Contains(t, res[1], "full")
}

// Sync ===============
func Test_TerminalSync(t *testing.T) {
	mapQueueOp := cmap.New()
	mapOutRouter := cmap.New()
	mapConfig := cmap.New()

	mqOpPub := NewMsgQueueOp(make(chan *Msg, 1), 1)
	mapConfig.Set(KeyBrokerName, "brokerName")
	mapQueueOp.Set(KeyPubQueue, mqOpPub)

	mc := mconn.NewConn()
	tid := "#Ttest"

	options := map[string]interface{}{
		"name":               tid,
		"conn":               mc.Server,
		"mapQueueOp":         mapQueueOp,
		"mapOutRouter":       mapOutRouter,
		"mapConfig":          mapConfig,
		"msInQueueOpTimeout": 1,
	}

	TerminalRun(options)

	client := adapter.NewSSDBOper(mc.Client, 1)

	confName := "myConf"
	confData := "this is the conf data"
	// 错误的参数个数
	res, _ := client.Cmd("sync", confName)
	assert.Equal(t, "err", res[0])
	assert.Contains(t, res[1], "need args")

	// 找不到配置
	res, _ = client.Cmd("sync", confName, "")
	assert.Equal(t, "newest", res[0])

	// 版本不一致
	mapConfig.Set(KeyVerConf, "v1")
	mapConfig.Set(confName, confData)
	res, _ = client.Cmd("sync", confName, "")
	assert.Equal(t, "ok", res[0])
	assert.Equal(t, "v1", res[1])
	assert.Contains(t, res[2], confData)

	// 版本一致
	res, _ = client.Cmd("sync", confName, "v1")
	assert.Equal(t, "newest", res[0])

	// 不可序列化配置
	mapConfig.Set(confName, mqOpPub)
	res, _ = client.Cmd("sync", confName, "")
	assert.Equal(t, "err", res[0])
	assert.Contains(t, res[1], "encode")
}
