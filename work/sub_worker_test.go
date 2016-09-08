package work

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/manage"
	"github.com/chashu-code/micro-broker/pool"
	"github.com/chashu-code/micro-broker/protocol"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
)

func pretendRead(s string) *redis.Resp {
	buf := bytes.NewBufferString(s)
	return redis.NewRespReader(buf).Read()
}

func newManager() *manage.Manager {
	conf := manage.NewConfig()
	mgr := manage.NewManager(conf)
	mgr.AddProtocolGenFn(1, protocol.NewV1Protocol)
	return mgr
}

func newSubWorker() *SubWorker {
	mgr := newManager()
	w := &SubWorker{
		subIP:  adjustSubIP(mgr.IP(), mgr.IP()),
		destIP: mgr.IP(),
	}
	w.mgr = mgr
	return w
}

type testBuffer struct {
	bytes.Buffer
}

func (b *testBuffer) Sync() error {
	return nil
}

func (b *testBuffer) Lines() []string {
	output := strings.Split(b.String(), "\n")
	return output[:len(output)-1]
}

func (b *testBuffer) Stripped() string {
	return strings.TrimRight(b.String(), "\n")
}

func Test_SubWorker_adjustSubIP(t *testing.T) {
	assert.Equal(t, defaults.IPLocal, adjustSubIP("x", "x"))
	assert.Equal(t, "a", adjustSubIP("a", "x"))
}

func Test_SubWorker_resToMsg(t *testing.T) {
	w := newSubWorker()
	// empty array
	r := redis.NewResp(nil)
	msg, err := w.resToMsg(r)
	assert.Nil(t, err)
	assert.Nil(t, msg)

	// no a list bytes
	r = redis.NewResp("value is string")
	msg, err = w.resToMsg(r)
	assert.Contains(t, err.Error(), "convert to array")

	// error version
	bts := newMsgBytes(0, 0, w.mgr)
	r = redis.NewRespFlattenedStrings([][]byte{[]byte{0}, bts})
	msg, err = w.resToMsg(r)
	assert.Contains(t, err.Error(), "error version")

	// is dead
	bts = newMsgBytes(1, 0, w.mgr)
	r = redis.NewRespFlattenedStrings([][]byte{[]byte{0}, bts})
	msg, err = w.resToMsg(r)
	assert.Contains(t, err.Error(), "msg is dead")

	// ok
	bts = newMsgBytes(1, time.Now().Unix(), w.mgr)
	r = redis.NewRespFlattenedStrings([][]byte{[]byte{0}, bts})
	msg, err = w.resToMsg(r)
	assert.Nil(t, err)
	assert.Equal(t, uint(1), msg.V)

}

func Test_SubWorker_process(t *testing.T) {
	w := newSubWorker()
	sink := w.newSinkLog()
	w.redisPoolMap = pool.NewRedisPoolMap()
	w.mgr.Conf.WrkPauseSecs = 0
	w.mgr.Conf.PopTimeoutSecs = 1
	w.process()
	logHas(t, sink, "can't fetch redis pool")

	p, _, _ := w.redisPoolMap.FetchOrNew(w.mgr.IP(), 1)
	lstName := w.mgr.Outbox(w.subIP)
	p.Cmd("del", lstName)
	sink = w.newSinkLog()
	w.process()

	bts := newMsgBytes(0, time.Now().Unix(), w.mgr)
	p.Cmd("rpush", lstName, bts)
	sink = w.newSinkLog()
	w.process()
	logHas(t, sink, "error version")
	logNotHas(t, sink, "msgPack")

	bts = newMsgBytes(1, 0, w.mgr)
	p.Cmd("rpush", lstName, bts)
	sink = w.newSinkLog()
	w.process()
	logHas(t, sink, "msg is dead")
	logHas(t, sink, "msgPack")

	bts = newMsgBytes(1, time.Now().Unix(), w.mgr)
	p.Cmd("rpush", lstName, bts)
	w.newSinkLog()
	_, ok := w.mgr.MsgQ.Pop(false)
	assert.False(t, ok)
	w.process()
	_, ok = w.mgr.MsgQ.Pop(false)
	assert.True(t, ok)
}
