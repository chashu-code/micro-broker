package work

import (
	"errors"
	"testing"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
)

func newConfWorker() *ConfWorker {
	w := &ConfWorker{
		IP: defaults.IPLocal,
	}
	w.mgr = newManager()
	return w
}

func Test_ConfWorker_resToV(t *testing.T) {
	w := newConfWorker()
	r := redis.NewResp(nil)
	v, err := w.resToV(r)
	assert.Nil(t, err)
	assert.Equal(t, "", v)

	r = redis.NewResp(errors.New("test error"))
	v, err = w.resToV(r)
	assert.Contains(t, err.Error(), "version parse fail")

	r = redis.NewResp("123")
	v, err = w.resToV(r)
	assert.Nil(t, err)
	assert.Equal(t, "123", v)
}

func Test_ConfWorker_processCrontab(t *testing.T) {
	w := newConfWorker()
	w.redisPoolMap = pool.NewRedisPoolMap()

	p, _, _ := w.redisPoolMap.FetchOrNew(defaults.IPLocal, 1)
	tabName := w.mgr.CrontabName()

	// crontab empty
	p.Cmd("del", tabName)
	sink := w.newSinkLog()
	w.processCrontab(p)
	assert.Empty(t, sink.Logs())

	// crontab error data format
	p.Cmd("set", tabName, "abc")
	sink = w.newSinkLog()
	w.processCrontab(p)
	logHas(t, sink, "get crontab version fail")

	// 清理
	w.V = "dead"
	p.Cmd("del", tabName)
	sink = w.newSinkLog()
	w.processCrontab(p)
	logHas(t, sink, "clear crontab job")

	// 更新
	p.Cmd("hmset", tabName, "v", "update", "f1", "fv1")
	sink = w.newSinkLog()
	w.processCrontab(p)
	logHas(t, sink, "get crontab success")
	assert.Equal(t, "update", w.mgr.Conf.CrontabJobDslMap["v"])
	assert.Equal(t, "fv1", w.mgr.Conf.CrontabJobDslMap["f1"])
}
