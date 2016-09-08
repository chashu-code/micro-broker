package pool

import (
	"testing"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/stretchr/testify/assert"
)

func Test_RedisPoolMap_FetchOK(t *testing.T) {
	pmap := NewRedisPoolMap()
	poolF := pmap.Fetch(defaults.IPLocal)
	assert.Nil(t, poolF)

	pool, isNew, err := pmap.FetchOrNew(defaults.IPLocal, 2)
	assert.Nil(t, err)
	assert.True(t, isNew)
	assert.NotNil(t, pool)

	poolF = pmap.Fetch(defaults.IPLocal)
	assert.NotNil(t, poolF)

	pool, isNew, err = pmap.FetchOrNew("127.0.0.1", 2)
	assert.Nil(t, err)
	assert.False(t, isNew)
	assert.NotNil(t, pool)
}

func Test_RedisPoolMap_FetchErr(t *testing.T) {
	ipErr := "127.0.0.1:6377"

	pmap := NewRedisPoolMap()
	pool, isNew, err := pmap.FetchOrNew(ipErr, 2)
	assert.Error(t, err)
	assert.False(t, isNew)
	assert.Nil(t, pool)
	assert.Equal(t, 0, len(pmap.poolMap))

	poolF := pmap.Fetch(ipErr)
	assert.Nil(t, poolF)
}

func Test_RedisPoolMap_ipToAddr(t *testing.T) {
	pmap := NewRedisPoolMap()

	addr := pmap.ipToAddr(defaults.IPLocal)
	assert.Equal(t, "127.0.0.1:6379", addr)

	addr = pmap.ipToAddr("127.0.0.2")
	assert.Equal(t, "127.0.0.2:6379", addr)

	addr = pmap.ipToAddr("127.0.0.3:6636")
	assert.Equal(t, "127.0.0.3:6636", addr)
}
