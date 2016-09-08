package pool

import (
	"testing"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/stretchr/testify/assert"
)

func Test_BeanPoolMap_FetchOK(t *testing.T) {
	pmap := NewBeanPoolMap()
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

func Test_BeanPoolMap_FetchErr(t *testing.T) {
	ipErr := "127.0.0.1:11333"
	pmap := NewBeanPoolMap()
	pool, isNew, err := pmap.FetchOrNew(ipErr, 2)
	assert.Nil(t, err)
	assert.True(t, isNew)
	assert.NotNil(t, pool)
	assert.Equal(t, 1, len(pmap.poolMap))

	poolF := pmap.Fetch(ipErr)
	assert.NotNil(t, poolF)
}

func Test_BeanPoolMap_ipToAddr(t *testing.T) {
	pmap := NewBeanPoolMap()

	addr := pmap.ipToAddr(defaults.IPLocal)
	assert.Equal(t, "127.0.0.1:11300", addr)

	addr = pmap.ipToAddr("127.0.0.2")
	assert.Equal(t, "127.0.0.2:11300", addr)

	addr = pmap.ipToAddr("127.0.0.3:6636")
	assert.Equal(t, "127.0.0.3:6636", addr)
}
