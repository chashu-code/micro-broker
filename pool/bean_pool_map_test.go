package pool

import (
	"testing"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/stretchr/testify/assert"
)

func Test_BeanPoolMap_FetchOK(t *testing.T) {
	pmap := NewBeanPoolMap()
	pool, isNew, err := pmap.Fetch(defaults.IPLocal, 2)
	assert.Nil(t, err)
	assert.True(t, isNew)
	assert.NotNil(t, pool)
	pool, isNew, err = pmap.Fetch("127.0.0.1", 2)
	assert.Nil(t, err)
	assert.False(t, isNew)
	assert.NotNil(t, pool)
}

func Test_BeanPoolMap_FetchErr(t *testing.T) {
	pmap := NewBeanPoolMap()
	pool, isNew, err := pmap.Fetch("127.0.0.1:11333", 2)
	assert.Nil(t, err)
	assert.True(t, isNew)
	assert.NotNil(t, pool)
	assert.Equal(t, 1, len(pmap.poolMap))
}
