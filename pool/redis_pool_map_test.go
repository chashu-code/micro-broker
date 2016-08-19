package pool

import (
	"testing"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/stretchr/testify/assert"
)

func Test_RedisPoolMap_FetchOK(t *testing.T) {
	pmap := NewRedisPoolMap()
	pool, isNew, err := pmap.Fetch(defaults.IPLocal, 2)
	assert.Nil(t, err)
	assert.True(t, isNew)
	assert.NotNil(t, pool)
	pool, isNew, err = pmap.Fetch("127.0.0.1", 2)
	assert.Nil(t, err)
	assert.False(t, isNew)
	assert.NotNil(t, pool)
}

func Test_RedisPoolMap_FetchErr(t *testing.T) {
	pmap := NewRedisPoolMap()
	pool, isNew, err := pmap.Fetch("127.0.0.1:6377", 2)
	assert.Error(t, err)
	assert.False(t, isNew)
	assert.Nil(t, pool)
	assert.Equal(t, 0, len(pmap.poolMap))
}
