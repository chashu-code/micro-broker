package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_BeanPool_WithErrAddr(t *testing.T) {
	pool := NewBeanPool("127.0.0.1:11333", 2)
	fn := func(c *BeanClient) error {
		return nil
	}
	err := pool.With(fn)
	assert.NotNil(t, err)
	assert.Equal(t, 0, len(pool.pool))
}

func Test_BeanPool_WithOKAddr(t *testing.T) {
	pool := NewBeanPool("127.0.0.1:11300", 2)
	fn := func(c *BeanClient) error {
		info, err := c.Stats("default")
		assert.NotEmpty(t, info)
		return err
	}
	err := pool.With(fn)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(pool.pool))
}

func Test_BeanPool_GetPut(t *testing.T) {
	pool := NewBeanPool("127.0.0.1:11300", 1)
	c := pool.Get()
	assert.Equal(t, 0, len(pool.pool))

	pool.Put(c)
	assert.Equal(t, 1, len(pool.pool))

	c = pool.Get()
	assert.Equal(t, 0, len(pool.pool))

	c.Close()
	pool.Put(c)
	assert.Equal(t, 0, len(pool.pool))
}
