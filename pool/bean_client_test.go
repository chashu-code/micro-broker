package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func okBeanClient() *BeanClient {
	// 需要开启 beanstalkd 127.0.0.1:11300
	addr := "127.0.0.1:11300"
	return NewBeanClient(addr)
}

func Test_BeanClient_NewOK(t *testing.T) {
	c := okBeanClient()
	assert.Nil(t, c.LastCritical)
}

func Test_BeanClient_NewError(t *testing.T) {
	addr := "127.0.0.1:11333"
	c := NewBeanClient(addr)
	assert.NotNil(t, c.LastCritical)
}

func Test_BeanClient_Close(t *testing.T) {
	c := okBeanClient()
	c.Close()
	assert.NotNil(t, c.LastCritical)
}

func Test_BeanClient_Stats(t *testing.T) {
	c := okBeanClient()
	info, err := c.Stats("UNFOUND")
	assert.Empty(t, info)
	assert.Contains(t, err.Error(), "not found")
	info, err = c.Stats("default")
	assert.Nil(t, err)
	assert.NotEmpty(t, info)
}

func Test_BeanClient_Put(t *testing.T) {
	c := okBeanClient()
	data := []byte("hello")
	pri := uint32(0)
	delay := time.Duration(0)
	ttr := 5 * time.Minute
	_, errPut := c.Put("default", data, pri, delay, ttr)
	assert.Nil(t, errPut)
}
