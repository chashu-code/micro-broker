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

func Test_BeanClient_PutArgsWithCode(t *testing.T) {
	c := okBeanClient()
	for _, code := range []string{"", "aaa", "11|11"} {
		pri, delay, ttr, err := c.PutArgsWithCode(code)
		assert.Nil(t, err)
		assert.Equal(t, uint32(100), pri)
		assert.Equal(t, time.Duration(0), delay)
		assert.Equal(t, 5*time.Minute, ttr)
	}
	code := "2|x|4"
	pri, delay, ttr, err := c.PutArgsWithCode(code)
	assert.Error(t, err)

	code = "2|3|4"
	pri, delay, ttr, err = c.PutArgsWithCode(code)
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), pri)
	assert.Equal(t, 3*time.Second, delay)
	assert.Equal(t, 4*time.Second, ttr)
}

func Test_BeanClient_Put(t *testing.T) {
	c := okBeanClient()
	data := []byte("hello")
	code := "2|3|4"
	pri, delay, ttr, _ := c.PutArgsWithCode(code)
	_, errPut := c.Put("default", data, pri, delay, ttr)
	assert.Nil(t, errPut)
}
