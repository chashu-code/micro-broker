package pool

import (
	"errors"
	"strings"
	"time"

	"github.com/kr/beanstalk"
)

// BeanClient beanstalkd client
type BeanClient struct {
	conn         *beanstalk.Conn
	LastCritical error
}

// NewBeanClient 构建新的 beanstalkd client
func NewBeanClient(addr string) *BeanClient {
	conn, err := beanstalk.Dial("tcp", addr)
	return &BeanClient{
		conn:         conn,
		LastCritical: err,
	}
}

// Close 关闭连接
func (c *BeanClient) Close() error {
	if c.IsAble() {
		c.LastCritical = errors.New("connection has closed")
		return c.conn.Close()
	}
	return nil
}

// Stats 返回状态
func (c *BeanClient) Stats(tubeName string) (map[string]string, error) {

	if !c.IsAble() {
		return nil, errors.New("client disabled")
	}

	info, err := c.getTube(tubeName).Stats()
	c.errCheck(err)

	return info, err
}

// Put 推入任务
func (c *BeanClient) Put(tubeName string, body []byte, pri uint32, delay, ttr time.Duration) (uint64, error) {
	if !c.IsAble() {
		return 0, errors.New("client disabled")
	}
	id, err := c.getTube(tubeName).Put(body, pri, delay, ttr)
	c.errCheck(err)
	return id, err
}

func (c *BeanClient) getTube(name string) *beanstalk.Tube {
	return &beanstalk.Tube{
		Conn: c.conn,
		Name: name,
	}
}

// IsAble 是否可用？
func (c *BeanClient) IsAble() bool {
	return c.conn != nil && c.LastCritical == nil
}

func (c *BeanClient) errCheck(err error) {
	if err == nil {
		return
	}
	msg := err.Error()
	if strings.Contains(msg, "refused") || strings.Contains(msg, "EOF") {
		c.LastCritical = err
	}
}
