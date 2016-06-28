package tio

import (
	"time"

	"github.com/chashu-code/micro-broker/log"
	"github.com/kr/beanstalk"
)

// IJobClient 任务客户端接口
type IJobClient interface {
	Put(tube string, body []byte, pri uint32, delay, ttr time.Duration) (uint64, error)
	Stats(tube string) (map[string]string, error)
	Close()
}

// BeanJobClient Beanstalk Job Client
type BeanJobClient struct {
	log.Able

	addr string
	conn *beanstalk.Conn
	tube *beanstalk.Tube
}

// NewBeanJobClient 构建新的BeanJobClient
func NewBeanJobClient(addr string) *BeanJobClient {
	c := &BeanJobClient{addr: addr}
	c.FixFields = log.Fields{
		"server": addr,
	}
	return c
}

// Close 关闭连接
func (c *BeanJobClient) Close() {
	if c.conn != nil {
		c.tube = nil
		c.conn.Close()
		c.conn = nil
	}
}

// Put 推送任务至tube
func (c *BeanJobClient) Put(tube string, body []byte, pri uint32, delay, ttr time.Duration) (uint64, error) {
	if err := c.connect(); err != nil {
		return 0, err
	}

	t := c.getTube(tube)
	id, err := t.Put(body, pri, delay, ttr)
	c.closeIfEOF("BeanJobClient put fail", err) // 若发生错误，则关闭链接
	return id, err
}

// Stats 返回tube状态
func (c *BeanJobClient) Stats(tube string) (map[string]string, error) {
	if err := c.connect(); err != nil {
		return nil, err
	}

	t := c.getTube(tube)
	info, err := t.Stats()
	c.closeIfEOF("BeanJobClient stats fail", err) // 若发生错误，则关闭链接
	return info, err
}

func (c *BeanJobClient) getTube(tube string) *beanstalk.Tube {
	if c.tube == nil || c.tube.Name != tube {
		c.tube = &beanstalk.Tube{
			Conn: c.conn,
			Name: tube,
		}
	}
	return c.tube
}

func (c *BeanJobClient) connect() error {
	var err error
	if c.conn == nil {
		c.conn, err = beanstalk.Dial("tcp", AddrJobServerDefault)
		if err != nil {
			c.Log(log.Fields{
				"error": err,
			}).Error("BeanJobClient connect fail")
		}
	}
	return err
}

func (c *BeanJobClient) closeIfEOF(msg string, err error) {
	if err != nil {
		c.Log(log.Fields{
			"error": err,
		}).Error(msg)

		c.Close()
	}
}
