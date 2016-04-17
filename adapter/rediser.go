package adapter

import (
	"errors"
	"net/url"
	"time"

	"github.com/mediocregopher/radix.v2/pubsub"
	"github.com/mediocregopher/radix.v2/redis"
)

// IRedis 接口
type IRedis interface {
	LastCritical() error
	Cmd(cmd string, args ...interface{}) *redis.Resp
	Close() error

	Subscribe(channels ...interface{}) *pubsub.SubResp
	Receive() (string, error)
}

// RediserBuilder 构造方法类型
type RediserBuilder func(url string, msTimeout int) (IRedis, error)

// Rediser redis.Client的包装
type Rediser struct {
	client    *redis.Client
	subClient *pubsub.SubClient
}

// ErrRedisClientNil redis client 未初始化错误
var ErrRedisClientNil = errors.New("redis client is nil")

// ErrReceiveNotMessage 接收到的不是预期的 message
var ErrReceiveNotMessage = errors.New("redis receive not a message")

// ErrReceiveTimeout 接收 message timeout
var ErrReceiveTimeout = errors.New("redis receive timeout")

// BuildRedister 构造 Rediser方法
func BuildRedister(addr string, msTimeout int) (IRedis, error) {
	var r *Rediser
	var err error
	var uri *url.URL

	uri, err = url.Parse(addr)
	if err != nil {
		return r, err
	}

	r = &Rediser{}
	if r.client, err = redis.DialTimeout("tcp", uri.Host, time.Duration(msTimeout)*time.Millisecond); err != nil {
		return r, err
	}

	return r, nil
}

// LastCritical 最后错误
func (r *Rediser) LastCritical() error {
	if r.client != nil {
		return r.client.LastCritical
	}
	return ErrRedisClientNil
}

// Cmd 执行命令
func (r *Rediser) Cmd(cmd string, args ...interface{}) *redis.Resp {
	if r.client == nil {
		panic(ErrRedisClientNil)
	}
	return r.client.Cmd(cmd, args...)
}

// Close 关闭连接
func (r *Rediser) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

// Subscribe 订阅
func (r *Rediser) Subscribe(channels ...interface{}) *pubsub.SubResp {
	if r.client == nil {
		panic(ErrRedisClientNil)
	}

	// 构造 subClient
	if r.subClient == nil {
		r.subClient = pubsub.NewSubClient(r.client)
	}

	return r.subClient.Subscribe(channels...)
}

// Receive 接收订阅消息
func (r *Rediser) Receive() (string, error) {
	if r.subClient == nil {
		panic(ErrRedisClientNil)
	}

	resp := r.subClient.Receive()
	err := resp.Err
	if err == nil && resp.Type != pubsub.Message {
		err = ErrReceiveNotMessage
	} else if resp.Timeout() {
		err = ErrReceiveTimeout
	}

	return resp.Message, err
}
