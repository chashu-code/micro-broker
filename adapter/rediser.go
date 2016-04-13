package adapter

import (
	"errors"
	"net/url"

	"github.com/mediocregopher/radix.v2/redis"
)

// IRedis 接口
type IRedis interface {
	LastCritical() error
	Cmd(cmd string, args ...interface{}) *redis.Resp
	Close() error
	// Subscribe(channels ...interface{}) *pubsub.SubResp
}

// RediserBuilder 构造方法类型
type RediserBuilder func(url string) (IRedis, error)

// Rediser redis.Client的包装
type Rediser struct {
	client *redis.Client
}

// ErrRedisClientNil redis client 未初始化错误
var ErrRedisClientNil = errors.New("redis client is nil")

// BuildRedister 构造 Rediser方法
func BuildRedister(addr string) (IRedis, error) {
	var r *Rediser
	var err error
	var uri *url.URL

	uri, err = url.Parse(addr)
	if err != nil {
		return r, err
	}

	r = &Rediser{}
	if r.client, err = redis.Dial("tcp", uri.Host); err != nil {
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
