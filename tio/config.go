package tio

const (
	// AddrListenDefault 默认监听地址
	AddrListenDefault = "127.0.0.1:6636"
	// AddrJobServerDefault 默认Beanstalk监听地址
	AddrJobServerDefault = "127.0.0.1:11300"

	// MsgQueueSizeDefault 消息队列最大缓冲
	MsgQueueSizeDefault = 10
	// JobQueueOpTimeoutDefault 任务队列操作超时毫秒
	JobQueueOpTimeoutDefault = 1000
	// MsgQueueOpTimeoutDefault 消息队列操作超时毫秒
	MsgQueueOpTimeoutDefault = 100
	// MultiMsgQueuePoperTimeoutDefault 消息队列操作超时毫秒
	MultiMsgQueuePoperTimeoutDefault = 1000
	// NetTimeoutDefault 网络操作超时毫秒
	NetTimeoutDefault = 1000
	// TerminalTimeoutDefault 终端连接操作超时毫秒
	TerminalTimeoutDefault = 10000
	// JobPutWorkerSizeDefault size
	JobPutWorkerSizeDefault = 5
)

// Config 常用配置
type Config struct {
	AddrListen                string
	AddrJobServer             string
	MsgQueueSize              int
	MsgQueueOpTimeout         int
	MultiMsgQueuePoperTimeout int
	NetTimeout                int
	TerminalTimeout           int
	Protocol                  IProtocol
	ConnCallback              IConnCallback
}

// ConfigDefault 返回默认配置
func ConfigDefault() *Config {
	c := &Config{
		AddrListen:                AddrListenDefault,
		AddrJobServer:             AddrJobServerDefault,
		MsgQueueSize:              MsgQueueSizeDefault,
		MsgQueueOpTimeout:         MsgQueueOpTimeoutDefault,
		MultiMsgQueuePoperTimeout: MultiMsgQueuePoperTimeoutDefault,
		NetTimeout:                NetTimeoutDefault,
		TerminalTimeout:           TerminalTimeoutDefault,
	}
	return c
}
