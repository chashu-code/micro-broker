package defaults

const (
	// IPLocal 本地地址
	IPLocal = "local"

	// TIDMax 最大TID流水号，超过置0
	TIDMax = 10000000
	// DefaultJobPoolSize 默认Job池大小
	DefaultJobPoolSize = 3
	// DefaultPoolSize 默认池大小
	DefaultPoolSize = 10
	// DefaultPopTimeoutSecs 默认获取队列等待超时秒数
	DefaultPopTimeoutSecs = 5
	// DefaultSubWrkCount 默认订阅工作器数量
	DefaultSubWrkCount = 2
	// DefaultCarryWorkerCount 默认搬运工作器数量
	DefaultCarryWorkerCount = 20
	// DefaultMsgQueueSize 默认消息队列数量
	DefaultMsgQueueSize = 20
	// DefaultMsgQueueTimeoutMSecs 默认消息队列超时时间
	DefaultMsgQueueTimeoutMSecs = 1000

	// DefaultWrkPauseSecs 工作器需要间歇时，暂停秒数
	DefaultWrkPauseSecs = 5
)
