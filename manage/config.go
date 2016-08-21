package manage

import (
	"github.com/chashu-code/micro-broker/defaults"
	"github.com/uber-go/zap"
)

// Config 配置信息
type Config struct {
	JobPoolSize      int
	PoolSize         int
	PopTimeoutSecs   int
	SubWrkCount      int
	CarryWorkerCount int

	MsgQueueSize         int
	MsgQueueTimeoutMSecs int

	WrkPauseSecs int

	CrontabJobDslMap map[string]string
	IPConf           string

	LogLevel zap.Level
	LogPath  string
}

// NewConfig 构建新的配置
func NewConfig() *Config {
	return &Config{
		JobPoolSize:          defaults.DefaultJobPoolSize,
		PoolSize:             defaults.DefaultPoolSize,
		PopTimeoutSecs:       defaults.DefaultPopTimeoutSecs,
		SubWrkCount:          defaults.DefaultSubWrkCount,
		CarryWorkerCount:     defaults.DefaultCarryWorkerCount,
		MsgQueueSize:         defaults.DefaultMsgQueueSize,
		MsgQueueTimeoutMSecs: defaults.DefaultMsgQueueTimeoutMSecs,
		WrkPauseSecs:         defaults.DefaultWrkPauseSecs,
		CrontabJobDslMap:     make(map[string]string, 0),
		IPConf:               defaults.IPLocal,
		LogLevel:             zap.DebugLevel,
	}
}
