// Package log 提供日志增强
package log

import "github.com/Sirupsen/logrus"

// Fields 日志输出项目描述
type Fields map[string]interface{}

// Able 日志增强
type Able struct {
	// FixFields 固定的输出项目描述，一般在初始化时指定
	FixFields Fields
}

// Log  记录Fields，并返回支持 Debug Info Warning .. 等方法的日志实例
func (lg *Able) Log(fields Fields) *logrus.Entry {
	for k, v := range lg.FixFields {
		fields[k] = v
	}
	return logrus.WithFields(logrus.Fields(fields))
}
