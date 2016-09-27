package utils

import (
	"fmt"

	"github.com/uber-go/zap"
)

// LogRecoverBackFn 回调
type LogRecoverBackFn func(recoverInfo interface{})

// LogRecover 退出延迟记录用
func LogRecover(log zap.Logger, msg string, fn LogRecoverBackFn) {
	r := recover()

	if r != nil {
		err, ok := r.(error)
		if !ok {
			err = fmt.Errorf("%v", r)
		}
		log.Panic("unexpected "+msg, zap.Error(err))
	} else {
		log.Info(msg)
	}

	if fn == nil {
		if r != nil {
			panic(r)
		}
		return
	}

	fn(r)
}
