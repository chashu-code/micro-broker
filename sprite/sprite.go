package sprite

import (
	"github.com/Sirupsen/logrus"
	"github.com/chashu-code/micro-broker/fsm"
	"github.com/chashu-code/micro-broker/log"
)

const (
	// SpriteInitState 精灵初始化状态值
	SpriteInitState = "initial"
)

// Sprite 精灵结构
type Sprite struct {
	Srv    *fsm.Service
	report map[string]interface{}
}

// ISprite 精灵接口
type ISprite interface {
	Run(map[string]interface{})
	Stop()
	Report() map[string]interface{}
}

// Run 构造并运行精灵
func (s *Sprite) Run(options map[string]interface{}) {
	s.Srv = fsm.New(options)
	s.report = make(map[string]interface{})
	go s.Srv.Serve()
}

// Stop 停止精灵
func (s *Sprite) Stop() {
	evt := fsm.Event{
		Name: fsm.EvtNameStopFSM,
	}
	s.Srv.Flow(evt)
}

// Log 日志记录方法
func (s *Sprite) Log(fields log.Fields) *logrus.Entry {
	return s.Srv.Log(fields)
}

// Report 汇报自身相关信息
func (s *Sprite) Report() map[string]interface{} {
	return s.report
}
