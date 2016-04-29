package manage

import (
	"github.com/Sirupsen/logrus"
	cmap "github.com/streamrail/concurrent-map"
)

const (
	// AddrLocal 本地地址
	AddrLocal = "local"
)

const (
	// IDMapConf 配置表Id
	IDMapConf uint8 = iota
	// IDMapQueue 队列表Id
	IDMapQueue
)

// IManager 管理接口
type IManager interface {
	Name() string
	MapGet(id uint8, key string) (interface{}, bool)
	MapSet(id uint8, key string, value interface{}) bool
	MapDel(id uint8, key string)
	Map(id uint8) cmap.ConcurrentMap
	RouteNextDest(service string) string
	DestAddr(dest string) string
	Verbose() bool
	SetVerbose(bool)
	IsShutdown() bool
}

// Manager 管理基本功能
type Manager struct {
	shutdown   bool
	verbose    bool
	name       string
	mapCatalog map[uint8]cmap.ConcurrentMap
}

// Name 名称
func (m *Manager) Name() string {
	return m.name
}

func (m *Manager) init(name string) {
	m.name = name
	m.mapCatalog = make(map[uint8]cmap.ConcurrentMap)
	m.mapCatalog[IDMapConf] = cmap.New()
	m.mapCatalog[IDMapQueue] = cmap.New()
}

// 是否已停止
func (m *Manager) IsShutdown() bool {
	return m.shutdown
}

func (m *Manager) Verbose() bool {
	return m.verbose
}

func (m *Manager) SetVerbose(v bool) {
	m.verbose = v
	if v {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}
}

// MapGet 从指定表中查找key，并返回value，找不到返回 nil, false
func (m *Manager) MapGet(id uint8, key string) (interface{}, bool) {
	if mapItem, ok := m.mapCatalog[id]; ok {
		return mapItem.Get(key)
	}
	return nil, false
}

// MapSet 更新表内容，找不到表返回false
func (m *Manager) MapSet(id uint8, key string, val interface{}) bool {
	if mapItem, ok := m.mapCatalog[id]; ok {
		mapItem.Set(key, val)
		return true
	}
	return false
}

// MapDel 更新表内容，找不到表返回false
func (m *Manager) MapDel(id uint8, key string) {
	if mapItem, ok := m.mapCatalog[id]; ok {
		mapItem.Remove(key)
	}
}

// Map 返回指定id的表，找不到返回 nil
func (m *Manager) Map(id uint8) cmap.ConcurrentMap {
	if item, ok := m.mapCatalog[id]; ok {
		return item
	}
	return nil
}

// RouteNextDest 依据Service，进行路由，返回可用目标
func (m *Manager) RouteNextDest(service string) string {
	return "dev"
}

// DestAddr 返回目标访问地址
func (m *Manager) DestAddr(dest string) string {
	return AddrLocal
}
