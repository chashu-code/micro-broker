package manage

import (
	"container/list"
	"sort"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	cmap "github.com/streamrail/concurrent-map"
)

const (
	// KeyBrokerName  配置中，当前borker name
	KeyBrokerName = "#NAME"
	// KeyBrokersOnline 配置中，在线brokers
	KeyBrokersOnline = "#ON-BRKS"

	// KeyGateWaysOnline 配置中，在线的gateway
	KeyGateWaysOnline = "#ON-GWS"

	// KeyJobQueue 任务队列名
	KeyJobQueue = "#QUEUE-JOB"

	// KeyBrokerQueue 代理消息队列名
	KeyBrokerQueue = "#QUEUE-BRK"

	// KeyPubQueue 推送消息队列名
	KeyPubQueue = "#QUEUE-PUB"

	// KeyVerConf 配置版本名
	KeyVerConf = "#VER-CONF"

	// KeyVerGateWaysOnline 在线gateway版本
	KeyVerGateWaysOnline = "#VER-ON-GWS"

	// KeyVerBrokersOnline 在线broker版本
	KeyVerBrokersOnline = "#VER-ON-BRKS"

	// KeyVerServiceRoute 服务路由版本
	KeyVerServiceRoute = "#VER-SROUTE"

	// KeyServiceRoute 服务路由
	KeyServiceRoute = "#SROUTE"

	// KeyCronJob
	KeyCronJob = "#CRONJOB"

	// AddrLocal 本地地址
	AddrLocal = "local"
)

const (
	// IDMapConf 配置表Id
	IDMapConf uint8 = iota
	// IDMapQueue 队列表Id
	IDMapQueue
)

// ConfigSyncCallback 配置信息同步回调
type ConfigSyncCallback func()

// IManager 管理接口
type IManager interface {
	Init(string)
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
	Shutdown()
	WaitAdd()
	WaitDone()
	RegConfigSyncCallback(ConfigSyncCallback)
	SyncConfig(map[string]interface{})
}

// Manager 管理基本功能
type Manager struct {
	verbose             bool
	name                string
	mapCatalog          map[uint8]cmap.ConcurrentMap
	chanStop            chan struct{}
	waitGroupStop       *sync.WaitGroup
	lstConfSyncCallback *list.List
}

func (m *Manager) WaitAdd() {
	m.waitGroupStop.Add(1)
}

func (m *Manager) WaitDone() {
	m.waitGroupStop.Done()
}

// IsShutdown 是否已关闭
func (m *Manager) IsShutdown() bool {
	select {
	case <-m.chanStop:
		return true
	default:
		return false
	}
}

// Shutdown 停止
func (m *Manager) Shutdown() {
	close(m.chanStop)
	m.waitGroupStop.Wait()
}

// Name 名称
func (m *Manager) Name() string {
	return m.name
}

func (m *Manager) Init(name string) {
	m.name = name
	m.mapCatalog = make(map[uint8]cmap.ConcurrentMap)
	m.mapCatalog[IDMapConf] = cmap.New()
	m.mapCatalog[IDMapQueue] = cmap.New()
	m.chanStop = make(chan struct{}, 0)
	m.waitGroupStop = &sync.WaitGroup{}
	m.lstConfSyncCallback = list.New()
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

// RegConfigSyncCallback 注册配置信息同步回调
func (m *Manager) RegConfigSyncCallback(cb ConfigSyncCallback) {
	m.lstConfSyncCallback.PushBack(cb)
}

func (m *Manager) SyncConfig(info map[string]interface{}) {
	for k, v := range info {
		switch k {
		case KeyBrokersOnline, KeyGateWaysOnline:
			if vStr, ok := v.(string); ok {
				m.MapSet(IDMapConf, k, PickBrokersFromStr(vStr))
			}
		case KeyServiceRoute:
			// if vMap, ok = v.(map[string]interface{}); ok {
			// 	for service, rv := range vMap {
			// 		if routeMap, ok = rv.(map[string]interface{}); !ok {
			// 			continue
			// 		}
			// 		if router, err := PickRouterFromMap(routeMap); err != nil {
			// 			m.Log(log.Fields{
			// 				"service": service,
			// 				"error":   err,
			// 			}).Error("PickRouterFromMap fail")
			// 		} else {
			// 			m.MapSet(IDMapQueue, service, router)
			// 		}
			// 	}
			// }
		default:
			m.MapSet(IDMapConf, k, v)
		}
	}

	for e := m.lstConfSyncCallback.Front(); e != nil; e = e.Next() {
		cb := e.Value.(ConfigSyncCallback)
		cb()
	}
}

// PickBrokersFromStr 分割字符串，提取并返回排序后的borkers列表
func PickBrokersFromStr(str string) []string {
	if len(str) == 0 {
		return []string{}
	}
	bs := strings.Split(str, ",")
	sort.Strings(bs)
	return bs
}
