package manage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/pool"
	"github.com/chashu-code/micro-broker/utils"
	rxpool "github.com/mediocregopher/radix.v2/pool"
	"github.com/uber-go/zap"
)

// IRedisPoolMap connection pool map interface
type IRedisPoolMap interface {
	FetchOrNew(ip string, size int) (*rxpool.Pool, bool, error)
	Fetch(ip string) *rxpool.Pool
	// Pools() map[string]*rxpool.Pool
}

// IBeanPoolMap connection pool map interface
type IBeanPoolMap interface {
	FetchOrNew(ip string, size int) (*pool.BeanPool, bool, error)
	Fetch(ip string) *pool.BeanPool
	// Pools() map[string]*rxpool.Pool
}

// IProtocol 消息协议接口
type IProtocol interface {
	BytesToMsg(bts []byte) (*Msg, error)
	MsgToBytes(msg *Msg) ([]byte, error)
}

// WrkRunFn 工作器运行方法
type WrkRunFn func(manager *Manager, ip string, count int)

// ProtocolGenFn 协议构造方法
type ProtocolGenFn func() IProtocol

// Manager 管理基本功能
type Manager struct {
	RedisPoolMap IRedisPoolMap
	BeanPoolMap  IBeanPoolMap
	Conf         *Config
	Log          zap.Logger
	logWriter    *os.File
	MsgQ         *MsgQueue

	ip string

	SubWrkRun      WrkRunFn
	CarryWrkRun    WrkRunFn
	ConfWrkRun     WrkRunFn
	CrontabWrkRun  WrkRunFn
	ClearWrkRun    WrkRunFn
	protocolGenMap map[uint]ProtocolGenFn

	chanStop      chan struct{}
	waitGroupStop *sync.WaitGroup

	verbose bool

	tid     int
	tidLock *sync.RWMutex
}

// NewManager 构造新的 Manager
func NewManager(conf *Config) *Manager {
	m := &Manager{
		Conf:           conf,
		protocolGenMap: make(map[uint]ProtocolGenFn),
		tidLock:        new(sync.RWMutex),
	}
	m.Log = m.genLog(conf.LogPath)
	m.chanStop = make(chan struct{}, 0)
	m.waitGroupStop = &sync.WaitGroup{}
	m.MsgQ = NewMsgQueueWithSize(conf.MsgQueueTimeoutMSecs, conf.MsgQueueSize)

	return m
}

// LogSync 日志同步
func (m *Manager) LogSync() error {
	if m.logWriter != nil {
		return m.logWriter.Sync()
	}
	return nil
}

func (m *Manager) genLog(path string) zap.Logger {
	if path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
		if err != nil {
			panic(err)
		}
		m.logWriter = f
		return zap.New(
			zap.NewJSONEncoder(),
			zap.AddStacks(zap.ErrorLevel),
			m.Conf.LogLevel,
			zap.Fields(zap.String("ip", m.IP())),
			zap.Output(f),
		)
	}

	return zap.New(
		zap.NewJSONEncoder(),
		zap.AddStacks(zap.ErrorLevel),
		m.Conf.LogLevel,
		zap.Fields(zap.String("ip", m.IP())),
	)

}

// NextTID 下一个有效traceid
func (m *Manager) NextTID() string {
	m.tidLock.Lock()
	defer m.tidLock.Unlock()

	if m.tid < defaults.TIDMax {
		m.tid++
	} else {
		m.tid = 1
	}

	return m.IP() + "/" + strconv.FormatInt(time.Now().Unix(), 10) + "/" + strconv.Itoa(m.tid)
}

// Start 运行
func (m *Manager) Start() {
	defer utils.LogRecover(m.Log, "manager shutdown", nil)

	m.Log.Info("manager start",
		zap.String("ipConf", m.Conf.IPConf),
		zap.Int("subWrkCount", m.Conf.SubWrkCount),
		zap.Int("carryWrkCount", m.Conf.CarryWorkerCount),
	)

	m.ConfWrkRun(m, m.Conf.IPConf, 1)
	m.CarryWrkRun(m, "", m.Conf.CarryWorkerCount)
	m.CrontabWrkRun(m, defaults.IPLocal, 1) // will make local bean pool
	m.ConnectRedis(m.IP())                  // will make local redis pool
	m.ClearWrkRun(m, m.IP(), 1)             // get local redis pool

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	s := <-c
	m.Log.Info("manager recv signal:" + s.String())
	m.Shutdown()
}

// AddProtocolGenFn 添加对应版本的协议生成方法
func (m *Manager) AddProtocolGenFn(v uint, fn ProtocolGenFn) {
	m.protocolGenMap[v] = fn
}

// Inbox 转换成 inbox key
func (m *Manager) Inbox(v interface{}) string {
	return fmt.Sprintf("ms:inbox:%v", v)
}

// Outbox 转换成 outbox key
func (m *Manager) Outbox(v interface{}) string {
	return fmt.Sprintf("ms:outbox:%v", v)
}

// CrontabName 返回配置crontabhash表名
func (m *Manager) CrontabName() string {
	return "ms:crontab:" + m.IP()
}

// WaitAdd 加入等待组
func (m *Manager) WaitAdd() {
	m.waitGroupStop.Add(1)
}

// WaitDone 等待组减员
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

// IP 内网地址
func (m *Manager) IP() string {
	if m.ip != "" {
		return m.ip
	}
	m.ip = utils.LocalIP()
	return m.ip
}

// ConnectRedis 链接指定IP，并启动相应的SubWorker（如果是第一次链接）
func (m *Manager) ConnectRedis(ip string) (*rxpool.Pool, error) {
	p, isNew, err := m.RedisPoolMap.FetchOrNew(ip, m.Conf.PoolSize)
	if isNew {
		m.SubWrkRun(m, ip, m.Conf.SubWrkCount)
	}
	return p, err
}

// Unpack bytes => msg
func (m *Manager) Unpack(bts []byte) (*Msg, error) {
	if len(bts) < 2 {
		return nil, errors.New("Unpack need []byte len > 1")
	}

	v := uint(bts[0])
	gen := m.protocolGenMap[v]

	if gen == nil {
		return nil, fmt.Errorf("Unpack with error version: %v", v)
	}
	return gen().BytesToMsg(bts[1:])
}

// Pack msg => bytes
func (m *Manager) Pack(msg *Msg) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("Pack can't with nil")
	}

	gen := m.protocolGenMap[msg.V]
	if gen == nil {
		return nil, fmt.Errorf("Pack with error version: %v", msg.V)
	}

	bts, err := gen().MsgToBytes(msg)
	if err != nil {
		return nil, err
	}

	var buff bytes.Buffer
	buff.WriteByte(byte(msg.V))
	buff.Write(bts)
	return buff.Bytes(), nil
}
