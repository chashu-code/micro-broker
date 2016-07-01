package tio

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chashu-code/micro-broker/manage"
)

// reference: "github.com/gansidui/gotcp"

var (
	// ErrSendNilPacket 发送了空的数据包
	ErrSendNilPacket = errors.New("send a nil packet")
)

// Terminal 终端
type Terminal struct {
	Name        string
	Manager     manage.IManager
	SubMsgPoper *MultiMsgQueuePoper
	ResQueue    *MsgQueue

	Conf      *Config
	conn      net.Conn
	onceClose sync.Once
	flagClose int32

	rid uint
}

// TerminalRun 构造并执行Terminal
func TerminalRun(name string, manager manage.IManager, config *Config, conn net.Conn) {
	t := TerminalBuild(name, manager, config)
	t.conn = conn
	t.run()
}

func TerminalBuild(name string, manager manage.IManager, config *Config) *Terminal {
	t := &Terminal{
		Name:    name,
		Manager: manager,
		Conf:    config,
	}

	t.ResQueue = NewMsgQueue(t.Conf.NetTimeout)
	t.Manager.MapSet(manage.IDMapQueue, t.Name, t.ResQueue)
	return t
}

// RID 返回当前 rid 的字符串
func (t *Terminal) RID() string {
	return strconv.Itoa(int(t.rid))
}

// RIDNext 返回下一个rid
func (t *Terminal) RIDNext() string {
	t.rid++
	return t.RID()
}

// Protocol 返回协议
func (t *Terminal) Protocol() IProtocol {
	return t.Conf.Protocol
}

// MsgQueue 获取指定name的消息队列，找不到返回nil
func (t *Terminal) MsgQueue(name string) *MsgQueue {
	if v, ok := t.Manager.MapGet(manage.IDMapQueue, name); ok {
		if queue, ok := v.(*MsgQueue); ok {
			return queue
		}
	}
	return nil
}

// DelMsgQueue 移除并关闭指定name的MsgQueue
func (t *Terminal) DelMsgQueue(name string) {
	if v, ok := t.Manager.MapGet(manage.IDMapQueue, name); ok {
		t.Manager.MapDel(manage.IDMapQueue, name)
		if queue, ok := v.(*MsgQueue); ok {
			close(queue.C)
		}
	}
}

func (t *Terminal) run() {
	defer func() {
		if err := recover(); err != nil {
			t.Conf.ConnCallback.OnError(t, err)
		}
		t.close()
	}()

	t.Conf.ConnCallback.OnConnect(t)

	durDeadline := time.Duration(t.Conf.TerminalTimeout) * time.Millisecond

	for !(t.Manager.IsShutdown() || t.isClosed()) {
		if err := t.conn.SetDeadline(time.Now().Add(durDeadline)); err != nil {
			panic(err)
		}
		packet, err := t.doRecv()
		if err != nil {
			if !isTimeout(err) {
				panic(err)
			}
		} else {
			if !t.doPacket(packet) {
				return
			}
		}
	}
}

func (t *Terminal) doRecv() (IPacket, error) {
	// t.readAt = time.Now()
	return t.Conf.Protocol.ReadPacket(t.conn)
}

func (t *Terminal) doPacket(packet IPacket) bool {
	return t.Conf.ConnCallback.OnData(t, packet)
}

// SendPacket 发送数据包
func (t *Terminal) SendPacket(packet IPacket) error {
	data := packet.Bytes()
	if data == nil {
		return ErrSendNilPacket
	}
	_, err := t.conn.Write(data)
	return err
}

func (t *Terminal) isClosed() bool {
	return atomic.LoadInt32(&t.flagClose) == 1
}

func (t *Terminal) close() {
	t.onceClose.Do(func() {
		t.DelMsgQueue(t.Name)
		atomic.StoreInt32(&t.flagClose, 1)
		t.conn.Close()
		t.Conf.ConnCallback.OnClose(t)
	})
}
