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
	Server      IServer
	conn        net.Conn
	onceClose   sync.Once
	flagClose   int32
	protocol    IProtocol
	Manager     manage.IManager
	SubMsgPoper *MultiMsgQueuePoper

	rid      uint
	ResQueue *MsgQueue
}

// TerminalRun 构造并执行Terminal
func TerminalRun(name string, manager manage.IManager, server IServer, conn net.Conn) {
	t := &Terminal{
		Name:    name,
		Server:  server,
		Manager: manager,
		conn:    conn,
		protocol: &MPProtocol{
			timeoutDeadLine: time.Second,
		},
	}

	t.ResQueue = NewMsgQueue(NetTimeoutDefault)
	t.Manager.MapSet(manage.IDMapQueue, t.Name, t.ResQueue)
	t.run()
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
	return t.protocol
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
			t.Server.ConnCallback().OnError(t, err)
		}
		t.close()
	}()

	t.Server.ConnCallback().OnConnect(t)

	for !(t.Server.IsShutdown() || t.isClosed()) {
		// TODO 后期需要push时，再异步化
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
	return t.protocol.ReadPacket(t.conn)
}

func (t *Terminal) doPacket(packet IPacket) bool {
	return t.Server.ConnCallback().OnData(t, packet)
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
		t.Server.ConnCallback().OnClose(t)
	})
}
