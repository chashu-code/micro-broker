package manage

import (
	"errors"
	"strings"
	"testing"

	"github.com/chashu-code/micro-broker/defaults"
	"github.com/chashu-code/micro-broker/utils"
	"github.com/stretchr/testify/assert"
)

type testProtocol struct {
}

func (p *testProtocol) BytesToMsg(bts []byte) (*Msg, error) {
	if len(bts) != 2 {
		return nil, errors.New("error bytes length")
	}

	msg := &Msg{
		Code: string(bts),
	}

	return msg, nil
}

func (p *testProtocol) MsgToBytes(msg *Msg) ([]byte, error) {
	if len(msg.Code) != 2 {
		return nil, errors.New("error msg code length")
	}

	return []byte(msg.Code), nil
}

func genProtocol() IProtocol {
	return &testProtocol{}
}

func newManager() *Manager {
	conf := NewConfig()
	return NewManager(conf)
}

func Test_Manager_Inbox(t *testing.T) {
	mgr := newManager()
	assert.Equal(t, "ms:inbox:1", mgr.Inbox(1))
}

func Test_Manager_Outbox(t *testing.T) {
	mgr := newManager()
	assert.Equal(t, "ms:outbox:1", mgr.Outbox(1))
}

func Test_Manager_Shutdown(t *testing.T) {
	mgr := newManager()
	assert.False(t, mgr.IsShutdown())
	mgr.Shutdown()
	assert.True(t, mgr.IsShutdown())
}

func Test_Manager_IP(t *testing.T) {
	mgr := newManager()
	ips, _ := utils.IntranetIP()
	ip := ips[0]
	assert.Equal(t, ip, mgr.IP())
	assert.Equal(t, ip, mgr.IP())
}

func Test_Manager_CrontabName(t *testing.T) {
	mgr := newManager()
	name := "ms:crontab:" + mgr.IP()
	assert.Equal(t, name, mgr.CrontabName())
}

func Test_Manager_Pack(t *testing.T) {
	mgr := newManager()
	_, err := mgr.Pack(nil)
	assert.Error(t, err) // msg is nil

	mgr.AddProtocolGenFn(1, genProtocol)

	msg := &Msg{}
	_, err = mgr.Pack(msg)
	assert.Error(t, err) // error version

	msg.V = 1

	var bts []byte
	bts, err = mgr.Pack(msg)
	assert.Error(t, err) // msg.Code need 2 char

	msg.Code = "ab"
	bts, err = mgr.Pack(msg)
	assert.Equal(t, []byte{1, 'a', 'b'}, bts)
	assert.Nil(t, err)
}

func Test_Manager_UnPack(t *testing.T) {
	bts := []byte{2, 'a', 'b'}
	mgr := newManager()

	_, err := mgr.Unpack(bts)
	assert.Error(t, err)

	mgr.AddProtocolGenFn(2, genProtocol)

	var msg *Msg
	msg, err = mgr.Unpack(bts)
	assert.Nil(t, err)
	assert.Equal(t, "ab", msg.Code)
}

func Test_Manager_NextTID(t *testing.T) {
	mgr := newManager()

	count := 5
	tidMap := map[string]bool{}

	signal := make(chan bool, 0)

	go func() {
		for i := 0; i < count; i++ {
			tidMap[mgr.NextTID()] = true
		}
		signal <- true
	}()

	for i := 0; i < count; i++ {
		tidMap[mgr.NextTID()] = true
	}

	<-signal

	// 应该有10个
	assert.Equal(t, 10, len(tidMap))
	idMap := map[string]bool{}

	for tid := range tidMap {
		id := strings.Split(tid, "/")[2]
		idMap[id] = true
	}

	// 且id不同
	assert.Equal(t, 10, len(idMap))

	// 超过最大值，自动调整为1
	mgr.tid = defaults.TIDMax
	tid := mgr.NextTID()
	id := strings.Split(tid, "/")[2]
	assert.Equal(t, "1", id)
}
