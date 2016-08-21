package manage

import (
	"testing"

	"github.com/chashu-code/micro-broker/utils"
	"github.com/stretchr/testify/assert"
)

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

}

func Test_Manager_UnPack(t *testing.T) {

}

func Test_Manager_NextTID(t *testing.T) {

}

func Test_Manager_Connect(t *testing.T) {

}
