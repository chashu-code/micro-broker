package manage

import (
	"testing"

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
