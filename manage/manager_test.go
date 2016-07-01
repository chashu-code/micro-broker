package manage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ManagerMapOp(t *testing.T) {
	m := &Manager{}
	m.Init("test")

	// map
	idUnfound := uint8(10)
	mp := m.Map(idUnfound)
	assert.Nil(t, mp)

	mp = m.Map(IDMapConf)
	assert.NotNil(t, mp)

	// map_set
	result := m.MapSet(idUnfound, "hello", 1)
	assert.False(t, result)

	result = m.MapSet(IDMapConf, "hello", 1)
	assert.True(t, result)

	// map_get
	v, ok := m.MapGet(idUnfound, "hello")
	assert.False(t, ok)

	v, ok = m.MapGet(IDMapConf, "hello")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	v, ok = m.MapGet(IDMapConf, "unfound")
	assert.False(t, ok)
	assert.Nil(t, v)

	// map_del
	m.MapDel(idUnfound, "hello")
	m.MapDel(IDMapConf, "hello")

	_, ok = m.MapGet(IDMapConf, "hello")
	assert.False(t, ok)
}

func Test_ManagerVerbose(t *testing.T) {
	m := &Manager{}
	m.Init("test")

	assert.Equal(t, "test", m.Name())

	assert.False(t, m.Verbose())
	m.SetVerbose(true)
	assert.True(t, m.Verbose())
}

func Test_ManagerShutdown(t *testing.T) {
	m := &Manager{}
	m.Init("test")

	assert.False(t, m.IsShutdown())
	m.Shutdown()
	assert.True(t, m.IsShutdown())
}

func Test_ManagerPickBrokersFromStr(t *testing.T) {

}
