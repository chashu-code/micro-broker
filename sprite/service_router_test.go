package sprite

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_PickBrokersFromStr(t *testing.T) {
	// 空字符串，返回 []string{}
	bs := PickBrokersFromStr("")
	assert.Equal(t, 0, len(bs))

	// 分割并自动排序
	bs = PickBrokersFromStr("b,a,c")
	assert.Equal(t, []string{"a", "b", "c"}, bs)
}

func Test_calcBrokersOn(t *testing.T) {
	src := PickBrokersFromStr("a,b,c")
	// all empty
	all := []string{}
	on := calcBrokersOn(src, all)
	assert.Equal(t, src, on)

	// all > src
	all = PickBrokersFromStr("b,c,d,a,f,g")
	on = calcBrokersOn(src, all)
	assert.Equal(t, src, on)

	// all diff src
	all = PickBrokersFromStr("b,d,c,f,g")
	on = calcBrokersOn(src, all)
	assert.Equal(t, []string{"b", "c"}, on)
}
