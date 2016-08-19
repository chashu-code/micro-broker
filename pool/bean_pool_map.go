package pool

import (
	"strings"
	"sync"

	"github.com/chashu-code/micro-broker/defaults"
)

// BeanPoolMap redis connection pool map
type BeanPoolMap struct {
	lock    *sync.RWMutex
	poolMap map[string]*BeanPool
}

// NewBeanPoolMap 构建一个新的BeanPoolMap
func NewBeanPoolMap() *BeanPoolMap {
	return &BeanPoolMap{
		lock:    new(sync.RWMutex),
		poolMap: make(map[string]*BeanPool),
	}
}

// Fetch 获取或者构造指定路径的 BeanPool，result => pool, is_new, error
func (pmap *BeanPoolMap) Fetch(ip string, size int) (*BeanPool, bool, error) {
	var addr string
	if ip == defaults.IPLocal {
		addr = "127.0.0.1:11300"
	} else if !strings.Contains(ip, ":") {
		addr = ip + ":11300"
	}

	pmap.lock.Lock()
	defer pmap.lock.Unlock()

	if p := pmap.poolMap[addr]; p != nil {
		return p, false, nil
	}

	p := NewBeanPool(addr, size)
	pmap.poolMap[addr] = p
	return p, true, nil
}

// func (pmap *BeanPoolMap) Pools() map[string]*BeanPool {
// 	pmap.lock.RLock()
// 	defer pmap.lock.RUnlock()
// 	resMap := make(map[string]*BeanPool)
// 	for k, v := range pmap.poolMap {
// 		resMap[k] = v
// 	}
// 	return resMap
// }
