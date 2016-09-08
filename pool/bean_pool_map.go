package pool

import (
	"strings"
	"sync"

	"github.com/chashu-code/micro-broker/defaults"
)

// BeanPoolMap redis connection pool map
type BeanPoolMap struct {
	portTail string
	lock     *sync.RWMutex
	poolMap  map[string]*BeanPool
}

// NewBeanPoolMap 构建一个新的BeanPoolMap
func NewBeanPoolMap() *BeanPoolMap {
	return &BeanPoolMap{
		portTail: ":11300",
		lock:     new(sync.RWMutex),
		poolMap:  make(map[string]*BeanPool),
	}
}

func (pmap *BeanPoolMap) ipToAddr(ip string) string {
	addr := ip
	if ip == defaults.IPLocal {
		addr = "127.0.0.1" + pmap.portTail
	} else if !strings.Contains(ip, ":") {
		addr = ip + pmap.portTail
	}
	return addr
}

// FetchOrNew 获取或者构造指定路径的 BeanPool，result => pool, is_new, error
func (pmap *BeanPoolMap) FetchOrNew(ip string, size int) (*BeanPool, bool, error) {
	addr := pmap.ipToAddr(ip)

	pmap.lock.Lock()
	defer pmap.lock.Unlock()

	if p := pmap.poolMap[addr]; p != nil {
		return p, false, nil
	}

	p := NewBeanPool(addr, size)
	pmap.poolMap[addr] = p
	return p, true, nil
}

// Fetch 获取指定ip的BeanPool
func (pmap *BeanPoolMap) Fetch(ip string) *BeanPool {
	addr := pmap.ipToAddr(ip)

	return pmap.poolMap[addr]
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
