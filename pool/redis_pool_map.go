package pool

import (
	"strings"
	"sync"

	"github.com/chashu-code/micro-broker/defaults"
	rxpool "github.com/mediocregopher/radix.v2/pool"
)

// RedisPoolMap redis connection pool map
type RedisPoolMap struct {
	lock    *sync.RWMutex
	poolMap map[string]*rxpool.Pool
}

// NewRedisPoolMap 构建一个新的RedisPoolMap
func NewRedisPoolMap() *RedisPoolMap {
	return &RedisPoolMap{
		lock:    new(sync.RWMutex),
		poolMap: make(map[string]*rxpool.Pool),
	}
}

// Fetch 获取或者构造指定路径的 RedisPool，result => pool, is_new, error
func (pmap *RedisPoolMap) Fetch(ip string, size int) (*rxpool.Pool, bool, error) {
	addr := ip
	if ip == defaults.IPLocal {
		addr = "127.0.0.1:6379"
	} else if !strings.Contains(ip, ":") {
		addr = ip + ":6379"
	}

	pmap.lock.Lock()
	defer pmap.lock.Unlock()

	if p := pmap.poolMap[addr]; p != nil {
		return p, false, nil
	}

	p, err := rxpool.New("tcp", addr, size)

	if err != nil {
		return nil, false, err
	}

	pmap.poolMap[addr] = p
	return p, true, nil
}

// func (pmap *RedisPoolMap) Pools() map[string]*rxpool.Pool {
// 	pmap.lock.RLock()
// 	defer pmap.lock.RUnlock()
// 	resMap := make(map[string]*rxpool.Pool)
// 	for k, v := range pmap.poolMap {
// 		resMap[k] = v
// 	}
// 	return resMap
// }
