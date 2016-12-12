package qcache

import (
	"sync"
)

type Cache interface {
	Put(key string, frame *QFrame)
	Get(key string) *QFrame
}

type mapCache struct {
	lock   *sync.Mutex
	theMap map[string]*QFrame
}

func (c *mapCache) Put(key string, frame *QFrame) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.theMap[key] = frame
}

func (c *mapCache) Get(key string) *QFrame {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.theMap[key]
}

func newMapCache() *mapCache {
	return &mapCache{lock: &sync.Mutex{}, theMap: make(map[string]*QFrame)}
}
