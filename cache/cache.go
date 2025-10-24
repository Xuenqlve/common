package cache

import (
	"sync"
	"time"
)

// Cache 是一个简单的内存缓存实现
type Cache struct {
	items             map[string]Item
	mu                sync.RWMutex
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
	stopCleanup       chan bool
}

// 默认的过期时间常量
const (
	NoExpiration      time.Duration = -1
	DefaultExpiration time.Duration = 0
)

// NewCache 创建一个新的缓存实例
func NewCache(defaultExpiration, cleanupInterval time.Duration) *Cache {
	cache := &Cache{
		items:             make(map[string]Item),
		defaultExpiration: defaultExpiration,
		cleanupInterval:   cleanupInterval,
		stopCleanup:       make(chan bool),
	}

	// 启动定期清理过期项的协程
	if cleanupInterval > 0 {
		go cache.startCleanupTimer()
	}

	return cache
}

// startCleanupTimer 启动清理定时器
func (c *Cache) startCleanupTimer() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.DeleteExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

// Set 设置缓存项，可指定过期时间
func (c *Cache) Set(key string, value any, d time.Duration) {
	var exp int64
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}
	if d > 0 {
		exp = time.Now().Add(d).UnixNano()
	}

	c.mu.Lock()
	c.items[key] = Item{
		Value:      value,
		Expiration: exp,
		Created:    time.Now(),
	}
	c.mu.Unlock()
}

// Get 获取缓存项
func (c *Cache) Get(key string) (any, bool) {
	c.mu.RLock()
	item, found := c.items[key]
	c.mu.RUnlock()

	if !found {
		return nil, false
	}

	if item.Expired() {
		// 如果已过期，删除该项
		c.mu.Lock()
		delete(c.items, key)
		c.mu.Unlock()
		return nil, false
	}

	return item.Value, true
}

// Delete 删除缓存项
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// DeleteExpired 删除所有过期的缓存项
func (c *Cache) DeleteExpired() {
	now := time.Now().UnixNano()
	c.mu.Lock()
	for k, v := range c.items {
		if v.Expiration > 0 && now > v.Expiration {
			delete(c.items, k)
		}
	}
	c.mu.Unlock()
}

// Close 关闭缓存清理协程
func (c *Cache) Close() error {
	if c.cleanupInterval > 0 {
		c.stopCleanup <- true
	}
	return nil
}
