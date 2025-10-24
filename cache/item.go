package cache

import "time"

// Item 表示缓存项
type Item struct {
	Value      any
	Expiration int64
	Created    time.Time
}

// 判断缓存项是否过期
func (item Item) Expired() bool {
	if item.Expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > item.Expiration
}
