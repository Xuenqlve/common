package cache

import (
	"sync"
	"testing"
	"time"
)

// 测试 Set 和 Get 功能
func TestCacheSetGet(t *testing.T) {
	cache := NewCache(DefaultExpiration, 0)
	defer cache.Close()

	// 测试设置和获取
	cache.Set("key1", "value1", DefaultExpiration)
	value, found := cache.Get("key1")
	if !found {
		t.Error("缓存中应存在键 'key1'")
	}
	if value != "value1" {
		t.Errorf("预期值 'value1'，实际得到 '%v'", value)
	}

	// 测试不存在的键
	_, found = cache.Get("nonexistent")
	if found {
		t.Error("不应找到键 'nonexistent'")
	}

	// 测试使用自定义过期时间
	cache.Set("key2", 42, 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond) // 等待过期
	_, found = cache.Get("key2")
	if found {
		t.Error("键 'key2' 应该已过期")
	}

	// 测试永不过期
	cache.Set("key3", true, NoExpiration)
	time.Sleep(100 * time.Millisecond)
	_, found = cache.Get("key3")
	if !found {
		t.Error("使用 NoExpiration 的键 'key3' 不应过期")
	}
}

// 测试 Delete 功能
func TestCacheDelete(t *testing.T) {
	cache := NewCache(DefaultExpiration, 0)
	defer cache.Close()

	// 设置一个项并验证存在
	cache.Set("key1", "value1", DefaultExpiration)
	_, found := cache.Get("key1")
	if !found {
		t.Fatal("缓存中应存在键 'key1'")
	}

	// 删除并验证已删除
	cache.Delete("key1")
	_, found = cache.Get("key1")
	if found {
		t.Error("删除后不应找到键 'key1'")
	}

	// 删除不存在的键（应该不会出错）
	cache.Delete("nonexistent")
}

// 测试自动过期和 DeleteExpired 功能
func TestCacheExpiration(t *testing.T) {
	// 创建短清理间隔的缓存
	cache := NewCache(50*time.Millisecond, 100*time.Millisecond)
	defer cache.Close()

	// 添加几个项，其中一些带有自定义过期时间
	cache.Set("key1", 1, DefaultExpiration) // 使用默认过期时间 (50ms)
	cache.Set("key2", 2, 200*time.Millisecond)
	cache.Set("key3", 3, NoExpiration)

	// 等待默认过期时间
	time.Sleep(75 * time.Millisecond)

	// 验证过期情况
	_, found := cache.Get("key1")
	if found {
		t.Error("键 'key1' 应该已过期")
	}

	// key2 应该仍然存在
	_, found = cache.Get("key2")
	if !found {
		t.Error("键 'key2' 不应该已过期")
	}

	// key3 应该永不过期
	_, found = cache.Get("key3")
	if !found {
		t.Error("键 'key3' 不应过期")
	}

	// 再等待一段时间使 key2 过期
	time.Sleep(150 * time.Millisecond)

	// 手动调用 DeleteExpired
	cache.DeleteExpired()

	// key2 现在应该已过期
	_, found = cache.Get("key2")
	if found {
		t.Error("键 'key2' 应该已过期")
	}

	// key3 仍应存在
	_, found = cache.Get("key3")
	if !found {
		t.Error("键 'key3' 不应过期")
	}
}

// 测试并发安全性
func TestCacheConcurrency(t *testing.T) {
	cache := NewCache(5*time.Minute, 0)
	defer cache.Close()

	// 同时进行读取和写入操作
	const workers = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(workers * 2) // 读取和写入工作线程

	// 写入工作线程
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := string('A' + rune(workerID))
				cache.Set(key, j, DefaultExpiration)
			}
		}(i)
	}

	// 读取工作线程
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				for k := 0; k < workers; k++ {
					key := string('A' + rune(k))
					cache.Get(key)
				}
			}
		}(i)
	}

	// 等待所有工作线程完成
	wg.Wait()

	// 如果没有 panic，则测试通过
}

// 测试不同类型的值
func TestCacheDifferentTypes(t *testing.T) {
	cache := NewCache(DefaultExpiration, 0)
	defer cache.Close()

	// 测试各种类型
	testCases := []struct {
		key   string
		value any
	}{
		{"string", "hello"},
		{"int", 42},
		{"float", 3.14},
		{"bool", true},
		{"slice", []string{"a", "b", "c"}},
		{"map", map[string]int{"one": 1, "two": 2}},
		{"struct", struct{ Name string }{"test"}},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			cache.Set(tc.key, tc.value, DefaultExpiration)
			value, found := cache.Get(tc.key)
			if !found {
				t.Errorf("键 '%s' 未找到", tc.key)
				return
			}

			// 检查类型和值
			switch expected := tc.value.(type) {
			case string:
				if actual, ok := value.(string); !ok || actual != expected {
					t.Errorf("预期 %v，得到 %v", expected, value)
				}
			case int:
				if actual, ok := value.(int); !ok || actual != expected {
					t.Errorf("预期 %v，得到 %v", expected, value)
				}
			case float64:
				if actual, ok := value.(float64); !ok || actual != expected {
					t.Errorf("预期 %v，得到 %v", expected, value)
				}
			case bool:
				if actual, ok := value.(bool); !ok || actual != expected {
					t.Errorf("预期 %v，得到 %v", expected, value)
				}
			}
			// 其他类型（如 slice、map、struct）需要更复杂的比较，
			// 在这里我们只检查它们是否被正确存储和检索
		})
	}
}

// 测试缓存关闭功能
func TestCacheClose(t *testing.T) {
	// 创建带有清理间隔的缓存
	cache := NewCache(DefaultExpiration, 1*time.Minute)

	// 关闭缓存
	err := cache.Close()
	if err != nil {
		t.Errorf("关闭缓存时出错: %v", err)
	}

	// 测试关闭后的操作仍然有效
	cache.Set("key", "value", DefaultExpiration)
	value, found := cache.Get("key")
	if !found {
		t.Error("关闭后应仍能使用缓存")
	}
	if value != "value" {
		t.Errorf("预期 'value'，得到 %v", value)
	}

	// 关闭没有清理间隔的缓存（不应该有副作用）
	cache2 := NewCache(DefaultExpiration, 0)
	err = cache2.Close()
	if err != nil {
		t.Errorf("关闭无清理间隔的缓存时出错: %v", err)
	}
}
