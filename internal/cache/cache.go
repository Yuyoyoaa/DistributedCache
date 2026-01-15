package cache

// 本地内存 + 并发控制
import (
	"DistributedCache/internal/byteview"
	"DistributedCache/internal/core/fifo"
	"DistributedCache/internal/core/lfu"
	"DistributedCache/internal/core/lru"
	"DistributedCache/internal/core/policy"
	"sync"
)

// cache 是对底层缓存策略算法的并发安全封装
// 它不应该被外部直接使用，而是作为 Group 的组件
// 开头大写就是可导出（公有）,小写是不可导出
type Cache struct {
	mu         sync.RWMutex          // 读写锁:允许多个读，但写操作互斥
	policy     policy.EvictionPolicy // 核心接口，支持多态
	cacheBytes int64                 // 允许使用的最大内存
	policyType string                // 策略类型: "lru", "fifo", "lfu"
	onEvicted  func(string, policy.Value)
}

// newCache 创建一个新的并发缓存实例
func NewCache(cacheBytes int64, policyType string, onEvicted func(string, policy.Value)) *Cache {
	return &Cache{
		cacheBytes: cacheBytes,
		policyType: policyType,
		onEvicted:  onEvicted,
	}
}

// add 写入缓存（并发安全）
func (c *Cache) Add(key string, val byteview.Byteview) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 懒加载：只有在第一次 add 时才初始化底层数据结构
	// 这样可以避免创建了 Group 但还没存数据时就占用了内存
	if c.policy == nil {
		c.policy = createPolicy(c.policyType, c.cacheBytes, c.onEvicted)
	}
	c.policy.Add(key, val)
}

// get 读取缓存（并发安全）
func (c *Cache) Get(key string) (value byteview.Byteview, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.policy == nil {
		return
	}

	if v, exist := c.policy.Get(key); exist {
		// 这里的 v 是 policy.Value 接口，需要断言回 byteview.ByteView
		return v.(byteview.Byteview), true
	}
	return
}

// createPolicy 工厂方法：根据字符串名称实例化对应的淘汰策略
func createPolicy(policyType string, maxBytes int64, onEvicted func(string, policy.Value)) policy.EvictionPolicy {
	switch policyType {
	case "fifo":
		return fifo.New(maxBytes, onEvicted)
	case "lfu":
		return lfu.New(maxBytes, onEvicted)
	case "lru":
		// 默认使用 LRU
		return lru.New(maxBytes, onEvicted)
	default:
		return lru.New(maxBytes, onEvicted)
	}
}
