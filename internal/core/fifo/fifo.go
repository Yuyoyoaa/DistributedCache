package fifo

import (
	"DistributedCache/internal/core/policy"
	"container/list"
)

// Cache 是一个FIFO 缓存
// cspell:ignore nbytes
type Cache struct {
	maxBytes  int64                                // 允许使用的最大内存
	nbytes    int64                                // 当前已使用的内存
	ll        *list.List                           // 双向链表
	cache     map[string]*list.Element             //键映射到链表节点(list.Element:next, prev *Element,Value)
	OnEvicted func(key string, value policy.Value) // 某条记录被移除时的回调,有些缓存值可能持有资源（文件、连接等），需要正确释放
}

type entry struct {
	key   string
	value policy.Value
}

func New(maxBytes int64, onEvicted func(string, policy.Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

func (c *Cache) Get(key string) (val policy.Value, ok bool) {
	if ele, exist := c.cache[key]; exist {
		// FIFO 这里不需要 MoveToFront，因为访问不改变“先入”的顺序
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return
}

func (c *Cache) Add(key string, val policy.Value) {
	if ele, exist := c.cache[key]; exist {
		// 更新值，位置不变（或者也可以看作新插入移到队尾，取决于具体定义，通常 FIFO 更新不改变淘汰序）
		kv := ele.Value.(*entry)
		c.nbytes = c.nbytes - int64(kv.value.Len()) + int64(val.Len())
		kv.value = val
	} else {
		ele := c.ll.PushBack(&entry{key, val}) //新增放队尾
		c.cache[key] = ele
		c.nbytes = c.nbytes + int64(val.Len()) + int64(len(key))
	}

	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

func (c *Cache) RemoveOldest() {
	ele := c.ll.Front()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes = c.nbytes - int64(len(kv.key)) - int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

func (c *Cache) Remove(key string) {
	if ele, exist := c.cache[key]; exist {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes = c.nbytes - int64(len(kv.key)) - int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

func (c *Cache) Len() int {
	return c.ll.Len()
}
