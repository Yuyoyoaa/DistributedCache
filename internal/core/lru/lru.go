package lru

import (
	"DistributedCache/internal/core/policy"
	"container/list"
)

// Cache 是一个LRU 缓存
type Cache struct {
	maxBytes  int64                                // 允许使用的最大内存
	nbytes    int64                                // 当前已使用的内存
	ll        *list.List                           // 双向链表
	cache     map[string]*list.Element             //键映射到链表节点(list.Element:next, prev *Element,Value)
	OnEvicted func(key string, value policy.Value) // 某条记录被移除时的回调,有些缓存值可能持有资源（文件、连接等），需要正确释放
}

// 链表存储的是 *entry（list.Element.Value），在淘汰最旧数据时，需要知道要删除的key
type entry struct {
	key   string
	value policy.Value
}

// New 创建一个新的LRU Cache
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
		c.ll.MoveToFront(ele)    // LRU 核心：访问即移动到队头
		kv := ele.Value.(*entry) // ele.Value里面存储的是一个 *entry类型（entry指针）
		return kv.value, true
	}
	return
}

func (c *Cache) Add(key string, val policy.Value) {
	if ele, exist := c.cache[key]; exist {
		// 更新现有节点
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.nbytes = c.nbytes - int64(kv.value.Len()) + int64(val.Len())
		kv.value = val
	} else {
		// 新增节点
		ele := c.ll.PushFront(&entry{key, val}) // 1.创建entry实例并取地址 2.将entry插入链表头部 3.返回新创建的 *list.Element（链表节点）
		c.cache[key] = ele
		c.nbytes = c.nbytes + int64(val.Len()) + int64(len(key))
	}

	// 内存超限，淘汰队尾
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
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
		c.removeElement(ele)
	}
}

func (c *Cache) removeElement(ele *list.Element) {
	c.ll.Remove(ele)
	kv := ele.Value.(*entry)
	delete(c.cache, kv.key)
	c.nbytes = c.nbytes - int64(len(kv.key)) - int64(kv.value.Len())
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

func (c *Cache) Len() int {
	return c.ll.Len()
}
