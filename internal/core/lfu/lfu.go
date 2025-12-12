package lfu

import (
	"DistributedCache/internal/core/policy"
	"container/heap"
)

// 最小堆的排序从队头到队尾是从小到大的
// cspell:ignore nbytes
type Cache struct {
	maxBytes  int64
	nbytes    int64
	pq        *priorityQueue // 优先队列（最小堆），按频率排序
	cache     map[string]*entry
	seq       int64                                // 全局递增序号，模拟时间
	OnEvicted func(key string, value policy.Value) // 淘汰回调函数
}

type entry struct {
	key   string
	value policy.Value
	freq  int   // 访问频率
	index int   // 在堆的索引
	ts    int64 // 时间戳/序号，用于频率相等时按访问时间淘汰
}

func New(maxBytes int64, onEvicted func(string, policy.Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		pq:        &priorityQueue{},
		cache:     map[string]*entry{},
		OnEvicted: onEvicted,
	}
}

func (c *Cache) Get(key string) (val policy.Value, ok bool) {
	if ele, exist := c.cache[key]; exist {
		c.update(ele, ele.value) // 增加频率，更新时间
		return ele.value, true
	}
	return
}

func (c *Cache) Add(key string, val policy.Value) {
	if ele, exist := c.cache[key]; exist {
		c.nbytes = c.nbytes - int64(ele.value.Len()) + int64(val.Len())
		c.update(ele, val)
	} else {
		c.seq++                                                 // 新增也算一次访问，序号增加
		ele := &entry{key: key, value: val, freq: 1, ts: c.seq} // 新增元素频率通常初始化为1
		c.cache[key] = ele
		heap.Push(c.pq, ele)
		c.nbytes = c.nbytes + int64(len(key)) + int64(val.Len())
	}

	// 循环淘汰直到容量满足
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

// update 内部帮助函数：更新值、增加频率、更新时间戳、调整堆
func (c *Cache) update(ele *entry, val policy.Value) {
	ele.value = val
	ele.freq++
	c.seq++
	ele.ts = c.seq // 更新为最新操作序号
	heap.Fix(c.pq, ele.index)
}

func (c *Cache) RemoveOldest() {
	if c.pq.Len() == 0 {
		return
	}
	ele := heap.Pop(c.pq).(*entry) // 弹出频率最低的
	delete(c.cache, ele.key)
	c.nbytes = c.nbytes - int64(len(ele.key)) - int64(ele.value.Len())
	if c.OnEvicted != nil {
		c.OnEvicted(ele.key, ele.value)
	}
}

func (c *Cache) Remove(key string) {
	if ele, exist := c.cache[key]; exist {
		heap.Remove(c.pq, ele.index)
		delete(c.cache, ele.key)
		c.nbytes = c.nbytes - int64(len(ele.key)) - int64(ele.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(ele.key, ele.value)
		}
	}
}

func (c *Cache) Len() int {
	return c.pq.Len()
}

// priorityQueue 实现 heap.Interface
type priorityQueue []*entry

func (pq priorityQueue) Len() int {
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].freq < pq[j].freq
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = j
	pq[j].index = i
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*entry)
	item.index = n
	*pq = append(*pq, item)
}

// 当调用 heap.Pop(pq)时，heap.Pop()会先将堆顶元素（频率最小）与最后一个元素交换
// 然后调用 pq.Pop() 取出最后一个元素
// 重新堆化，维护堆性质
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*pq = old[:n-1]
	return item
}
