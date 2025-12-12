package fifo

import (
	"DistributedCache/internal/core/policy"
	"testing"
)

// ---- mock value type for testing ----
// 项目中的 Value 要求实现 Len() int，因此这里用简单的 String 来 mock
type String string

func (s String) Len() int {
	return len(s)
}

// 基础测试：Add + Get
func TestCacheGet(t *testing.T) {
	fifo := New(0, nil)

	fifo.Add("key1", String("12345"))

	value, ok := fifo.Get("key1")
	if !ok {
		t.Fatalf("expected key1 exists")
	}
	if string(value.(String)) != "12345" {
		t.Fatalf("expected 12345, but got %v", value)
	}

	if _, ok := fifo.Get("not_exist"); ok {
		t.Fatalf("expected miss for not_exist")
	}
}

// 更新 key 时是否替换 value + nbytes 是否正确更新
func TestCacheUpdate(t *testing.T) {
	fifo := New(0, nil)

	fifo.Add("k", String("111"))
	if fifo.nbytes != int64(len("k")+len("111")) {
		t.Fatalf("unexpected nbytes, got %d", fifo.nbytes)
	}

	fifo.Add("k", String("22"))
	if fifo.nbytes != int64(len("k")+len("22")) { // 根据更新计算
		t.Fatalf("update nbytes mismatch, got %d", fifo.nbytes)
	}

	val, _ := fifo.Get("k")
	if string(val.(String)) != "22" {
		t.Fatalf("update value mismatch, got %v", val)
	}
}

// 测试 maxBytes 控制导致的淘汰（RemoveOldest）
func TestCacheEviction(t *testing.T) {
	evicted := make([]string, 0)

	// 注册回调函数来监测是否发生淘汰
	onEvicted := func(key string, value policy.Value) {
		evicted = append(evicted, key)
	}

	// 只允许存下 key1 和 key2 的空间
	capacity := int64(len("key1") + len("111") + len("key2") + len("222"))
	fifo := New(capacity, onEvicted)

	// add key1, key2
	fifo.Add("key1", String("111"))
	fifo.Add("key2", String("222"))

	// 添加 key3 会导致 key1 被淘汰（最先加入）
	fifo.Add("key3", String("333"))

	if _, ok := fifo.Get("key1"); ok {
		t.Fatalf("expected key1 to be evicted")
	}

	if len(evicted) == 0 || evicted[0] != "key1" {
		t.Fatalf("onEvicted not triggered or wrong key, got %v", evicted)
	}
}

// 测试 Remove(key)
func TestCacheRemove(t *testing.T) {
	lru := New(0, nil)

	lru.Add("a", String("1"))
	lru.Add("b", String("2"))

	lru.Remove("a")

	if _, ok := lru.Get("a"); ok {
		t.Fatalf("expected a to be removed")
	}

	if lru.Len() != 1 {
		t.Fatalf("expected cache len = 1, got %d", lru.Len())
	}
}

// ---- Test: Update does not change order ----
func TestUpdateDoesNotMove(t *testing.T) {
	// 设置刚好能容纳3个元素的容量
	f := New(9, nil) // 每个元素: 2(key) + 1(value) = 3字节 × 3个 = 9字节

	f.Add("k1", String("a")) // 2+1=3字节
	f.Add("k2", String("b")) // 2+1=3字节，总6
	f.Add("k3", String("c")) // 2+1=3字节，总9

	// 记录淘汰顺序
	var evictedOrder []string
	f.OnEvicted = func(key string, value policy.Value) {
		evictedOrder = append(evictedOrder, key)
	}

	// 更新k2，不应该改变它在队列中的位置
	f.Add("k2", String("b")) // 大小不变，只是更新

	// 添加第4个元素，应该淘汰k1（最早加入的）
	f.Add("k4", String("d")) // 触发淘汰

	// 验证淘汰顺序
	if len(evictedOrder) != 1 || evictedOrder[0] != "k1" {
		t.Fatalf("expected k1 evicted first, got %v", evictedOrder)
	}

	// 验证k2仍然存在（证明更新没有改变位置）
	if _, ok := f.Get("k2"); !ok {
		t.Fatal("k2 should still exist after update")
	}
}
