package lru

//  Go 测试框架会为每个测试函数创建新的测试环境
import (
	"DistributedCache/internal/core/policy"
	"testing"
)

type String string

// ---- mock value type for testing ----
// cspell:ignore nbytes
// 项目中的 Value 要求实现 Len() int，因此这里用简单的 String 来 mock
func (s String) Len() int {
	return len(s)
}

// 基础测试：Add + Get
func TestGet(t *testing.T) {
	lru := New(int64(0), nil) // 当 maxBytes = 0时，意味着缓存没有容量限制
	lru.Add("key1", String("1234"))

	val, ok := lru.Get("key1")
	if !ok {
		t.Fatalf("expected key1 exists")
	}
	if string(val.(String)) != "1234" {
		t.Fatalf("expected 1234,but got %v", val)
	}

	if _, ok := lru.Get("not_exist"); ok {
		t.Fatalf("expected miss for not_exist")
	}
}

// 更新 key 时是否替换 value + nbytes 是否正确更新
func TestUpdate(t *testing.T) {
	lru := New(int64(0), nil)

	lru.Add("key1", String("111"))
	if lru.nbytes != int64(len("key1")+len("111")) {
		t.Fatalf("unexpected nbytes, got %d", lru.nbytes)
	}

	lru.Add("key1", String("22"))
	if lru.nbytes != int64(len("key1")+len("22")) { // 根据更新计算
		t.Fatalf("update nbytes mismatch, got %d", lru.nbytes)
	}

	val, _ := lru.Get("key1")
	if string(val.(String)) != "22" {
		t.Fatalf("update value mismatch, got %v", val)
	}
}

// 测试 maxBytes 控制导致的淘汰（RemoveOldest）
func TestEviction(t *testing.T) {
	evicted := make([]string, 0)

	// 注册回调函数来监控是否发生淘汰
	onEvicted := func(key string, value policy.Value) {
		evicted = append(evicted, key) // 记录移除的key
	}

	// 只允许存下 key1 和 key2 的空间
	capacity := int64(len("key1") + len("111") + len("key2") + len("222"))
	lru := New(capacity, onEvicted)

	// add key1,key2
	lru.Add("key1", String("111"))
	lru.Add("key2", String("222"))

	// 添加 key3 会导致 key1 被淘汰（最久未使用）
	lru.Add("key3", String("333"))

	if _, ok := lru.Get("key1"); ok {
		t.Fatalf("expected key1 to be evicted")
	}

	if len(evicted) == 0 || evicted[0] != "key1" {
		t.Fatalf("onEvicted not triggered or wrong key, got %v", evicted)
	}
}

// 测试Remove(key)
func TestRemove(t *testing.T) {
	lru := New(int64(0), nil)

	lru.Add("key1", String("111"))
	lru.Add("key2", String("222"))

	lru.Remove("key1")

	if _, ok := lru.Get("key1"); ok {
		t.Fatalf("expected a to be removed")
	}

	if lru.Len() != 1 {
		t.Fatalf("expected cache len = 1, got %d", lru.Len())
	}
}

// Get 应移动元素到队头，确保 LRU 的最旧元素正确被淘汰
func TestGetMoveToFront(t *testing.T) {
	lru := New(int64(1000), nil)

	lru.Add("a", String("1"))
	lru.Add("b", String("2"))
	lru.Add("c", String("3"))

	// 访问 a -> a 变成最新使用
	lru.Get("a")

	// 下一次插入导致 b 被淘汰（因为b成为最久未使用）
	// 保证只能容纳 b,c 或 a,c
	lru.maxBytes = int64(len("b") + len("2") + len("c") + len("3"))

	lru.Add("d", String("4")) // 会淘汰 b

	if _, ok := lru.Get("b"); ok {
		t.Fatalf("expected b to be evicted")
	}
}

// 测试 nbytes 累加/减少是否正确
func TestNbytes(t *testing.T) {
	lru := New(0, nil)

	lru.Add("key1", String("111"))
	expected := int64(len("key1") + len("111"))
	if lru.nbytes != expected {
		t.Fatalf("nbytes incorrect, want %d got %d", expected, lru.nbytes)
	}

	// 删除key1
	lru.Remove("key1")
	if lru.nbytes != 0 {
		t.Fatalf("nbytes should be 0 after removal, got %d", lru.nbytes)
	}
}
