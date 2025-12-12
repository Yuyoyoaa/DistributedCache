package lfu

import (
	"DistributedCache/internal/core/policy"
	"testing"
)

// mock Value
type String string

func (s String) Len() int { return len(s) }

// ---- Test: Get ----
func TestGet(t *testing.T) {
	lfu := New(0, nil)
	lfu.Add("a", String("111"))

	val, ok := lfu.Get("a")
	if !ok || string(val.(String)) != "111" {
		t.Fatalf("expected a=111, got %v", val)
	}

	if _, ok := lfu.Get("not_found"); ok {
		t.Fatalf("expected miss for not_found")
	}
}

// ---- Test: LFU eviction ----
func TestLFUEviction(t *testing.T) {
	var evictedKey string
	var evictedVal policy.Value

	// 容量设为 20，防止添加 k3 时因容量过小导致 k3 也被连带淘汰
	// k1=6, k2=6, k3=10. 总共 22. 22-6(k2) = 16 <= 20. 安全。
	lfu := New(20, func(key string, value policy.Value) {
		evictedKey = key
		evictedVal = value
	})

	lfu.Add("k1", String("1111")) // freq=1, ts=1
	lfu.Add("k2", String("2222")) // freq=1, ts=2

	// 访问 k1，使其频率增加，并且更新时间戳
	lfu.Get("k1") // freq(k1)=2, ts=3. 此时 k1 比 k2 频率高

	// k2: freq=1
	// k1: freq=2

	// 添加 k3，触发淘汰
	lfu.Add("k3", String("33333333")) // freq=1, ts=4

	// 此时堆中：k2(freq=1), k3(freq=1), k1(freq=2)
	// k2 和 k3 频率相等，但 k2 的 ts(2) < k3 的 ts(4)，所以 k2 更旧，应淘汰 k2。

	if evictedKey != "k2" {
		t.Fatalf("expected k2 evicted, but got %s", evictedKey)
	}
	if evictedVal != String("2222") {
		t.Fatalf("expected 2222, but got %v", evictedVal)
	}
}

// ---- Test: Update value should keep frequency but increase freq by 1 ----
func TestUpdateValue(t *testing.T) {
	l := New(100, nil)

	l.Add("k1", String("abc")) // freq=1
	l.Add("k1", String("xyz")) // freq=2 now

	if l.cache["k1"].freq != 2 {
		t.Fatalf("expected freq=1, got %d", l.cache["k1"].freq)
	}
}

// ---- Test: Remove ----
func TestRemove(t *testing.T) {
	l := New(100, nil)

	l.Add("k1", String("1"))
	l.Add("k2", String("2"))

	l.Remove("k1")

	if _, ok := l.Get("k1"); ok {
		t.Fatalf("expected k1 removed")
	}

	if l.Len() != 1 {
		t.Fatalf("expected len=1, got %d", l.Len())
	}
}

// ---- Test: LFU ordering: lowest frequency first ----
func TestLFUFrequencyOrder(t *testing.T) {
	evicted := ""

	l := New(4, func(key string, value policy.Value) {
		evicted = key
	})

	l.Add("a", String("1")) // freq=1
	l.Add("b", String("2")) // freq=1

	// Increase freq of b
	l.Get("b") // freq=2

	l.Add("c", String("3")) // force eviction

	if evicted != "a" {
		t.Fatalf("expected a (lowest freq) evicted, got %s", evicted)
	}
}
