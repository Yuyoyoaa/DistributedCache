package cache

import (
	"DistributedCache/internal/byteview"
	"DistributedCache/internal/core/policy"
	"testing"
)

func TestCacheBasic(t *testing.T) {
	c := NewCache(100, "lru", nil)

	v1 := byteview.New([]byte("hello"))
	c.Add("k1", v1)

	val, ok := c.Get("k1")
	if !ok {
		t.Fatalf("expected k1 exist")
	}
	if string(val.ByteSlice()) != "hello" {
		t.Fatalf("unexpected value: %s", val.ByteSlice())
	}
}

// 测试淘汰策略是否被调用
func TestCacheEviction(t *testing.T) {
	evicted := ""
	onEvicted := func(key string, value policy.Value) {
		evicted = key
	}

	c := NewCache(10, "lru", onEvicted)

	c.Add("k1", byteview.New([]byte("12345")))
	c.Add("k2", byteview.New([]byte("12345"))) // 超过容量触发淘汰

	if evicted == "" {
		t.Fatalf("Eviction expected but did not happen")
	}
}
