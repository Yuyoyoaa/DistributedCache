package policy

// Value 是缓存中存储的值必须实现的接口
// Len 返回该值占用的内存大小（字节数）

// 一个名为 Value的接口类型，这个接口只要求实现一个方法：Len() int
// 任何实现了 Len() int方法的类型都可以被当作 Value接口使用
type Value interface {
	Len() int
}

// EvictionPolicy 定义了淘汰策略的通用接口
type EvictionPolicy interface {
	// Get 获取缓存值
	Get(key string) (Value, bool)

	// Add 添加或更新缓存值
	Add(key string, value Value)

	// Remove 移除指定key
	Remove(key string)

	// Len  返回当前缓存的条目数
	Len() int
}
