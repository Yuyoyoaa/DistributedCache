package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash 定义哈希函数的函数签名
// 默认为crc32.ChecksumIEEE
type Hash func(data []byte) uint32

// Map 是一致性哈希的主数据结构
type Map struct {
	hash     Hash           // 哈希函数
	replicas int            // 虚拟节点倍数
	keys     []int          // 哈希环（有序的虚拟节点哈希值列表）
	hashMap  map[int]string //虚拟节点哈希值 -> 真实节点名称的映射
}

// New 创建一个新的 Map 实例
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 添加真实节点到哈希环
// keys 是真实节点的名称，例如 "10.0.0.1:8080", "10.0.0.2:8080"
// ...string 等价于 []string
// cluster.Add("server1:8080", "server2:8080", "server3:8080")
// 如果为 func (m *Map) Add(keys []string)，则cluster.Add([]string{"server1:8080", "server2:8080", "server3:8080"})
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		// 为每个真实节点创建m.replicas 个虚拟节点
		for i := 0; i < m.replicas; i++ {
			// 虚拟节点的名称规则为：strconv.Ttoa(i)+key
			//  例如：010.0.0.1:8080, 110.0.0.1:8080 ...
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))

			// 将虚拟节点的哈希值添加到哈希环中
			m.keys = append(m.keys, hash)

			// 记录虚拟节点哈希值与真实节点名称的映射关系：虚拟节点->真实节点
			m.hashMap[hash] = key
		}
	}
	// 对环上的哈希值进行排序，以便进行二分查找
	sort.Ints(m.keys)
}

// Get 根据 key 选择最近的节点
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}

	// 计算 key 的哈希值
	hash := int(m.hash([]byte(key)))

	// 二分查找：找到第一个匹配的虚拟节点的下标
	// sort.Search 返回满足 func(i) 为 true 的最小 i
	// 这里即找到第一个 >= hash 的虚拟节点
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	// 如果 idx == len(m.keys)，说明没有找到 >= hash 的节点
	// 根据环形结构，应该映射到第一个节点（m.keys[0]）
	// 所以这里取模操作处理了回环的情况
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
