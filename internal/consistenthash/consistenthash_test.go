package consistenthash

import (
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {
	// 自定义哈希函数，方便测试结果可预测
	// 这里简单的将数字字符串转换为整数作为哈希值
	hash := New(3, func(key []byte) uint32 {
		i, _ := strconv.Atoi(string(key))
		return uint32(i)
	})

	// 添加节点 "2", "4", "6"
	// 虚拟节点会有:
	// "2" -> 02, 12, 22
	// "4" -> 04, 14, 24
	// "6" -> 06, 16, 26
	hash.Add("6", "4", "2")

	testCases := map[string]string{
		"2":  "2", // 2 的哈希是 2，匹配虚拟节点 02，真实节点 "2"
		"11": "2", // 11 的最近节点是 12 (虚拟节点 12 对应真实节点 "2")
		"23": "4", // 23 的最近节点是 24 (虚拟节点 24 对应真实节点 "4")
		"27": "2", // 27 大于所有节点，回环到 02 (真实节点 "2")
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s, but got %s", k, v, hash.Get(k))
		}
	}

	// 添加节点 "8"
	// 虚拟节点: 08, 18, 28
	hash.Add("8")

	// 27 本来是 "2"，现在有了 28，应该映射到 "8"
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s, but got %s", k, v, hash.Get(k))
		}
	}
}
