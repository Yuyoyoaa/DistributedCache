package config

import (
	"encoding/json"
	"os"
)

// ServerConfig 定义服务端配置结构
// 采用扁平化结构，直接对应 main.go 中的 cfg.Addr, cfg.EtcdAddrs 等字段调用
type ServerConfig struct {
	Addr        string   `json:"addr"`         // 本节点监听地址，例如 "localhost:8001"
	EtcdAddrs   []string `json:"etcd_addrs"`   // Etcd 集群地址列表
	CacheSize   int64    `json:"cache_size"`   // 最大缓存大小 (bytes)
	CachePolicy string   `json:"cache_policy"` // 淘汰策略: "lru", "lfu", "fifo"
	ServiceName string   `json:"service_name"` // 服务注册前缀，例如 "distributed-cache/nodes"
}

// DefaultConfig 返回默认配置
// 当配置文件不存在或加载失败时，main.go 会使用此默认值
func DefaultConfig() *ServerConfig {
	return &ServerConfig{
		Addr:        "localhost:8001",
		EtcdAddrs:   []string{"localhost:2379"},
		CacheSize:   2 << 20, // 默认 2MB
		CachePolicy: "lru",
		ServiceName: "distributed-cache/nodes",
	}
}

// LoadConfig 从指定路径加载 JSON 配置文件
func LoadConfig(path string) (*ServerConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		// 返回错误，由调用方(main.go)决定是否使用默认配置
		return nil, err
	}
	defer file.Close()

	cfg := &ServerConfig{}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
