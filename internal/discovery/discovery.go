package discovery

import (
	"context"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Discovery 负责发现服务
// 功能：监听 etcd 中特定前缀的服务节点变化，维护实时节点列表
type Discovery struct {
	cli       *clientv3.Client
	prefix    string   //监听的服务前缀，如 "distributed-cache/nodes"
	serverMap sync.Map // 线程安全的节点映射表，存储当前在线的节点地址 // 使用 sync.Map 替代 map[string]struct{} 以获得更好的并发性能
}

// NewDiscovery 创建一个发现器
func NewDiscovery(endpoints []string) (*Discovery, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &Discovery{
		cli: cli,
	}, nil
}

// WatchService 开始监听服务节点变化
// prefix:监听的前缀，例如 "distributed-cache/nodes"
// updatePeers: 回调函数，当节点列表变化时被调用，参数是新的节点地址列表
// 工作流程:
//  1. 获取当前已存在的节点（初始化）
//  2. 开启 Watch 协程监听后续变化
func (d *Discovery) WatchService(prefix string, updatePeers func([]string)) error {
	d.prefix = prefix

	// 1. 初始同步：获取当前 etcd 中已存在的所有节点
	// WithPrefix() 表示获取所有以 prefix 开头的键
	resp, err := d.cli.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	var nodes []string
	// 遍历所有找到的键值对
	for _, kv := range resp.Kvs {
		node := string(kv.Value) // kv.Value 存储的是节点地址，如 "localhost:8001"
		// sync.Map.Store 是线程安全的存储操作
		// 将节点添加到 serverMap 中
		d.serverMap.Store(node, struct{}{})
		nodes = append(nodes, node)
	}

	// 初始化 Hash 环（由调用方 updatePeers 实现）
	if len(nodes) > 0 {
		updatePeers(nodes)
		log.Printf("[Discovery] Initial nodes found: %v", nodes)
	}

	// 2. 开启 Watch 协程监听后续变化
	// 这个协程会一直运行，直到 etcd 连接关闭
	go d.wacher(prefix, updatePeers)
	return nil
}

// watcher 监听 etcd 键空间变化
// 这是服务发现的核心逻辑，通过 etcd 的 Watch 机制实时感知节点上下线
// prefix: 监听的前缀
// updatePeers: 节点变化时的回调函数
func (d *Discovery) wacher(prefix string, updatePeers func([]string)) {
	// 创建 Watch 通道
	// WithPrefix() 表示监听所有以 prefix 开头的键的变化
	rch := d.cli.Watch(context.Background(), prefix, clientv3.WithPrefix())
	log.Printf("[Discovery] Watching prefix: %s", prefix)

	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut: // 新增或修改
				node := string(ev.Kv.Value) // 从 Value 获取节点地址
				d.serverMap.Store(node, struct{}{})
				log.Printf("[Discovery] Node added: %s", node)
			case clientv3.EventTypeDelete: // 删除
				// key 是 serviceName/nodeAddr，我们需要从 Key 中提取或者直接存 Value 即可
				// 在 Register 中我们 Key 包含了地址，但 Value 也是地址，这里依赖 Value 比较方便
				// 注意：删除事件可能没有 Value，需要根据 Key 解析，或者依赖之前的逻辑
				key := string(ev.Kv.Key)
				node := key[len(prefix)+1:] // +1 是为了去掉分隔符 "/"
				d.serverMap.Delete(node)
				log.Printf("[Discovery] Node removed: %s", node)
			}
		}

		// 每次事件发生后，重建节点列表并通知回调
		var nodes []string
		d.serverMap.Range(func(key, value interface{}) bool {
			nodes = append(nodes, key.(string))
			return true
		})
		updatePeers(nodes)
	}
}

// Stop 关闭连接
func (d *Discovery) Stop() error {
	return d.cli.Close()
}
