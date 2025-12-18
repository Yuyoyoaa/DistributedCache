package group

import (
	pb "DistributedCache/api/cachepb"
	"DistributedCache/internal/byteview"
	"DistributedCache/internal/cache"
	"DistributedCache/internal/singleflight"
	"fmt"
	"log"
	"sync"
)

// Getter 定义了当缓存不存在时，如何从数据源（DB/文件）获取数据的接口
type Getter interface {
	Get(key string) ([]byte, error)
}

// 定义函数类型，实现Getter接口
type GetterFunc func(key string) ([]byte, error)

// GetterFunc类型实现Getter接口的Get方法
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// Group 是分布式缓存的核心结构
type Group struct {
	name      string              //缓存空间名称，例如 "scores", "users"
	getter    Getter              //缓存未命中时的回源回调
	mainCache *cache.Cache        //本地并发缓存 (封装了 LRU/LFU 等)
	peers     PeerPicker          // 节点选择器
	loader    *singleflight.Group // 防止缓存击穿
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

// NewGroup 创建一个新的缓存组
// cacheBytes："缓存最大字节数
// policyType："lru","fifo","lfu"
func NewGroup(name string, cacheBytes int64, policyType string, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache.NewCache(cacheBytes, policyType, nil),
		loader:    &singleflight.Group{},
	}
	groups[name] = g
	return g
}

// GetGroup 返回指定名称的Group
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// RegisterPeers 注册节点选择器
// 这将允许 Group 选择远程节点
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
}

// Get获取缓存值
// 流程：本地缓存 -> (如果没命中) -> 选节点 -> (如果是远程) RPC获取 -> (如果是本地) 回源获取
func (g *Group) Get(key string) (byteview.Byteview, error) {
	if key == "" {
		return byteview.Byteview{}, fmt.Errorf("key is required")
	}

	// 1.查本地缓存
	if v, ok := g.mainCache.Get(key); ok {
		log.Println("[Cache] Hit local")
		return v, nil
	}

	// 2.缓存为命中，调用load（包含远程加载和本地回源）
	return g.load(key)
}

// load 加载数据，使用 singleflight 确保并发场景下只有一个请求去加载
func (g *Group) load(key string) (value byteview.Byteview, err error) {
	// 使用 singleflight 包装
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		// 内部加载逻辑
		if g.peers != nil {
			// 使用一致性哈希选择节点
			if peer, ok := g.peers.PickPeer(key); ok {
				// 如果选中的不是自己，尝试从远程获取
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[Cache] Failed to get from peer", err)
			}
		}
		// 如果没有远程节点，或者选中的是自己，获取远程获取失败，则回源
		return g.getLocally(key)
	})

	if err == nil {
		return viewi.(byteview.Byteview), nil
	}
	return
}

// getLocally 调用用户回调函数g.getter.Get()获取源数据
func (g *Group) getLocally(key string) (byteview.Byteview, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return byteview.Byteview{}, err
	}
	value := byteview.New(bytes)

	// 将源数据填充到本地缓存
	g.populateCache(key, value)
	return value, nil
}

// getFromPeer 通过 RPC 访问远程节点
func (g *Group) getFromPeer(peer PeerGetter, key string) (byteview.Byteview, error) {
	// 1. 准备请求：告诉远程节点我要什么
	req := &pb.GetRequest{
		Group: g.name, // 缓存空间
		Key:   key,    // 具体key
	}
	// 2. 远程调用
	res := &pb.GetResponse{}
	err := peer.Get(req, res) // 网络请求！
	if err != nil {
		return byteview.Byteview{}, err
	}

	// 远程获取的数据
	value := byteview.New(res.Value)
	// 【关键点 - 热点互备】
	// 如果这里我们把远程获取到的数据写入本地缓存：
	// 优点：下次请求直接命中本地，不再走网络，极大缓解热点 Key 对 Owner 节点的冲击。
	// 缺点：数据一致性稍差，占用本地内存。
	g.populateCache(key, value)
	return value, nil
}

// populateCache 将数据添加到本地缓存
func (g *Group) populateCache(key string, value byteview.Byteview) {
	g.mainCache.Add(key, value)
}
