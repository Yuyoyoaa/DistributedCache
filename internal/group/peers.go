package group

import pb "DistributedCache/api/cachepb"

// 分布式缓存节点通信的接口抽象层

// PeerPicker  是必须实现的接口，用于根据 Key 选择对应的节点（基于一致性哈希）
type PeerPicker interface {
	// PickPeer 根据 key 选择节点
	// 返回 peer 节点抽象（用于通信）和 true（如果找到了非本地节点）
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter 是节点客户端必须实现的接口
// 对应于 HTTP 客户端或 gRPC 客户端
// 用于从那个节点获取数据（基于 Protobuf）
type PeerGetter interface {
	// Get 从对应组查找缓存值
	Get(in *pb.GetRequest, out *pb.GetResponse) error
}
