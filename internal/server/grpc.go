package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "DistributedCache/api/cachepb"
	"DistributedCache/internal/consistenthash"
	"DistributedCache/internal/group"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server 模块扮演三个角色：
// 1. gRPC 服务器：接收其他节点或客户端的请求
// 2. 节点选择器（PeerPicker）：基于一致性哈希为 key 选择远程节点
// 3. 客户端管理：为每个远程节点维护一个 grpcGetter（实现 PeerGetter）
// 每个节点都有自己的 Server 实例
type Server struct {
	pb.UnimplementedGroupCacheServer                        // 必须嵌入，以兼容 gRPC 接口
	self                             string                 // 当前节点地址，例如”localhost:8001“
	mu                               sync.Mutex             // 保护 peers 和 peersGetter
	peers                            *consistenthash.Map    // 一致性哈希环
	peersGetter                      map[string]*grpcGetter // 映射：远程节点地址 -> gRPC 客户端
}

func NewServer(self string) *Server {
	return &Server{
		self:        self,
		peers:       consistenthash.New(50, nil),
		peersGetter: make(map[string]*grpcGetter),
	}
}

// Start 启动 gRPC 服务端监听
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.self) // 在 s.self指定的地址上进行 TCP 监听
	if err != nil {
		return fmt.Errorf("failed to listen at %s: %v", s.self, err)
	}
	grpcServer := grpc.NewServer()             // 调用 grpc 包的 NewServer 函数
	pb.RegisterGroupCacheServer(grpcServer, s) // 把缓存服务挂到 gRPC 服务器上，让外部可以调用

	s.Log("gRPC server listening at %s", s.self)
	return grpcServer.Serve(lis)
}

// Get 处理来自其他节点或客户端的gRPC Get请求
// 这是Server的Get（接电话），调用时机​：其他节点/客户端请求数据时
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	groupName := req.GetGroup()
	key := req.GetKey()

	if groupName == "" || key == "" {
		return nil, fmt.Errorf("invalid request: group or key is empty")
	}

	// 找到对应的 Group
	g := group.GetGroup(groupName)
	if g == nil {
		return nil, fmt.Errorf("no such group: %s", groupName)
	}

	// 调用 Group.Get 获取数据 (内部可能触发回源或远程拉取）
	view, err := g.Get(key)
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: view.ByteSlice()}, nil
}

// SetPeers 设置（或更新）哈希环上的节点列表
func (s *Server) SetPeers(peers ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 重建哈希环
	s.peers = consistenthash.New(50, nil)
	s.peers.Add(peers...) // 将所有节点添加到哈希环中

	// 重建远程 Getter 映射(创建新的 map来存储每个节点的客户端连接)
	// 键：节点地址（如 "192.168.1.1:8000"）
	// 值：grpcGetter对象，用于向该节点发起RPC调用
	// 为每个真实节点配置一个gRPC客户端
	s.peersGetter = make(map[string]*grpcGetter, len(peers))
	for _, peer := range peers {
		s.peersGetter[peer] = &grpcGetter{addr: peer}
	}
	s.Log("peers set: %v", peers)
}

// PickPeer 根据 key 选择对应的远程节点的grpc客户端
// 实现 group.PeerPicker 接口
func (s *Server) PickPeer(key string) (group.PeerGetter, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 一致性哈希环为空
	if s.peers == nil {
		return nil, false
	}
	peerAddr := s.peers.Get(key) // 根据 key 选择一致性哈希环最近的节点
	// 如果选到的是非本节点，则返回对应的getter
	if peerAddr != "" && peerAddr != s.self {
		getter := s.peersGetter[peerAddr] // 找到对应节点的grpc客户端
		if getter != nil {
			s.Log("Pick remote peer %s for key=%s", peerAddr, key)
			return getter, true
		}
	}
	return nil, false
}

// grpcGetter 表示一个远程节点的 gRPC 客户端，实现 group.PeerGetter
type grpcGetter struct {
	addr string
}

// Log 辅助日志
func (s *Server) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", s.self, fmt.Sprintf(format, v...))
}

// Get 通过 gRPC 客户端向远端节点获取值
// g *grpcGetter：接收者，表示客户端实例
// in *pb.GetRequest：输入参数，包含要获取的数据的key和group
// out *pb.GetResponse：输出参数，用于接收返回的数据
// 客户端（打电话）,需要向远程节点请求数据时
func (g *grpcGetter) Get(in *pb.GetRequest, out *pb.GetResponse) error {
	// 1. 📞 打电话给远程节点
	// 建立到远程节点的gRPC连接,g.addr：远程节点地址（如 "192.168.1.1:8000"）
	// insecure.NewCredentials()：使用不安全的连接（无TLS加密）
	// defer conn.Close()：确保函数结束时关闭连接
	conn, err := grpc.Dial(g.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s failed: %v", g.addr, err)
	}
	defer conn.Close()

	// 通过连接创建gRPC客户端
	// 2. 🗣️ 说："我要这个数据"
	client := pb.NewGroupCacheClient(conn)
	resp, err := client.Get(context.Background(), in)
	if err != nil {
		return fmt.Errorf("rpc Get to %s failed: %v", g.addr, err)
	}
	// 3. 👂 听对方回复
	out.Value = resp.Value
	return nil
}
