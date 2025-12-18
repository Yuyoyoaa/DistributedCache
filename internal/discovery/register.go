package discovery

// 正确的导入路径
import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3" // 注意这里
)

// 注意这里

// Register 负责向 Etcd 注册服务并保持心跳
type Register struct {
	cli         *clientv3.Client                        // etcd 客户端实例，用于与 etcd 集群通信
	leaseID     clientv3.LeaseID                        // 租约 ID，用于设置键值对的生存时间（TTL）
	keepaliveCh <-chan *clientv3.LeaseKeepAliveResponse // 心跳响应通道，接收 etcd 的心跳确认
	closeCh     chan struct{}                           // 关闭信号通道，用于优雅停止注册和心跳
}

// NewRegister 创建一个注册器
// endpoints []string - etcd 集群的地址列表，如 []string{"localhost:2379", "192.168.1.1:2379"}
// 主要职责:
//  1. 建立与 etcd 集群的连接
//  2. 初始化 Register 结构体的各个字段
//  3. 为优雅关闭准备必要的通道
func NewRegister(endpoints []string) (*Register, error) {
	// clientv3.New 是 etcd 官方客户端库提供的构造函数
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second, // 如果5秒内没有建立连接，返回超时错误,客户端会继续尝试其他 endpoint
	})
	if err != nil {
		return nil, err
	}

	//   - leaseID:     租约ID（还未创建租约）
	//   - keepaliveCh: 心跳通道（还未开始心跳）
	return &Register{
		cli:     cli,
		closeCh: make(chan struct{}),
	}, nil
}

// Register 注册服务,将当前节点注册到 etcd 服务发现系统
// serviceName : 服务名称，例如 "distributed-cache/nodes"
// nodeAddr: 当前节点地址，例如 "localhost:8001"
// ttl: 租约过期时间（秒）

// 工作流程:
//  1. 创建租约（设置TTL）
//  2. 将服务信息写入 etcd 并绑定租约
//  3. 启动心跳保持租约有效
//  4. 开启监听协程处理心跳响应
func (r *Register) Register(serviceName string, nodeAddr string, ttl int64) error {
	// 1. 创建租约
	lease := clientv3.NewLease(r.cli)
	// 向 etcd 申请一个租约，指定 TTL（Time To Live）
	// 例如 ttl=10 表示：如果10秒内没有续约，租约会自动过期
	grantResp, err := lease.Grant(context.Background(), ttl)
	if err != nil {
		return nil
	}
	r.leaseID = grantResp.ID

	// 2. 写入键值对(Key: serviceName/nodeAddr, Value: nodeAddr) 并绑定租约
	// 例如 Key: "distributed-cache/nodes/localhost:8001"
	key := serviceName + "/" + nodeAddr
	kv := clientv3.NewKV(r.cli) // // 创建 KV 客户端，用于操作键值对
	// WithLease(r.leaseID) 是关键：将此键值对与租约关联
	_, err = kv.Put(context.Background(), key, nodeAddr, clientv3.WithLease(r.leaseID))
	if err != nil {
		return err
	}

	// 3. 设置 KeepAlive 自动续租
	// 启动心跳机制，定期向 etcd 发送续租请求
	// 如果节点正常运行，租约会一直续期，服务信息就不会过期
	keepAliveCh, err := lease.KeepAlive(context.Background(), r.leaseID)
	if err != nil {
		return err
	}
	r.keepaliveCh = keepAliveCh

	log.Printf("[Discovery] Registered node: %s with TTL: %d", key, ttl)
	// 4. 开启协程监听续租响应，处理连接断开的情况
	// 这个协程会一直运行，直到收到关闭信号或心跳失败
	go r.watcher()
	return nil
}

// watcher 监听续租通道，处理心跳响应
// 这个协程负责：
//  1. 监控心跳是否正常
//  2. 处理心跳失败的情况
//  3. 响应关闭信号
func (r *Register) watcher() {
	for {
		select {
		case <-r.closeCh:
			// 收到关闭信号，优雅退出协程
			// 这通常发生在调用 Stop() 方法时
			return
		case res, ok := <-r.keepaliveCh:
			if !ok {
				log.Println("[Discovery] Lease keep-alive channel closed, stopping watcher")
				return
			}
			// 打印心跳日志
			// 成功收到心跳响应
			// res 包含续租确认信息
			// 此时租约的 TTL 会被重置，重新开始计时
			log.Printf("[Discovery] Lease renewed: %d", res.ID)
			_ = res
		}
	}
}

// Stop 停止注册服务
func (r *Register) Stop() error {
	// 1. 关闭关闭信号通道，通知所有协程退出
	// 这是 Go 中通知多个协程退出的常用模式
	close(r.closeCh)
	// 撤销租约，Etcd 会自动删除绑定的 Key
	// 如果没有执行这一步，etcd 会等待租约自然过期后才删除
	if _, err := r.cli.Revoke(context.Background(), r.leaseID); err != nil {
		return err
	}
	return r.cli.Close()
}
