package test

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"testing"
	"time"

	pb "DistributedCache/api/cachepb" // 引用生成的 gRPC 代码

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	serverAddr = flag.String("addr", "localhost:8001", "The address of the cache server")
)

// client 包装 gRPC 客户端
type client struct {
	pb.GroupCacheClient
	conn *grpc.ClientConn
}

func newClient(addr string) (*client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := pb.NewGroupCacheClient(conn)
	return &client{GroupCacheClient: c, conn: conn}, nil
}

func (c *client) Close() {
	c.conn.Close()
}

// BenchmarkGet 测试 Get 接口的吞吐量
// 运行方式: go test -v -bench=BenchmarkGet -benchmem -args -addr=localhost:8001
func BenchmarkGet(b *testing.B) {
	// 1. 初始化客户端连接
	cli, err := newClient(*serverAddr)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer cli.Close()

	groupName := "scores"
	// 可选：预先定义一些测试用的 key
	keys := []string{"Tom", "Jack", "Sam", "Amy", "Bob"}

	// 2. 并行测试
	b.ResetTimer()

	// 注意这里：我们将 testing.PB 参数命名为 p，避免与包名 pb 冲突
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			// 随机选择一个 Key
			key := keys[rand.Intn(len(keys))]

			// 构造请求
			req := &pb.GetRequest{
				Group: groupName,
				Key:   key,
			}

			// 发送 RPC 请求
			_, err := cli.Get(context.Background(), req)
			if err != nil {
				// 在 Benchmark 中通常不 Fatal，通过日志记录错误率即可
				// 或者忽略非关键错误，专注于吞吐量
				// log.Printf("rpc error: %v", err)
			}
		}
	})
}

// TestMain 用于解析 flags
func TestMain(m *testing.M) {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	log.Printf("Starting benchmark against %s", *serverAddr)
	m.Run()
}
