package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "DistributedCache/api/cachepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		serverAddr string
		groupName  string
		key        string
	)
	flag.StringVar(&serverAddr, "server", "localhost:8001", "Connect to which cache server node")
	flag.StringVar(&groupName, "group", "scores", "Cache group name")
	flag.StringVar(&key, "key", "Tom", "Key to fetch")
	flag.Parse()

	// 1. 连接到 gRPC 服务器
	// 使用不安全的连接（无 TLS）方便测试
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close() // 函数return时会关闭连接，然后四次握手Fin

	client := pb.NewGroupCacheClient(conn)

	// 2. 构造请求
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel() // 确保函数退出时调用 cancel 清理资源

	req := &pb.GetRequest{
		Group: groupName,
		Key:   key,
	}

	// 3. 发起调用
	log.Printf("Requesting Key [%s] from Group [%s] via Node [%s]...", key, groupName, serverAddr)
	start := time.Now()
	resp, err := client.Get(ctx, req)
	elapsed := time.Since(start)

	if err != nil {
		log.Fatalf("Could not get cache: %v", err)
	}

	// 4. 输出结果
	fmt.Printf("--------------------------------\n")
	fmt.Printf("Result: %s\n", string(resp.GetValue()))
	fmt.Printf("Time:   %v\n", elapsed)
	fmt.Printf("--------------------------------\n")
}
