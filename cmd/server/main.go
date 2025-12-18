package main

import (
	"DistributedCache/internal/discovery"
	"DistributedCache/internal/group"
	"DistributedCache/internal/server"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// 模拟数据库
var mockDB = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
	"Amy":  "985",
	"Bob":  "211",
}

func main() {
	var (
		port      int
		api       bool
		etcdAddrs string
		groupName string
	)
	// 命令行参数解析
	// &port: 将解析的值存储到这个变量
	//"port": 命令行标志名，用 --port=8002或 -port=8002
	//8001: 默认值（如果不指定参数）
	//"server port": 参数说明（帮助信息中显示）
	flag.IntVar(&port, "port", 8001, "server port")
	flag.BoolVar(&api, "api", false, "Start a api server (Not implemented in this demo)")
	flag.StringVar(&etcdAddrs, "etcd", "localhost:2379", "Etcd endpoints, separated by comma")
	flag.StringVar(&groupName, "group", "scores", "Cache group name")
	flag.Parse() //  ← 这里开始解析！

	// 1.初始化核心组件：Group(缓存命名空间)
	// 定义当缓存未命中时如何回源获取数据
	getter := group.GetterFunc(func(key string) ([]byte, error) {
		log.Printf("[SlowDB] Searching key: %s", key)
		if v, ok := mockDB[key]; ok {
			return []byte(v), nil
		}
		return nil, fmt.Errorf("%s not exist", key)
	})

	// 创建 Group： 名字 "scores", 最大缓存 2^20 bytes (1MB), 策略 LRU
	g := group.NewGroup(groupName, 2<<20, "lru", getter)
	log.Printf("Group [%s] initialized", groupName)

	// 2.初始化 gRPC Server
	// 注意：这里使用 localhost 方便测试，生产环境应获取真实本机 IP
	addr := fmt.Sprintf("localhost:%d", port)
	svr := server.NewServer(addr)

	// 将 Server 注册为 Group 的 PeerPicker (允许 Group 通过 Server 选节点)
	g.RegisterPeers(svr)

	// 3.Etcd 服务发现与注册
	etcdEndpoints := []string{etcdAddrs}
	servicePrefix := "distributed-cache/nodes"

	// A. 服务注册 (Register): 把自己告诉大家
	reg, err := discovery.NewRegister(etcdEndpoints)
	if err != nil {
		log.Fatalf("Failed to create etcd register: %v", err)
	}
	defer reg.Stop()

	// 注册并维持租约 (TTL 10秒)
	if err := reg.Register(servicePrefix, addr, 10); err != nil {
		log.Fatalf("Failed to register service: %v", err)
	}
	log.Printf("Node registered to Etcd: %s", addr)

	// B. 服务发现 (Discovery): 听听还有谁在
	disc, err := discovery.NewDiscovery(etcdEndpoints)
	if err != nil {
		log.Fatalf("Failed to create etcd discovery: %v", err)
	}
	defer disc.Stop()

	// 监听节点变化，一旦有变动就更新 Hash 环
	err = disc.WatchService(servicePrefix, func(peers []string) {
		// 更新一致性哈希环
		svr.SetPeers(peers...)
		log.Printf("Cluster peers updated: %v", peers)
	})
	if err != nil {
		log.Fatalf("Failed to watch service: %v", err)
	}

	// 4. 启动 gRPC 服务
	// 因为 svr.Start() 是阻塞的，我们在主协程处理信号，把它放在子协程
	go func() {
		log.Printf("Server is running at %s...", addr)
		if err := svr.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// 5. 优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Server is shutting down...")
}
