package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"DistributedCache/config"
	"DistributedCache/internal/discovery"
	"DistributedCache/internal/group"
	"DistributedCache/internal/server"
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
	// 1. 尝试加载配置文件
	// 默认读取当前目录下的 config.json
	// 如果文件不存在，LoadConfig 会返回默认配置，不会报错
	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		// 如果是 JSON 格式错误等严重错误，则通过日志警告，但依然使用默认值继续运行
		log.Printf("[Config] Failed to load config.json: %v, using defaults.", err)
		cfg = config.DefaultConfig()
	}

	// 2. 解析命令行参数
	// 策略：使用 Config 中的值作为 Flag 的默认值。
	// 这样：命令行 > 配置文件 > 代码硬编码默认值

	var (
		port        int
		api         bool
		etcdAddrs   string
		groupName   string
		cacheSize   int64
		cachePolicy string
	)

	// 从 cfg.Addr ("localhost:8001") 中解析出端口作为默认端口
	defaultPort := 8001
	if _, pStr, err := net.SplitHostPort(cfg.Addr); err == nil {
		if p, err := strconv.Atoi(pStr); err == nil {
			defaultPort = p
		}
	}

	flag.IntVar(&port, "port", defaultPort, "server port")
	flag.BoolVar(&api, "api", false, "Start a api server")
	// 将 config 中的字符串数组拼接成逗号分隔字符串，作为 flag 默认值
	flag.StringVar(&etcdAddrs, "etcd", strings.Join(cfg.EtcdAddrs, ","), "Etcd endpoints, separated by comma")
	flag.StringVar(&groupName, "group", "scores", "Cache group name")
	// 增加缓存配置的 flag，允许通过命令行覆盖 config.json
	flag.Int64Var(&cacheSize, "cache_size", cfg.CacheSize, "Max cache size in bytes")
	flag.StringVar(&cachePolicy, "cache_policy", cfg.CachePolicy, "Cache replacement policy (lru, lfu, fifo)")

	flag.Parse() // ← 解析命令行，如果有传参，会覆盖上面的默认值

	_ = api

	// 3. 初始化核心组件：Group (缓存命名空间)
	// 定义当缓存未命中时如何回源获取数据
	getter := group.GetterFunc(func(key string) ([]byte, error) {
		log.Printf("[SlowDB] Searching key: %s", key)
		if v, ok := mockDB[key]; ok {
			return []byte(v), nil
		}
		return nil, fmt.Errorf("%s not exist", key)
	})

	// 创建 Group
	// 使用配置中的 cacheSize 和 cachePolicy
	g := group.NewGroup(groupName, cacheSize, cachePolicy, getter)
	log.Printf("Group [%s] initialized. Policy: %s, Size: %d bytes", groupName, cachePolicy, cacheSize)

	// 4. 初始化 gRPC Server
	// 组装地址
	addr := fmt.Sprintf("localhost:%d", port)
	svr := server.NewServer(addr)

	// 将 Server 注册为 Group 的 PeerPicker
	g.RegisterPeers(svr)

	// 5. Etcd 服务发现与注册
	// 将 flag 解析出来的 etcdAddrs (逗号分隔) 转回 slice
	etcdEndpoints := strings.Split(etcdAddrs, ",")
	// 使用 config 中的 ServiceName (通常是前缀，如 "distributed-cache/nodes")
	servicePrefix := cfg.ServiceName

	// A. 服务注册 (Register)
	reg, err := discovery.NewRegister(etcdEndpoints)
	if err != nil {
		log.Fatalf("Failed to create etcd register: %v", err)
	}
	defer reg.Stop()

	// 注册并维持租约 (TTL 10秒)
	if err := reg.Register(servicePrefix, addr, 10); err != nil {
		log.Fatalf("Failed to register service: %v", err)
	}
	log.Printf("Node registered to Etcd: %s (Prefix: %s)", addr, servicePrefix)

	// B. 服务发现 (Discovery)
	disc, err := discovery.NewDiscovery(etcdEndpoints)
	if err != nil {
		log.Fatalf("Failed to create etcd discovery: %v", err)
	}
	defer disc.Stop()

	// 监听节点变化
	err = disc.WatchService(servicePrefix, func(peers []string) {
		svr.SetPeers(peers...)
		log.Printf("Cluster peers updated: %v", peers)
	})
	if err != nil {
		log.Fatalf("Failed to watch service: %v", err)
	}

	// 6. 启动 gRPC 服务
	go func() {
		log.Printf("Server is running at %s...", addr)
		if err := svr.Start(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// 7. 优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Server is shutting down...")
	if err := svr.Close(); err != nil {
		log.Printf("Failed to close server: %v", err)
	}
}
