package test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "DistributedCache/api/cachepb"
	"DistributedCache/internal/discovery"
	"DistributedCache/internal/group"
	"DistributedCache/internal/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ============================================================================
// 辅助工具
// ============================================================================

func getEtcdEndpoints() []string {
	env := os.Getenv("ETCD_ENDPOINTS")
	if env != "" {
		return strings.Split(env, ",")
	}
	return []string{"localhost:2379"}
}

func getFreePort() int {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func mockDB(keyCount int) map[string]string {
	db := make(map[string]string, keyCount)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key_%06d", i)
		val := fmt.Sprintf("value_%06d_%s", i, strings.Repeat("x", 100))
		db[key] = val
	}
	return db
}

func newGRPCClient(addr string) (pb.GroupCacheClient, *grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewGroupCacheClient(conn), conn, nil
}

// nodeInstance 封装一个完整的缓存节点
type nodeInstance struct {
	svr  *server.Server
	grp  *group.Group
	addr string
}

// startCluster 启动多节点集群
// 关键：每个节点有自己的 Group 实例（独立的 singleflight），但共享同一个 groupName
// 这样跨节点 RPC 到达远程节点时，远程节点使用自己的 Group 处理请求，不会死锁
func startCluster(t *testing.T, nodeCount int, groupName string, db map[string]string, cacheBytes int64, policy string) []*nodeInstance {
	t.Helper()

	var nodes []*nodeInstance
	var addrs []string

	for i := 0; i < nodeCount; i++ {
		port := getFreePort()
		addr := fmt.Sprintf("localhost:%d", port)
		addrs = append(addrs, addr)

		// 每个节点创建自己独立的 Group 实例
		// 使用 节点唯一名 作为 Group name，这样各节点的 singleflight 相互独立
		nodeGroupName := fmt.Sprintf("%s_node_%d", groupName, i)
		g := group.NewGroup(nodeGroupName, cacheBytes, policy, group.GetterFunc(
			func(key string) ([]byte, error) {
				time.Sleep(1 * time.Millisecond) // 模拟回源
				if v, ok := db[key]; ok {
					return []byte(v), nil
				}
				return nil, fmt.Errorf("key %s not found", key)
			}))

		svr := server.NewServer(addr)
		g.RegisterPeers(svr)

		go func(a string) {
			if err := svr.Start(); err != nil {
				t.Logf("[Node %s] stopped: %v", a, err)
			}
		}(addr)

		nodes = append(nodes, &nodeInstance{svr: svr, grp: g, addr: addr})
	}

	time.Sleep(300 * time.Millisecond)

	// 让所有节点互相感知
	for _, n := range nodes {
		n.svr.SetPeers(addrs...)
	}

	return nodes
}

func stopCluster(nodes []*nodeInstance) {
	for _, n := range nodes {
		_ = n.svr.Close()
	}
}

// ============================================================================
// 1. 多节点网络压测
//    每个节点有独立的 Group，客户端请求对应节点的 Group
//    PickPeer 选到远程时，远程节点用自己的 Group（独立 singleflight），不会死锁
// ============================================================================

func TestStress_MultiNodeNetwork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		nodeCount    = 3
		clientCount  = 10
		requestCount = 200
		dbKeyCount   = 50
		cacheBytes   = 1 << 20
		policyType   = "lru"
	)

	db := mockDB(dbKeyCount)
	groupName := fmt.Sprintf("stress_multi_%d", time.Now().UnixNano())

	nodes := startCluster(t, nodeCount, groupName, db, cacheBytes, policyType)
	defer stopCluster(nodes)

	t.Logf("集群已启动: %d 个节点", nodeCount)
	for i, n := range nodes {
		t.Logf("  Node %d: %s (Group: %s_node_%d)", i, n.addr, groupName, i)
	}

	// 并发压测：每个客户端连固定的节点，请求该节点的 Group
	var (
		totalSuccess int64
		totalError   int64
		totalLatency int64
		wg           sync.WaitGroup
	)

	keys := make([]string, 0, dbKeyCount)
	for k := range db {
		keys = append(keys, k)
	}

	startTime := time.Now()

	for c := 0; c < clientCount; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			nodeIdx := clientID % nodeCount
			targetAddr := nodes[nodeIdx].addr
			targetGroup := fmt.Sprintf("%s_node_%d", groupName, nodeIdx)

			cli, conn, err := newGRPCClient(targetAddr)
			if err != nil {
				t.Errorf("[Client %d] 连接 %s 失败: %v", clientID, targetAddr, err)
				return
			}
			defer conn.Close()

			for i := 0; i < requestCount; i++ {
				key := keys[rand.Intn(len(keys))]

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				start := time.Now()
				resp, err := cli.Get(ctx, &pb.GetRequest{
					Group: targetGroup,
					Key:   key,
				})
				elapsed := time.Since(start)
				cancel()

				if err != nil {
					atomic.AddInt64(&totalError, 1)
					continue
				}

				atomic.AddInt64(&totalSuccess, 1)
				atomic.AddInt64(&totalLatency, int64(elapsed))

				expected := db[key]
				if string(resp.GetValue()) != expected {
					t.Errorf("[Client %d] Key=%s 数据不一致", clientID, key)
				}
			}
		}(c)
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	total := totalSuccess + totalError
	avgLatency := time.Duration(0)
	if totalSuccess > 0 {
		avgLatency = time.Duration(totalLatency / totalSuccess)
	}

	t.Logf("=== 多节点网络压测结果 ===")
	t.Logf("节点: %d, 客户端: %d, 每客户端请求: %d", nodeCount, clientCount, requestCount)
	t.Logf("总请求: %d, 成功: %d, 失败: %d", total, totalSuccess, totalError)
	if total > 0 {
		t.Logf("成功率: %.2f%%", float64(totalSuccess)/float64(total)*100)
	}
	t.Logf("QPS: %.2f, 平均延迟: %v, 总耗时: %v", float64(total)/totalTime.Seconds(), avgLatency, totalTime)

	if total > 0 && float64(totalSuccess)/float64(total) < 0.90 {
		t.Errorf("成功率过低: %.2f%%", float64(totalSuccess)/float64(total)*100)
	}
}

// ============================================================================
// 2. Etcd 服务发现压测
// ============================================================================

func TestStress_EtcdDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	endpoints := getEtcdEndpoints()
	disc, err := discovery.NewDiscovery(endpoints)
	if err != nil {
		t.Skipf("etcd 不可用 (%v): %v", endpoints, err)
	}
	disc.Stop()

	nodePrefix := fmt.Sprintf("stress-test/%d/nodes", time.Now().UnixNano())

	t.Run("快速注册注销", func(t *testing.T) {
		const cycles = 10
		var totalRegTime, totalDeregTime int64

		for i := 0; i < cycles; i++ {
			nodeAddr := fmt.Sprintf("localhost:%d", 10000+i)

			start := time.Now()
			reg, err := discovery.NewRegister(endpoints)
			if err != nil {
				t.Fatalf("第 %d 轮创建注册器失败: %v", i, err)
			}
			err = reg.Register(nodePrefix, nodeAddr, 5)
			regTime := time.Since(start)
			if err != nil {
				t.Fatalf("第 %d 轮注册失败: %v", i, err)
			}
			atomic.AddInt64(&totalRegTime, int64(regTime))

			start = time.Now()
			_ = reg.Stop()
			deregTime := time.Since(start)
			atomic.AddInt64(&totalDeregTime, int64(deregTime))
		}

		t.Logf("注册循环: %d 次", cycles)
		t.Logf("平均注册耗时: %v", time.Duration(totalRegTime/int64(cycles)))
		t.Logf("平均注销耗时: %v", time.Duration(totalDeregTime/int64(cycles)))
	})

	t.Run("并发注册", func(t *testing.T) {
		const concurrency = 10
		var wg sync.WaitGroup
		var successCount, failCount int64

		start := time.Now()
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				nodeAddr := fmt.Sprintf("localhost:%d", 20000+id)
				reg, err := discovery.NewRegister(endpoints)
				if err != nil {
					atomic.AddInt64(&failCount, 1)
					return
				}
				defer reg.Stop()

				err = reg.Register(nodePrefix+"/concurrent", nodeAddr, 5)
				if err != nil {
					atomic.AddInt64(&failCount, 1)
					return
				}
				atomic.AddInt64(&successCount, 1)
				time.Sleep(500 * time.Millisecond)
			}(i)
		}
		wg.Wait()

		t.Logf("并发注册 %d 个, 成功: %d, 失败: %d, 耗时: %v",
			concurrency, successCount, failCount, time.Since(start))
		if failCount > 0 {
			t.Errorf("存在注册失败: %d", failCount)
		}
	})

	t.Run("Watch变化感知", func(t *testing.T) {
		watchPrefix := nodePrefix + "/watch"
		var watchedChanges int64

		d, err := discovery.NewDiscovery(endpoints)
		if err != nil {
			t.Fatalf("创建发现器失败: %v", err)
		}
		defer d.Stop()

		err = d.WatchService(watchPrefix, func(peers []string) {
			atomic.AddInt64(&watchedChanges, 1)
		})
		if err != nil {
			t.Fatalf("Watch 失败: %v", err)
		}

		const watchNodes = 5
		for i := 0; i < watchNodes; i++ {
			nodeAddr := fmt.Sprintf("localhost:%d", 30000+i)
			reg, err := discovery.NewRegister(endpoints)
			if err != nil {
				t.Fatalf("注册失败: %v", err)
			}
			_ = reg.Register(watchPrefix, nodeAddr, 5)
			time.Sleep(200 * time.Millisecond)
			_ = reg.Stop()
			time.Sleep(200 * time.Millisecond)
		}

		time.Sleep(2 * time.Second)
		t.Logf("Watch 感知 %d 次变化（注册 %d 个节点）", atomic.LoadInt64(&watchedChanges), watchNodes)
	})
}

// ============================================================================
// 3. 跨节点 gRPC 通信压测
//    重点测试：请求到达 Server-A → PickPeer 选中 Server-B → RPC 到 Server-B → Server-B 本地回源
//    由于每个节点 Group 独立，不会产生 singleflight 死锁
// ============================================================================

func TestStress_CrossNodeGRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		nodeCount    = 3
		clientCount  = 10
		requestCount = 100
		dbKeyCount   = 100
		cacheBytes   = 1 << 20
	)

	db := mockDB(dbKeyCount)
	groupName := fmt.Sprintf("stress_cross_%d", time.Now().UnixNano())

	nodes := startCluster(t, nodeCount, groupName, db, cacheBytes, "lru")
	defer stopCluster(nodes)

	t.Logf("跨节点集群已启动: %d 个节点", nodeCount)

	var (
		totalSuccess int64
		totalFail    int64
		totalLatency int64
		wg           sync.WaitGroup
	)
	nodeReqCount := make([]int64, nodeCount)

	keys := make([]string, 0, dbKeyCount)
	for k := range db {
		keys = append(keys, k)
	}

	startTime := time.Now()

	for c := 0; c < clientCount; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			nodeIdx := clientID % nodeCount
			cli, conn, err := newGRPCClient(nodes[nodeIdx].addr)
			if err != nil {
				t.Errorf("[Client %d] 连接失败: %v", clientID, err)
				return
			}
			defer conn.Close()

			targetGroup := fmt.Sprintf("%s_node_%d", groupName, nodeIdx)

			for i := 0; i < requestCount; i++ {
				key := keys[rand.Intn(len(keys))]

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				start := time.Now()
				_, err := cli.Get(ctx, &pb.GetRequest{Group: targetGroup, Key: key})
				elapsed := time.Since(start)
				cancel()

				if err != nil {
					atomic.AddInt64(&totalFail, 1)
					continue
				}
				atomic.AddInt64(&totalSuccess, 1)
				atomic.AddInt64(&totalLatency, int64(elapsed))
				atomic.AddInt64(&nodeReqCount[nodeIdx], 1)
			}
		}(c)
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	total := totalSuccess + totalFail
	avgLatency := time.Duration(0)
	if totalSuccess > 0 {
		avgLatency = time.Duration(totalLatency / totalSuccess)
	}

	t.Logf("=== 跨节点 gRPC 压测结果 ===")
	t.Logf("总请求: %d, 成功: %d, 失败: %d", total, totalSuccess, totalFail)
	if total > 0 {
		t.Logf("成功率: %.2f%%, QPS: %.2f", float64(totalSuccess)/float64(total)*100, float64(total)/totalTime.Seconds())
	}
	t.Logf("平均延迟: %v, 总耗时: %v", avgLatency, totalTime)
	for i, n := range nodes {
		t.Logf("  Node %d (%s): %d 请求", i, n.addr, nodeReqCount[i])
	}
}

// ============================================================================
// 4. 磁盘回源压测（单节点，不涉及跨节点路由，纯测回源性能）
// ============================================================================

func TestStress_DiskBackfill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const (
		clientCount  = 20
		requestCount = 100
		dbKeyCount   = 200
		cacheBytes   = 1 << 10 // 1KB 极小缓存
		hotKeyCount  = 10
	)

	db := mockDB(dbKeyCount)

	var backfillCount int64
	var mu sync.Mutex
	backfillPerKey := make(map[string]int)

	groupName := fmt.Sprintf("stress_backfill_%d", time.Now().UnixNano())

	group.NewGroup(groupName, cacheBytes, "lru", group.GetterFunc(
		func(key string) ([]byte, error) {
			atomic.AddInt64(&backfillCount, 1)
			mu.Lock()
			backfillPerKey[key]++
			mu.Unlock()

			time.Sleep(time.Duration(5+rand.Intn(10)) * time.Millisecond)

			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("not found: %s", key)
		}))

	// 单节点，不设置 Peers，所有请求都本地回源
	port := getFreePort()
	addr := fmt.Sprintf("localhost:%d", port)
	svr := server.NewServer(addr)
	go func() {
		if err := svr.Start(); err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()
	defer svr.Close()
	time.Sleep(200 * time.Millisecond)

	keys := make([]string, 0, dbKeyCount)
	for k := range db {
		keys = append(keys, k)
	}
	hotKeys := keys[:hotKeyCount]

	var (
		totalSuccess int64
		totalFail    int64
		totalLatency int64
		wg           sync.WaitGroup
	)

	startTime := time.Now()

	for c := 0; c < clientCount; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			cli, conn, err := newGRPCClient(addr)
			if err != nil {
				t.Errorf("[Client %d] 连接失败: %v", clientID, err)
				return
			}
			defer conn.Close()

			for i := 0; i < requestCount; i++ {
				var key string
				if rand.Float64() < 0.8 {
					key = hotKeys[rand.Intn(len(hotKeys))]
				} else {
					key = keys[rand.Intn(len(keys))]
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				start := time.Now()
				resp, err := cli.Get(ctx, &pb.GetRequest{Group: groupName, Key: key})
				elapsed := time.Since(start)
				cancel()

				if err != nil {
					atomic.AddInt64(&totalFail, 1)
					continue
				}
				atomic.AddInt64(&totalSuccess, 1)
				atomic.AddInt64(&totalLatency, int64(elapsed))

				expected := db[key]
				if string(resp.GetValue()) != expected {
					t.Errorf("Key=%s 数据不一致", key)
				}
			}
		}(c)
	}

	wg.Wait()
	totalTime := time.Since(startTime)

	total := totalSuccess + totalFail
	avgLatency := time.Duration(0)
	if totalSuccess > 0 {
		avgLatency = time.Duration(totalLatency / totalSuccess)
	}

	t.Logf("=== 磁盘回源压测结果 ===")
	t.Logf("缓存: %d bytes, 客户端: %d, 每客户端请求: %d", cacheBytes, clientCount, requestCount)
	t.Logf("总请求: %d, 成功: %d, 失败: %d", total, totalSuccess, totalFail)
	if total > 0 {
		t.Logf("成功率: %.2f%%, QPS: %.2f", float64(totalSuccess)/float64(total)*100, float64(total)/totalTime.Seconds())
	}
	t.Logf("平均延迟: %v, 总耗时: %v", avgLatency, totalTime)
	t.Logf("--- 回源统计 ---")
	t.Logf("回源次数: %d / 总请求: %d", backfillCount, total)
	if total > 0 {
		t.Logf("回源比率: %.2f%%", float64(backfillCount)/float64(total)*100)
		t.Logf("Singleflight 抑制率: %.2f%%", (1-float64(backfillCount)/float64(total))*100)
	}

	t.Logf("--- 热点 Key 回源次数 ---")
	for i, k := range hotKeys {
		if i >= 5 {
			break
		}
		t.Logf("  %s: %d 次", k, backfillPerKey[k])
	}
}

// ============================================================================
// 5. Benchmark
// ============================================================================

func BenchmarkStress_SingleNodeThroughput(b *testing.B) {
	db := mockDB(100)
	groupName := fmt.Sprintf("bench_%d", time.Now().UnixNano())

	group.NewGroup(groupName, 1<<20, "lru", group.GetterFunc(
		func(key string) ([]byte, error) {
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("not found")
		}))

	port := getFreePort()
	addr := fmt.Sprintf("localhost:%d", port)
	svr := server.NewServer(addr)
	go func() { _ = svr.Start() }()
	defer svr.Close()
	time.Sleep(200 * time.Millisecond)

	cli, conn, err := newGRPCClient(addr)
	if err != nil {
		b.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	keys := make([]string, 0, 100)
	for k := range db {
		keys = append(keys, k)
	}
	for _, key := range keys {
		_, _ = cli.Get(context.Background(), &pb.GetRequest{Group: groupName, Key: key})
	}

	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			key := keys[rand.Intn(len(keys))]
			_, _ = cli.Get(context.Background(), &pb.GetRequest{Group: groupName, Key: key})
		}
	})
}
