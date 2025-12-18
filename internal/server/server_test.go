package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "DistributedCache/api/cachepb"
	"DistributedCache/internal/group"
)

// ---------------------------------------------------------------------
// 1. 辅助工具与 Mock
// ---------------------------------------------------------------------

// mockGetter 模拟数据库回源操作
type mockGetter map[string][]byte

func (m mockGetter) Get(key string) ([]byte, error) {
	if v, ok := m[key]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("key not found in db: %s", key)
}

// setupTestGroup 辅助函数，用于在测试前初始化一个 Group
func setupTestGroup(name string) *group.Group {
	db := mockGetter{
		"Tom":   []byte("630"),
		"Jack":  []byte("589"),
		"Sam":   []byte("567"),
		"Alice": []byte("12345"),
	}
	// 创建一个 Group，配置较小的缓存以便测试
	return group.NewGroup(name, 2<<10, "lru", group.GetterFunc(db.Get))
}

// ---------------------------------------------------------------------
// 2. 单元测试
// ---------------------------------------------------------------------

// TestPickPeer 测试一致性哈希的节点选择逻辑
func TestPickPeer(t *testing.T) {
	// 1. 创建 Server，假设当前节点是端口 8000
	s := NewServer("localhost:8000")

	// 2. 设置 Peer 列表
	s.SetPeers("localhost:8000", "localhost:8001", "localhost:8002")

	// 3. 测试用例
	// 注意：一致性哈希的结果取决于具体的 Hash 算法（通常是 CRC32）。
	// 这里我们主要测试：
	// a. PickPeer 是否能返回 true/false
	// b. 选中自己时应返回 false
	// c. 选中远程时应返回 true 且 getter 不为空

	// 我们遍历一些 key，只要能覆盖到 "选中自己" 和 "选中别人" 两种情况即可
	hitSelf := false
	hitRemote := false

	keys := []string{"1", "2", "3", "4", "5", "key_a", "key_b"}
	for _, key := range keys {
		picker, ok := s.PickPeer(key)
		if !ok {
			// 理论上是选中了自己 (localhost:8000)
			hitSelf = true
			t.Logf("Key [%s] picked self (local)", key)
		} else {
			// 选中了远程节点
			hitRemote = true
			// 验证 picker 类型
			if _, ok := picker.(*grpcGetter); !ok {
				t.Errorf("Peer getter should be type *grpcGetter")
			}
			t.Logf("Key [%s] picked remote peer", key)
		}
	}

	if !hitSelf {
		t.Log("Warning: Test didn't hit 'self' node, might need more keys or specific hash calculation")
	}
	if !hitRemote {
		t.Error("Error: Test failed to pick any remote peer")
	}
}

// TestServer_Get_Direct 测试 Server.Get 方法的核心逻辑（不走网络，直接函数调用）
func TestServer_Get_Direct(t *testing.T) {
	groupName := "scores_direct"
	setupTestGroup(groupName)

	s := NewServer("localhost:9999")

	// 测试正常情况
	req := &pb.GetRequest{
		Group: groupName,
		Key:   "Tom",
	}

	resp, err := s.Get(context.Background(), req)
	if err != nil {
		t.Fatalf("Server.Get failed: %v", err)
	}

	if string(resp.Value) != "630" {
		t.Errorf("Expected value '630', got '%s'", string(resp.Value))
	}

	// 测试不存在的 Group
	reqBadGroup := &pb.GetRequest{Group: "unknown_group", Key: "Tom"}
	if _, err := s.Get(context.Background(), reqBadGroup); err == nil {
		t.Error("Expected error for unknown group, got nil")
	}
}

// ---------------------------------------------------------------------
// 3. 集成测试 (Integration Test)
// ---------------------------------------------------------------------

// TestGRPC_Integration 启动真实的 gRPC 服务端，并通过 Client 调用
func TestGRPC_Integration(t *testing.T) {
	// --- 1. 准备环境 ---
	addr := "localhost:9001" // 测试用的端口
	groupName := "scores_rpc"

	// 初始化 Group (这是 Server 端实际查找数据的地方)
	setupTestGroup(groupName)

	// --- 2. 启动 Server ---
	s := NewServer(addr)

	// 在协程中启动 Server，因为 s.Start() 是阻塞的
	go func() {
		// Start 内部会监听端口并阻塞
		if err := s.Start(); err != nil {
			// 注意：在实际测试中，如果 Server 关闭可能会导致这里报错，属于正常
			t.Logf("Server stopped: %v", err)
		}
	}()

	// 等待一小会儿，确保 Server 启动完毕
	time.Sleep(1 * time.Second)

	// --- 3. 模拟 Client 端调用 ---
	// 我们直接利用 grpcGetter (它本质上就是一个 Client 封装)
	// 或者手动创建 Client 也可以。这里为了测试 grpcGetter 代码，直接用它。

	clientGetter := &grpcGetter{addr: addr}

	req := &pb.GetRequest{
		Group: groupName,
		Key:   "Jack", // 对应 mockDb 中的 "589"
	}
	resp := &pb.GetResponse{}

	// 调用 Get (内部会 Dial -> RPC Call -> Receive)
	err := clientGetter.Get(req, resp)
	if err != nil {
		t.Fatalf("RPC Call failed: %v", err)
	}

	// --- 4. 验证结果 ---
	expected := "589"
	if string(resp.Value) != expected {
		t.Errorf("Integration test failed. Expected %s, got %s", expected, string(resp.Value))
	} else {
		t.Logf("Integration test success! Got value: %s", string(resp.Value))
	}
}
