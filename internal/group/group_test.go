package group

import (
	pb "DistributedCache/api/cachepb"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// 模拟数据库数据
var db = map[string]string{
	"Tom":  "600",
	"jack": "598",
	"Sam":  "673",
}

// TestGetter 测试 GetterFunc 接口转换是否正常
func TestGetter(t *testing.T) {
	// 声明一个类型为Getter的变量f
	// GetterFunc(...)：GetterFunc是实现Getter接口的类型
	// 它接受一个匿名函数作为参数
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	// reflect.DeepEqual可以比较几乎所有类型
	expect := []byte("key")
	if v, _ := f.Get("key"); !reflect.DeepEqual(v, expect) {
		t.Errorf("callback failed")
	}
}

// 测试本地缓存获取流程（缓存未命中 -> 回源 -> 缓存命中）
func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db))

	// 创建 Group，定义回源逻辑
	gee := NewGroup("scores", 2<<10, "lru", GetterFunc(
		func(key string) ([]byte, error) {
			// 记录回源次数
			loadCounts[key]++
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	// 1. 遍历测试数据，第一次获取应触发回源
	for k, v := range db {
		view, err := gee.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		// 假设 ByteView 有 String() 方法，如果没有请使用 string(view.ByteSlice())
		if view.String() != v {
			t.Errorf("option %s, expected %s, got %s", k, v, view.String())
		}

		// 验证回源次数为 1
		if loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		}
	}

	// 2. 再次获取，应该直接命中缓存，loadCounts 不应增加
	for k, _ := range db {
		_, err := gee.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		}
	}

	// 3. 测试不存在的 key
	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknown should be empty, but %s got", view)
	}
}

// TestGetGroup 测试 Group 的注册与获取
func TestGetGroup(t *testing.T) {
	gName := "scores_group_test"
	NewGroup(gName, 2<<10, "lru", GetterFunc(
		func(key string) ([]byte, error) { return nil, nil }))

	if group := GetGroup(gName); group == nil || group.name != gName {
		t.Fatalf("group %s not exist", gName)
	}

	if group := GetGroup("scores_not_exist"); group != nil {
		t.Fatalf("expect nil, but %v got", group)
	}
}

// TestSingleflight 测试并发防击穿机制
func TestSingleflight(t *testing.T) {
	var loadCount int
	var mu sync.Mutex

	// 创建一个模拟耗时的 Group
	g := NewGroup("flight_test", 2<<10, "lru", GetterFunc(
		func(key string) ([]byte, error) {
			mu.Lock()
			loadCount++ // 记录这个函数被调用了多少次
			mu.Unlock()
			time.Sleep(100 * time.Millisecond) // 模拟慢查询
			return []byte("bar"), nil
		}))

	var wg sync.WaitGroup
	// 发起 10 个并发请求
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			view, err := g.Get("foo") // 所有 goroutine 都请求相同的 key: "foo"
			if err != nil {
				t.Errorf("Get error: %v", err)
			}
			if view.String() != "bar" {
				t.Errorf("expect bar, got %s", view.String())
			}
		}()
	}
	wg.Wait() // 等待所有 10 个 goroutine 完成

	// 验证回源逻辑只执行了一次
	if loadCount != 1 {
		t.Errorf("expect 1 call,got %d", loadCount)
	}
}

// --- Mock 对象用于测试远程节点获取逻辑 ---

// mockPeerGetter 模拟远程节点客户端
type mockPeerGetter struct {
	data map[string][]byte // 远程节点存储的数据
}

func (m *mockPeerGetter) Get(in *pb.GetRequest, out *pb.GetResponse) error {
	// 模拟网络请求：根据 key 返回数据
	if v, ok := m.data[in.Key]; ok {
		out.Value = v
		return nil
	}
	return fmt.Errorf("remote key not found")
}

// mockPeerPicker 模拟节点选择器
type mockPeerPicker struct {
	peers map[string]*mockPeerGetter // 节点地址->远程客户端
}

func (m *mockPeerPicker) PickPeer(key string) (PeerGetter, bool) {
	// 模拟逻辑：如果 key 是 "remote_key"，则返回 mock 节点
	if key == "remote_key" && len(m.peers) > 0 {
		for _, p := range m.peers {
			return p, true
		}
	}
	return nil, false
}

// TestGetFromPeer 测试从远程节点获取数据
func TestGetFromPeer(t *testing.T) {
	// 1. 准备远程节点数据
	remoteVal := []byte("remote_value_123")
	peer := &mockPeerGetter{
		data: map[string][]byte{
			"remote_key": remoteVal,
		},
	}

	// 2.准备 PeerPicker
	picker := &mockPeerPicker{
		peers: map[string]*mockPeerGetter{
			"peer1": peer,
		},
	}

	// 3.创建 Group
	g := NewGroup("remote_test", 2<<10, "lru", GetterFunc(
		func(key string) ([]byte, error) {
			return nil, fmt.Errorf("should not be called for remote hit")
		}))

	// 注册节点选择器
	g.RegisterPeers(picker)

	// 4. 执行Get，预期命中远程节点
	view, err := g.Get("remote_key")
	if err != nil {
		t.Fatalf("failed to get from peer: %v", err)
	}

	if !reflect.DeepEqual(view.ByteSlice(), remoteVal) {
		t.Errorf("expect %s, got %s", string(remoteVal), view.String())
	}

	// 5. 验证是否已写入本地缓存 （再次获取不应报错，也不应走远程 - 虽然 Mock 没计数，但可以验证值存在）
	view2, err := g.Get("remote_key")
	if err != nil {
		t.Fatalf("failed to get from local cache after remote fetch: %v", err)
	}
	if view2.String() != string(remoteVal) {
		t.Errorf("expect %s, got %s", string(remoteVal), view2.String())
	}
}
