package singleflight

import "sync"

// call 表示正在进行中，或已经结束的请求
type call struct {
	wg  sync.WaitGroup // 等待组，用于同步
	val interface{}    // 调用结果
	err error          // 调用错误
}

// Group 是 singleflight 的主数据结构，管理不同key的请求（call）
type Group struct {
	mu sync.Mutex       // 保护 m 的并发访问
	m  map[string]*call // key: 请求的键, value: 对应的调用信息
}

// Do 针对相同的key，无论Do被调用多少次，函数fn都只会被调用一次，等待fn调用结束了，返回返回值或错误
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// 锁只保护map的更新，实际业务逻辑在锁外执行
	g.mu.Lock() // 加锁：保护对 g.m 的访问
	// // 1. 懒初始化 map(不要为可能永远不会被使用的资源提前付费。等到真正需要时，再按需创建。)
	if g.m == nil {
		g.m = make(map[string]*call)
	}

	// 如果 key 对应的请求已经存在，则等待其完成，并共享结果
	if c, exist := g.m[key]; exist {
		g.mu.Unlock() //  解锁：检查完成，释放锁
		c.wg.Wait()   // 等待正在进行的请求完成
		return c.val, c.err
	}

	// 如果 key 对应的请求不存在，创建一个新的 call
	c := new(call)
	c.wg.Add(1)  // 发起请求前加锁，他并发请求c.wg.Wait() 等待任务完成
	g.m[key] = c // 添加到 map 中，表示 key 正在处理
	g.mu.Unlock()

	// 执行请求（如查询数据库/远程节点）
	c.val, c.err = fn()
	c.wg.Done() // 请求完成,计数器 -1

	g.mu.Lock()
	delete(g.m, key) // 更新 g.m，移除 key
	g.mu.Unlock()

	return c.val, c.err
}
