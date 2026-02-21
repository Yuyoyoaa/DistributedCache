#!/bin/bash

# 运行所有压力测试（非 -short 模式）
# 需要先启动 etcd（如果测试 etcd 相关场景）

echo "=== 运行压力测试 ==="

# 1. 多节点网络 + 跨机器 + 磁盘回源（不依赖 etcd）
go test -v -run "TestStress_MultiNodeNetwork|TestStress_CrossNodeGRPC|TestStress_DiskBackfill" \
    -timeout 300s ./test/

# 2. Etcd 服务发现压测（需要 etcd 环境）
# 设置 etcd 地址：
# export ETCD_ENDPOINTS="localhost:2379"
go test -v -run "TestStress_EtcdDiscovery" -timeout 120s ./test/

# 3. Benchmark 基准测试
go test -v -bench="BenchmarkStress" -benchmem -benchtime=10s -timeout 120s ./test/