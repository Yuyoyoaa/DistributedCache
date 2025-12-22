#!/bin/bash

# 遇到错误立即退出
set -e

# 项目根目录
ROOT_DIR=$(pwd)
SERVER_BIN="./bin/server"
CLIENT_BIN="./bin/client"

# 1. 编译代码
echo "Building server..."
go build -o $SERVER_BIN ./cmd/server
echo "Building client..."
go build -o $CLIENT_BIN ./cmd/client

# 2. 检查 Etcd 是否运行
echo "Checking Etcd..."
if ! pgrep -x "etcd" > /dev/null && ! docker ps | grep "etcd" > /dev/null; then
    echo "Etcd is not running. Starting local etcd via docker..."
    docker run -d --rm --name etcd-server \
    -p 2379:2379 \
    -e ALLOW_NONE_AUTHENTICATION=yes \
    bitnami/etcd:latest
    sleep 3 # 等待 etcd 启动
else
    echo "Etcd is running."
fi

# 3. 清理旧的 Server 进程
echo "Cleaning up old server processes..."
trap "kill 0" EXIT # 脚本退出时杀死所有子进程

# 4. 启动集群 (3个节点)
# 注意：这需要你的 main.go 支持从配置文件读取，或者支持这里传入的命令行参数
# 这里假设 main.go 仍然使用 flag 接收参数
echo "Starting cluster nodes..."

# 节点1 (端口 8001)
$SERVER_BIN -port=8001 -etcd=localhost:2379 -group=scores &
PID1=$!
echo "Node 1 started (PID: $PID1, Port: 8001)"

# 节点2 (端口 8002)
$SERVER_BIN -port=8002 -etcd=localhost:2379 -group=scores &
PID2=$!
echo "Node 2 started (PID: $PID2, Port: 8002)"

# 节点3 (端口 8003)
$SERVER_BIN -port=8003 -etcd=localhost:2379 -group=scores &
PID3=$!
echo "Node 3 started (PID: $PID3, Port: 8003)"

sleep 2
echo "Cluster started successfully!"
echo "Press Ctrl+C to stop the cluster..."

# 5. 保持运行，直到用户按下 Ctrl+C
wait