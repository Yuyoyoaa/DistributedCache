package server

import (
	"DistributedCache/internal/group"
	"log"
	"net/http"
)

// StartHTTP 启动一个简单的 HTTP 服务器用于调试
// 访问格式：/api?group=xxx&key=xxx
func StartHTTP(addr string) {
	http.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		groupName := r.URL.Query().Get("group")

		if key == "" || groupName == "" {
			http.Error(w, "missing key or group", http.StatusBadRequest)
			return
		}

		g := group.GetGroup(groupName)
		if g == nil {
			http.Error(w, "no such group: "+groupName, http.StatusNotFound)
			return
		}

		view, err := g.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(view.ByteSlice())
	})
	log.Println("HTTP server is running at", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
