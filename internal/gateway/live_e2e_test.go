package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

// routeMoment 读一条请求的路由此刻记下的进度：收到了多少字节、最近一段内容
// 是什么时候。网关写它们时拿着 trace 的锁，所以这里也拿同一把锁。
func routeMoment(s *Server, model string) (bytes, contentAt int64, found bool) {
	s.trace.mu.Lock()
	defer s.trace.mu.Unlock()
	for _, rt := range s.trace.routes {
		if rt.Model == model {
			return rt.Bytes, rt.ContentAt, true
		}
	}
	return 0, 0, false
}

// 端到端：一个真实请求走完整个网关，上游是一段一段慢慢吐的 SSE。请求还在
// 进行时，它的路由上就该有字节在涨；结束后，那个数必须等于客户端真正收到的
// 字节数——这一栏只有这一个数字的来源，所以不能只用构造的计数器来证明。
func TestLiveCountsARealStream(t *testing.T) {
	chunks := []string{
		"data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"two\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":5,\"total_tokens\":8}}\n\n",
		"data: [DONE]\n\n",
	}
	// 上游吐完第一段就停下，等这个用例看过进度再继续：这才有"进行中"可看
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for i, c := range chunks {
			if _, err := io.WriteString(w, c); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			if i == 0 {
				<-release
			}
		}
	}))
	defer up.Close()
	defer func() {
		// 用例失败在半路时也要放上游走，否则 httptest 关不掉
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	if err := provider.Save(provider.Provider{ID: "live", Name: "Live", Chat: up.URL + "/v1", Key: "k", Models: []string{"m"}}); err != nil {
		t.Fatal(err)
	}

	s := New()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"live/m","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("content-type", "application/json")
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Handler().ServeHTTP(rec, req)
	}()

	// 等到下游真的收到了第一段字节：那一刻起，这条路由上就该有数
	var midBytes, midContentAt int64
	deadline := time.Now().Add(5 * time.Second)
	for {
		if bytes, contentAt, ok := routeMoment(s, "live/m"); ok && bytes > 0 {
			midBytes, midContentAt = bytes, contentAt
			break
		}
		if time.Now().After(deadline) {
			close(release)
			_, _, found := routeMoment(s, "live/m")
			t.Fatalf("a request whose first bytes arrived has no progress on its route (found=%v)", found)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if midBytes != int64(len(chunks[0])) {
		t.Fatalf("mid-stream bytes are %d, the client got %d", midBytes, len(chunks[0]))
	}
	if midContentAt == 0 {
		t.Fatal("content arrived but the route has no content time")
	}

	close(release)
	<-done

	bytes, _, ok := routeMoment(s, "live/m")
	if !ok {
		t.Fatal("the route is gone")
	}
	// 行上的"收到"就是客户端收到的字节数：差一个字节都是错的
	if want := int64(rec.Body.Len()); bytes != want {
		t.Fatalf("counted %d bytes, the client received %d", bytes, want)
	}
	if want := int64(len(strings.Join(chunks, ""))); bytes != want {
		t.Fatalf("counted %d bytes, the upstream wrote %d", bytes, want)
	}

	s.trace.mu.Lock()
	rt := s.trace.routes[len(s.trace.routes)-1]
	done2, output := rt.Done, rt.Output
	s.trace.mu.Unlock()
	if !done2 {
		t.Fatal("the route is still open after the reply ended")
	}
	// 请求里真的带了 usage：5 个输出 token 来自上游，不是估算
	if output != 5 {
		t.Fatalf("the route says %d output tokens, the vendor said 5", output)
	}
}
