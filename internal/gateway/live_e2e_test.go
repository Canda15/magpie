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

// 端到端：一个真实请求走完整个网关，上游是一段一段慢慢吐的 SSE。请求还在
// 进行时快照里就该看到字节在涨；结束后该落在「最近结束」里，而且它记的
// 字节数必须等于客户端真正收到的字节数——这页只有这一个数字的来源，
// 所以不能只用构造的计数器来证明。
func TestLiveCountsARealStream(t *testing.T) {
	liveReset()
	defer liveReset()

	chunks := []string{
		"data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"two\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":5,\"total_tokens\":8}}\n\n",
		"data: [DONE]\n\n",
	}
	// 上游吐完第一段就停下，等这个用例看过快照再继续：这才有"进行中"可看
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

	// 等到下游真的收到了第一段字节：那一刻起，这一行就该是"在收"，
	// 而且已经记下的字节数不是 0
	var mid LiveRequest
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, active, _ := LiveSnapshot()
		if len(active) == 1 && active[0].Bytes > 0 {
			mid = active[0]
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatalf("a request whose first bytes arrived is still %+v", active)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if mid.Phase != "receiving" {
		t.Fatalf("mid-stream phase is %q, want receiving: %+v", mid.Phase, mid)
	}
	if mid.Model != "m" || mid.Provider != "live" {
		t.Fatalf("the row doesn't say who answered: %+v", mid)
	}

	close(release)
	<-done

	_, active, recent := LiveSnapshot()
	if len(active) != 0 || len(recent) != 1 {
		t.Fatalf("after it ended: %d active, %d recent", len(active), len(recent))
	}
	end := recent[0]
	// 页面上"累计响应"就是客户端收到的字节数：差一个字节都是错的
	if want := int64(rec.Body.Len()); end.Bytes != want {
		t.Fatalf("counted %d bytes, the client received %d", end.Bytes, want)
	}
	if want := int64(len(strings.Join(chunks, ""))); end.Bytes != want {
		t.Fatalf("counted %d bytes, the upstream wrote %d", end.Bytes, want)
	}
	if end.Phase != "complete" || end.EndReason != "complete" || end.Output != 5 || end.Status != 200 {
		t.Fatalf("the record it ended as: %+v", end)
	}
	// 请求里真的带了 usage：5 个输出 token 来自上游，不是估算
	if end.Output != 5 {
		t.Fatalf("output tokens: %+v", end)
	}
}
