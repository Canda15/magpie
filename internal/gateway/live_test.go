package gateway

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

// quietRates 让用例学到的样本落到临时目录：估算的样本会写文件，测试绝不能
// 写进用户的真实配置目录，也不能被上一次跑留下的样本影响。
func quietRates(t *testing.T) {
	t.Helper()
	old := liveRatesFile
	// 目录只取一次：每次调用 t.TempDir() 都是**另一个**目录，那样"写进去"和
	// "读回来"会落到不同地方，重启那一个用例就永远读不到自己写的文件。
	dir := t.TempDir()
	liveRatesFile = func() string { return filepath.Join(dir, "receive-tokens.json") }
	resetRates()
	t.Cleanup(func() {
		liveRatesFile = old
		resetRates()
	})
}

// liveChatEvent 是一条 Chat 流式事件，正文是 s。
func liveChatEvent(s string) []byte {
	return []byte(`data: {"choices":[{"delta":{"content":"` + s + `"}}]}` + "\n\n")
}

// 一条请求：接入时三个数都是 0，写到客户端的字节与其中的内容各算各的，
// 结束时内容定稿，结束后再写的几笔（编码器收尾）照样算进去。
func TestLiveCountsBytesAndContent(t *testing.T) {
	quietRates(t)

	live := liveBegin(nil, "vendor/model", provider.Chat)
	if b, c, at := live.totals(); b != 0 || c != 0 || at != 0 {
		t.Fatalf("a request with no byte yet: %d bytes, %d content, content at %d", b, c, at)
	}

	first := liveChatEvent("hello")
	live.observe(first, "text/event-stream")
	b, c, at := live.totals()
	if b != int64(len(first)) {
		t.Fatalf("counted %d bytes, the client got %d", b, len(first))
	}
	if c != 5 {
		t.Fatalf("content bytes of %q are %d, want 5", first, c)
	}
	if at == 0 {
		t.Fatal("the first content left no time")
	}

	live.routed("vendor", "model")
	live.finish("complete", 42)
	if _, c, _ := live.totals(); c != 5 {
		t.Fatalf("content after it ended: %d, want 5", c)
	}

	// 结束后还有几笔：编码器收尾，客户端真的收到了，就该算
	trailing := liveChatEvent("!")
	live.observe(trailing, "text/event-stream")
	if b, c, _ := live.totals(); b != int64(len(first)+len(trailing)) || c != 6 {
		t.Fatalf("after a trailing write: %d bytes, %d content", b, c)
	}
	// 再 finish 一次什么也不做（编码器收尾会把 finish 叫两次）
	live.finish("complete", 42)
	if _, c, _ := live.totals(); c != 6 {
		t.Fatalf("a second finish changed the content to %d", c)
	}
}

// 网关自己的心跳（": keepalive"）只算字节，不算内容，也不动"最近内容"的时间：
// 否则上游真的卡住时，行上也会一直显示在动，而这正是要看见的事。
func TestLiveHeartbeatIsBytesNotContent(t *testing.T) {
	quietRates(t)

	live := liveBegin(nil, "m", provider.Chat)
	live.observe(liveChatEvent("a"), "text/event-stream") // 第一段内容
	firstBytes, firstContent, firstAt := live.totals()

	beat := []byte(": keepalive\n")
	live.observe(beat, "text/event-stream") // 上游沉默，网关发了一次心跳
	bytes, content, at := live.totals()
	if bytes != firstBytes+int64(len(beat)) {
		t.Fatalf("the heartbeat's bytes weren't counted: %d then %d", firstBytes, bytes)
	}
	if at != firstAt {
		t.Fatalf("a heartbeat moved the last content's time: %d then %d", firstAt, at)
	}
	if content != firstContent {
		t.Fatalf("a heartbeat was counted as content: %d then %d", firstContent, content)
	}

	// 内容再来：时间跟着走，那样页面就不再显示静默
	time.Sleep(2 * time.Millisecond)
	live.observe(liveChatEvent("b"), "text/event-stream")
	if _, _, back := live.totals(); back <= firstAt {
		t.Fatalf("content didn't move the time forward: %d then %d", firstAt, back)
	}
}

// commentOnly 只认 SSE 注释：有任何一行是内容（哪怕同一笔里还有注释）都不是心跳。
func TestCommentOnlyTellsHeartbeatsApart(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{": keepalive\n", true},
		{": keepalive\n\n", true},
		{"\n: ping\r\n\n", true},
		{"", false},
		{"\n\n", false},
		{"data: {\"a\":1}\n\n", false},
		{": keepalive\ndata: {\"a\":1}\n\n", false},
	}
	for _, c := range cases {
		if got := commentOnly([]byte(c.in)); got != c.want {
			t.Fatalf("commentOnly(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// 内容字节要按协议数到每一段真正写出来的字：正文、推理、工具名与参数。
// 数多了会把"事件多"当成"写得多"，数少了则反过来。
func TestContentBytesPerProtocol(t *testing.T) {
	cases := []struct {
		name  string
		proto provider.Protocol
		body  string
		want  int
	}{
		{"chat text", provider.Chat, `{"choices":[{"delta":{"content":"hello"}}]}`, 5},
		{"chat reasoning", provider.Chat, `{"choices":[{"delta":{"reasoning_content":"hm"}}]}`, 2},
		// 工具名与参数分两个事件来，各算各的
		{"chat tool name", provider.Chat, `{"choices":[{"delta":{"tool_calls":[{"function":{"name":"read"}}]}}]}`, 4},
		{"chat tool args", provider.Chat, `{"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{\"a\":1}"}}]}}]}`, 7},
		// 整包（非流式）回复在 message 里
		{"chat whole", provider.Chat, `{"choices":[{"message":{"content":"pong"}}]}`, 4},
		// Responses：只看 *.delta，*.done 会把同一段再说一遍
		{"responses delta", provider.Responses, `{"type":"response.output_text.delta","delta":"abc"}`, 3},
		{"responses done", provider.Responses, `{"type":"response.output_text.done","text":"abc"}`, 0},
		{"anthropic text", provider.Anthropic, `{"type":"content_block_delta","delta":{"text":"abc"}}`, 3},
		{"anthropic thinking", provider.Anthropic, `{"type":"content_block_delta","delta":{"thinking":"de"}}`, 2},
		{"anthropic tool", provider.Anthropic, `{"type":"content_block_start","content_block":{"type":"tool_use","name":"grep"}}`, 4},
		{"gemini text", provider.Gemini, `{"candidates":[{"content":{"parts":[{"text":"abc"}]}}]}`, 3},
		{"gemini call", provider.Gemini, `{"candidates":[{"content":{"parts":[{"functionCall":{"name":"ls","args":{}}}]}}]}`, 4},
		// 不认识的东西一律 0：宁可少算，不可算错
		{"not json", provider.Chat, `data: [DONE]`, 0},
		{"empty", provider.Chat, `{}`, 0},
	}
	for _, c := range cases {
		if got := liveContentOf(c.proto, []byte(c.body)); got != c.want {
			t.Fatalf("%s: %s gave %d, want %d", c.name, c.body, got, c.want)
		}
	}
}

// 一条 SSE 流被拆成任意大小的字节写入（TCP 与 UTF-8 都会切），数出来的内容
// 必须与整条流一次写入一样——否则分片越碎、估算越飘。
func TestContentCounterSurvivesSplitWrites(t *testing.T) {
	stream := append(append(liveChatEvent("hello"), liveChatEvent(" world")...), []byte(": keepalive\n\n")...)

	whole := &liveContentCounter{proto: provider.Chat}
	whole.add(provider.Chat, "text/event-stream", stream)
	if got := whole.flush(); got != 11 {
		t.Fatalf("written whole: %d content bytes, want 11", got)
	}

	cut := &liveContentCounter{proto: provider.Chat}
	for i := 0; i < len(stream); i += 7 {
		end := i + 7
		if end > len(stream) {
			end = len(stream)
		}
		cut.add(provider.Chat, "text/event-stream", stream[i:end])
	}
	if got := cut.flush(); got != 11 {
		t.Fatalf("written in 7-byte pieces: %d content bytes, want 11", got)
	}
}

// 整包回复（非流式）没有事件可数，在结束时数一次。
func TestContentCounterCountsAWholeReply(t *testing.T) {
	c := &liveContentCounter{proto: provider.Chat}
	c.add(provider.Chat, "application/json", []byte(`{"choices":[{"message":{"content":"pong"}}]}`))
	if got := c.flush(); got != 4 {
		t.Fatalf("a whole reply gave %d content bytes, want 4", got)
	}
}

// 样本够 3 组才估；估的是累计比例，向下取整；样本永远不从估算来。
func TestRatesLearnThenEstimate(t *testing.T) {
	quietRates(t)

	liveRates.learn("vendor", "m", 100, 10)
	if got := liveRates.estimate("vendor", "m", 500); got != 0 {
		t.Fatalf("one sample must not estimate, got %d", got)
	}
	liveRates.learn("vendor", "m", 100, 10)
	liveRates.learn("vendor", "m", 200, 20)
	// 累计 400 内容字节 / 40 tokens：10 字节一个 token
	if got := liveRates.estimate("vendor", "m", 950); got != 95 {
		t.Fatalf("950 content bytes at 10:1 is 95 tokens, got %d", got)
	}
	// 换个上游就是另一组：还没学
	if got := liveRates.estimate("other", "m", 950); got != 0 {
		t.Fatalf("another provider was estimated from this one's samples: %d", got)
	}
	// 没有内容就没什么可估
	if got := liveRates.estimate("vendor", "m", 0); got != 0 {
		t.Fatalf("no content estimated %d tokens", got)
	}
}

// 学到的样本要能跨重启：写进文件、读回来还在。
func TestRatesSurviveARestart(t *testing.T) {
	quietRates(t)
	liveRates.learn("vendor", "m", 300, 30)
	liveRates.learn("vendor", "m", 300, 30)
	liveRates.learn("vendor", "m", 300, 30)

	// 换一份内存里的表，只留文件：等同于进程重启
	liveRates.mu.Lock()
	liveRates.m, liveRates.loaded = map[string]rateSample{}, false
	liveRates.mu.Unlock()

	if got := liveRates.estimate("vendor", "m", 600); got != 60 {
		t.Fatalf("after a restart 600 content bytes at 10:1 is 60 tokens, got %d", got)
	}
}

// 失败的和没有 usage 的回复不学：学了会把比例带偏。被打断的学——usage 到了
// 就说明厂商侧把回复写完了，客户端提前断开只可能少记最后几笔内容字节；
// 不学会让这类客户端的组合永远凑不满样本。
func TestRatesLearnFromRepliesTheVendorFinished(t *testing.T) {
	quietRates(t)

	for _, c := range []struct {
		reason string
		vendor string
		output int
		learn  bool
	}{
		{"failed", "v-failed", 10, false},      // 上游给的是错误状态：样本不代表正常回复
		{"interrupted", "v-nousage", 0, false}, // 客户端走得太早，usage 没到：没有 token 可学
		{"complete", "v-nousage", 0, false},    // 没有 usage：不知道多少个 token
		{"complete", "v-ok", 10, true},
		{"interrupted", "v-gone", 10, true}, // usage 已到：厂商侧回复是完整的
	} {
		live := liveBegin(nil, "m", provider.Chat)
		live.routed(c.vendor, "m")
		live.observe(liveChatEvent("hello"), "text/event-stream")
		live.finish(c.reason, c.output)
		liveRates.mu.Lock()
		got := liveRates.m[c.vendor+"/m"]
		liveRates.mu.Unlock()
		if c.learn != (got.Seen == 1) {
			t.Fatalf("%s with output %d: seen %d, want learned=%v", c.reason, c.output, got.Seen, c.learn)
		}
		if c.learn && (got.Bytes != 5 || got.Output != 10) {
			t.Fatalf("%s was learned wrong: %+v", c.reason, got)
		}
	}
}

// 网关自己的辅助请求没有记录：所有方法在 nil 上都安全，什么也不做。
func TestLiveIsOptional(t *testing.T) {
	var live *liveRow
	live.observe([]byte("x"), "")
	live.routed("p", "m")
	live.finish("failed", 0)
	if b, c, at := live.totals(); b != 0 || c != 0 || at != 0 {
		t.Fatalf("a nil record counted something: %d, %d, %d", b, c, at)
	}

	if liveOf(context.Background()) != nil {
		t.Fatal("a context without a record has one")
	}
	live = liveBegin(nil, "m", provider.Chat)
	if got := liveOf(withLive(context.Background(), live)); got != live {
		t.Fatal("the record put in the context isn't the one taken out")
	}
}

// 路由页「谁回答」那一格要说的就是同一个数：写入同时落到这次请求的路由上，
// 心跳只加字节、不动最近内容时间（否则一卡住的回复看起来也一直在动）。
func TestLiveWritesReachTheRoute(t *testing.T) {
	quietRates(t)

	rt := &Route{Agent: "a", Model: "m", Time: time.Now()}
	f := &inflight{tr: &trace{}}
	f.route.Store(rt)

	live := liveBegin(f, "m", provider.Chat)
	live.routed("vendor", "m")
	first := liveChatEvent("hello")
	live.observe(first, "text/event-stream")
	if rt.Bytes != int64(len(first)) || rt.ContentAt == 0 {
		t.Fatalf("the route didn't hear the first bytes: %+v", rt)
	}
	content := rt.ContentAt

	beat := []byte(": keepalive\n")
	live.observe(beat, "text/event-stream")
	if want := int64(len(first) + len(beat)); rt.Bytes != want {
		t.Fatalf("the heartbeat's bytes didn't reach the route: %d, want %d", rt.Bytes, want)
	}
	if rt.ContentAt != content {
		t.Fatalf("a heartbeat moved the route's content time: %d then %d", content, rt.ContentAt)
	}

	// 路由在这一笔之前还没开始：漏掉的那几笔由后面的总量补上
	f2 := &inflight{tr: &trace{}}
	late := liveBegin(f2, "m", provider.Chat)
	late.observe(first, "text/event-stream")
	lateRoute := &Route{Agent: "a", Model: "m", Time: time.Now()}
	f2.route.Store(lateRoute)
	late.observe(first, "text/event-stream")
	if want := int64(2 * len(first)); lateRoute.Bytes != want {
		t.Fatalf("a route begun late got %d bytes, want the running total %d", lateRoute.Bytes, want)
	}
}

// 估算跟着样本走：样本够了，路由上就有 estTok；不够，路由上就是 0（页面就不
// 显示 tokens 那一格），而不是一个编出来的数。
func TestRouteGetsTheEstimateOnlyOnceItIsCalibrated(t *testing.T) {
	quietRates(t)

	rt := &Route{Agent: "a", Model: "m", Time: time.Now()}
	f := &inflight{tr: &trace{}}
	f.route.Store(rt)
	live := liveBegin(f, "m", provider.Chat)
	live.routed("vendor", "m")
	live.observe(liveChatEvent("hello"), "text/event-stream")
	if rt.EstTok != 0 {
		t.Fatalf("estimated %d tokens with no sample at all", rt.EstTok)
	}

	// 三次完整成功之后（每次 5 内容字节、2 tokens），同一条路由立刻能估
	for i := 0; i < liveRateMin; i++ {
		done := liveBegin(nil, "m", provider.Chat)
		done.routed("vendor", "m")
		done.observe(liveChatEvent("hello"), "text/event-stream")
		done.finish("complete", 2)
	}
	live.observe(liveChatEvent("hello"), "text/event-stream")
	// 这条路由上已经攒了 10 内容字节：样本是 15 字节 6 token，10×6/15 = 4
	if rt.EstTok != 4 {
		t.Fatalf("10 content bytes at 2.5 bytes a token is 4; got %d", rt.EstTok)
	}
}
