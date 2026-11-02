package gateway

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

// 接收监控：网关把响应字节写给客户端时顺手记一份，路由页的行每秒重画一次
// 进度。观测点放在 captureResponseWriter（每个响应字节都经过它），所以不需要
// 客户端改地址，也不会改变一个字节：这里只计数，不缓存正文、不解析词句。
//
// 一条记录只答三个问题：写给客户端多少字节、其中多少是**内容**、最近一次收到
// 内容是什么时候。静默（很久没有新内容）由页面拿第三个数和当下相减得出，
// 所以它不进状态，也不是失败。
//
// 时间一律是毫秒时间戳：页面直接相减，不必解析时间格式。

// liveRow 是一条进行中的记录：observe 在网关的热路径上，所以只加锁改几个数。
type liveRow struct {
	mu sync.Mutex
	// bytes 是写给客户端的全部字节，含 SSE 封装与网关自己的心跳注释：
	// 它答"这条回复有多大"，不是"回复长了多少"。
	bytes int64
	// content 是这些字节里的**内容**：正文、推理、工具调用的名字与参数，
	// 不含 JSON/SSE 封装。估 tokens 用它——封装按事件数涨，内容才是模型写的。
	content int64
	// contentAt 是最近一次收到**内容**的时间；心跳不动它，否则一个卡住的
	// 回复看起来也一直在动。
	contentAt int64
	// provider 与 model 是这次尝试真正去的那一家（routed 记），样本按它们分组。
	provider string
	model    string
	ended    bool
	// f 是这条请求在网关里的在飞记录：它上面挂着这次请求的路由（Route），
	// 进度要写到那条路由上（路由页的行与「谁回答」那一格）。为 nil 时（网关
	// 自己的辅助请求，或没被计数的请求）什么也不报。
	f *inflight
	// counter 数内容字节（live_content.go）：估 tokens 的原料。它只认一种
	// 协议，也就是客户端这一侧的协议。
	counter *liveContentCounter
}

// liveBegin 在 serve 开始时登记一条请求，返回的 row 交给 captureResponseWriter。
// f 是它在网关里的在飞记录（可为 nil），model 是这次请求要的模型（路由换了
// 成员之后由 routed 改写），from 是客户端这一侧的协议。
func liveBegin(f *inflight, model string, from provider.Protocol) *liveRow {
	return &liveRow{f: f, model: model, counter: &liveContentCounter{proto: from}}
}

// observe 吃一笔写给客户端的字节：字节数、最近内容时间、内容字节与 tokens
// 估算都在这里更新，然后把进度报给这条请求的路由。
//
// 三个数各说各的：bytes 含封装与心跳（"这条回复多大"），content 只算模型的
// 文字（估 tokens 用），contentAt 只被内容刷新（"多久没动静"）。心跳只加
// bytes——否则一个卡住的回复永远显示正在接收，而看见这件事正是这一栏的用处。
func (l *liveRow) observe(p []byte, contentType string) {
	if l == nil || len(p) == 0 {
		return
	}
	now := time.Now().UnixMilli()
	heartbeat := commentOnly(p)
	added := l.counter.add(l.counter.proto, contentType, p)

	l.mu.Lock()
	l.bytes += int64(len(p))
	if !heartbeat {
		l.contentAt = now
	}
	l.content += added
	l.mu.Unlock()

	l.tellRoute()
}

// commentOnly 判断一次写入是不是只有 SSE 注释（心跳）——空行不算内容。
// 只认注释：翻译后的协议可能用 JSON 事件当心跳，那一种这里仍算内容，
// 因为在这一层分辨它就是解析正文了，而这一层刻意不解析。
func commentOnly(p []byte) bool {
	saw := false
	for _, line := range bytes.Split(p, []byte("\n")) {
		line = bytes.TrimRight(line, "\r")
		if len(line) == 0 {
			continue
		}
		if line[0] != ':' {
			return false
		}
		saw = true
	}
	return saw
}

// totals 是这条请求此刻的三个数（调用方不持锁）：写给客户端的字节、其中的
// 内容字节、最近一段内容的时间。网关自己的辅助请求没有计数器，所以 nil 上也
// 要安全——那三个数就是 0。
func (l *liveRow) totals() (bytes, content, contentAt int64) {
	if l == nil {
		return 0, 0, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bytes, l.content, l.contentAt
}

// tellRoute 把进度报给这条请求的路由：路由页的行与「谁回答」那一格都读它。
// 路由可能还没开始（请求刚接入时），那就等下一笔——数字是总量，后面的写入
// 会补上，不会漏。
func (l *liveRow) tellRoute() {
	if l.f == nil || l.f.tr == nil {
		return
	}
	rt := l.f.route.Load()
	if rt == nil {
		return
	}
	l.mu.Lock()
	bytes, content, contentAt := l.bytes, l.content, l.contentAt
	provider, model := l.provider, l.model
	l.mu.Unlock()
	est := liveRates.estimate(provider, model, content)
	l.f.tr.liveBytes(rt, bytes, contentAt, est)
}

// routed 记下这次尝试真正去的供应商与模型：路由可能换了成员，
// 页面与样本要看的都是最终那一个。
func (l *liveRow) routed(provider, model string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.provider = provider
	if model != "" {
		l.model = model
	}
	l.mu.Unlock()
}

// finish 收尾：把内容字节定稿（缓冲区里最后一条事件也算上），并把这一条记成
// 一组样本。
//
// 样本的条件是"上游没有失败、而且厂商真的报了输出 tokens"。失败（错误状态）与
// 没有 usage 的回复学不得，学了会把比例带偏；客户端断开不算数：usage 已经到了，
// 说明厂商侧把回复写完了（agent 客户端常在拿到最后一段内容后立刻断开，不等
// 收尾的 usage 事件——这样的组合若不学，永远凑不满样本）。断开早的样本内容
// 字节会缺最后几笔，比例因此略偏低，可接受。内容字节是整个请求的（含 fallback
// 前那一次试过的一小段），与 usage 的归属略有出入，所以它只是一组样本，不是定论。
//
// 重复调用无副作用；结束后再写的字节（编码器收尾）照样计数，所以定稿之后
// 的 observe 会让 content 继续涨——那几笔也是客户端真的收到的。
func (l *liveRow) finish(reason string, output int) {
	if l == nil {
		return
	}
	total := l.counter.flush()

	l.mu.Lock()
	if l.ended {
		l.mu.Unlock()
		return
	}
	l.ended = true
	l.content = total
	provider, model, content := l.provider, l.model, l.content
	l.mu.Unlock()

	if reason != "failed" && output > 0 {
		liveRates.learn(provider, model, content, output)
	}
	l.tellRoute()
}

type liveCtxKey struct{}

// withLive 把记录放进请求上下文，路由尝试（attempt）就能告诉它去了哪一家。
func withLive(ctx context.Context, l *liveRow) context.Context {
	return context.WithValue(ctx, liveCtxKey{}, l)
}

// liveOf 取出请求的记录，没有就是 nil（网关自己的辅助请求不应被记录）。
func liveOf(ctx context.Context) *liveRow {
	l, _ := ctx.Value(liveCtxKey{}).(*liveRow)
	return l
}
