package gateway

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

// 接收监控：网关把响应字节写给客户端时顺手记一份，页面每几百毫秒取一次快照。
// 观测点放在 captureResponseWriter（每个请求都经过它），所以不需要客户端改地址，
// 也不会改变任何字节：这里只有计数，不缓存、不解析正文。
//
// 一条记录只答三个问题：现在在哪一步、一共收到多少字节、最近一次收到**内容**
// 是什么时候。静默（很久没有新内容）不是失败，所以它由快照时的时间差算出来，
// 而不是写进状态。
//
// 时间一律用毫秒时间戳：页面直接相减，不必解析时间格式。

// liveRecent 是页面能看到的已结束请求条数，够滚动就够了。
const liveRecent = 50

// liveQuiet 之后没有新内容就显示"静默"。上游在思考时本来就不发内容，
// 所以这只是提示，不是结论。
const liveQuiet = 15 * time.Second

// LiveRequest 是一次经过网关的请求，页面上的一行。
type LiveRequest struct {
	ID       int64  `json:"id"`
	Agent    string `json:"agent,omitempty"`
	Model    string `json:"model,omitempty"`
	Provider string `json:"provider,omitempty"`
	// Phase 是进行中的阶段：waiting（已接入，还没有内容）、receiving（在收）、
	// quiet（超过 liveQuiet 没有新内容）；结束后 Phase 就是 EndReason。
	Phase string `json:"phase"`
	// Bytes 是写给客户端的全部字节，含 SSE 封装与网关自己的心跳注释；
	// 它是"这条回复有多大"，不是"回复长了多少"。
	Bytes int64 `json:"bytes"`
	// Content 是这些字节里的**内容**：正文、推理、工具调用的名字与参数，
	// 不含 JSON/SSE 封装。估 tokens 用它——封装按事件数涨，内容才是模型写的。
	Content int64 `json:"content,omitempty"`
	Output  int   `json:"output,omitempty"` // 厂商 usage 报的输出 tokens，没到就是 0
	// Estimate 是内容换算出的输出 tokens（未到 usage 时的估算）。它由这个
	// 上游这个模型自己的字节/token 比得出，样本不够时是 0（页面说"待校准"，
	// 而不是编一个数）。
	Estimate int `json:"estimate,omitempty"`
	Status   int `json:"status,omitempty"`
	// Started 是接入时间，FirstByteAt 是第一个字节的时间（等待上游时为 0），
	// ContentAt 是最近一次收到**内容**的时间（心跳不算），Ended 是结束时间
	Started     int64 `json:"started"`
	FirstByteAt int64 `json:"firstByteAt,omitempty"`
	ContentAt   int64 `json:"contentAt,omitempty"`
	Ended       int64 `json:"ended,omitempty"`
	// EndReason: complete（正常结束）、failed（网关给出了错误状态）、
	// interrupted（客户端断开）
	EndReason string `json:"endReason,omitempty"`
}

// liveRow 是一条进行中的记录：Write 在网关的热路径上，所以只加锁改几个数。
type liveRow struct {
	mu    sync.Mutex
	rec   LiveRequest
	ended bool
	// f 是这条请求在网关里的在飞记录：它上面挂着这次请求的路由（Route），
	// 路由页要靠它把字节数与最近内容时间写回「谁回答」那一格。为 nil 时
	// 只做本页自己的计数（网关自己的辅助请求、或没被计数的请求）。
	f *inflight
	// content 数内容字节（live_content.go）：估 tokens 的原料。它只认一种
	// 协议，也就是客户端这一侧的协议。
	content *liveContentCounter
}

// liveRegistry 是网关里所有进行中的请求，和最近结束的几条。
// 已结束的也存指针：写入端在结束时可能还有几个字节（编码器收尾），
// 快照时再读，才不会漏计。
var liveRegistry = struct {
	sync.Mutex
	seq    int64
	open   map[int64]*liveRow
	recent []*liveRow
}{open: map[int64]*liveRow{}}

// liveBegin 在 serve 开始时登记一条请求，返回的 row 交给 captureResponseWriter。
// f 是它在网关里的在飞记录（可为 nil），用来把进度写回这条请求的路由；
// from 是客户端这一侧的协议，内容字节按它来数。
func liveBegin(f *inflight, agent, model string, from provider.Protocol) *liveRow {
	now := time.Now().UnixMilli()
	liveRegistry.Lock()
	liveRegistry.seq++
	row := &liveRow{f: f, content: &liveContentCounter{proto: from}, rec: LiveRequest{
		ID:      liveRegistry.seq,
		Agent:   agent,
		Model:   model,
		Phase:   "waiting",
		Started: now,
	}}
	liveRegistry.open[row.rec.ID] = row
	liveRegistry.Unlock()
	return row
}

// observe 吃一笔写给客户端的字节：字节数、最近内容时间、内容字节与 tokens
// 估算都在这里更新，然后把进度报给这条请求的路由（路由页的行与「谁回答」）。
//
// 三个数各说各的：Bytes 含封装与心跳（"这条回复多大"），Content 只算模型的
// 文字（估 tokens 用），ContentAt 只被内容刷新（"多久没动静"）。心跳只加
// Bytes——否则一个卡住的回复永远显示正在接收，而那正是这一页要说的事。
func (l *liveRow) observe(p []byte, contentType string) {
	if l == nil || len(p) == 0 {
		return
	}
	now := time.Now().UnixMilli()
	heartbeat := commentOnly(p)
	added := l.content.add(l.content.proto, contentType, p)

	l.mu.Lock()
	l.rec.Bytes += int64(len(p))
	if l.rec.FirstByteAt == 0 {
		l.rec.FirstByteAt = now
	}
	if !heartbeat {
		l.rec.ContentAt = now
	}
	l.rec.Content += added
	rec := l.rec
	l.mu.Unlock()

	l.tellRoute(rec)
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

// tellRoute 把进度报给这条请求的路由：路由页的行与「谁回答」那一格都读它。
// 路由可能还没开始（请求刚接入时），那就等下一笔——数字是总量，后面的写入
// 会补上，不会漏。
func (l *liveRow) tellRoute(rec LiveRequest) {
	if l.f == nil || l.f.tr == nil {
		return
	}
	rt := l.f.route.Load()
	if rt == nil {
		return
	}
	est := liveRates.estimate(rec.Provider, rec.Model, rec.Content)
	l.f.tr.liveBytes(rt, rec.Bytes, rec.ContentAt, est)
}

// routed 记下这次尝试真正去的供应商与模型：路由可能换了成员，
// 页面上要看的是最终那一个。
func (l *liveRow) routed(provider, model string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.rec.Provider = provider
	if model != "" {
		l.rec.Model = model
	}
	l.mu.Unlock()
}

// header 记下状态码，结束时用来区分"完成"和"失败"。
func (l *liveRow) header(code int) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.rec.Status = code
	l.mu.Unlock()
}

// finish 把记录挪到已结束的那一头；重复调用无副作用。
//
// 结束时也把这一条记成一组样本——条件是"上游没有失败、而且厂商真的报了
// 输出 tokens"。失败（错误状态）与没有 usage 的回复学不得，学了会把比例
// 带偏；客户端断开不算数：usage 已经到了，说明厂商侧把回复写完了（agent
// 客户端常在拿到最后一段内容后立刻断开，不等收尾的 usage 事件——这样的
// 组合若不学，永远凑不满样本）。断开早的样本内容字节会缺最后几笔，比例
// 因此略偏低，可接受。内容字节是整个请求的（含 fallback 前那一次试过的一小段），
// 与 usage 的归属略有出入，所以它只是一组样本，不是定论。
func (l *liveRow) finish(reason string, output int) {
	if l == nil {
		return
	}
	content := l.content.flush()

	l.mu.Lock()
	if l.ended {
		l.mu.Unlock()
		return
	}
	l.ended = true
	l.rec.Ended = time.Now().UnixMilli()
	l.rec.EndReason = reason
	l.rec.Phase = reason
	l.rec.Output = output
	l.rec.Content = content
	if l.rec.Status == 0 {
		l.rec.Status = 200 // 没写过头就是 200
	}
	// 估算在结束时定稿：usage 到了它只是参考，没到它就是页面能说的最好一句
	l.rec.Estimate = liveRates.estimate(l.rec.Provider, l.rec.Model, content)
	rec := l.rec
	id := rec.ID
	l.mu.Unlock()

	// 上游没失败且厂商报了 usage 就学：客户端断开不代表回复不完整（见上）。
	if reason != "failed" && output > 0 {
		liveRates.learn(rec.Provider, rec.Model, content, output)
	}
	l.tellRoute(rec)

	liveRegistry.Lock()
	delete(liveRegistry.open, id)
	liveRegistry.recent = append(liveRegistry.recent, l)
	if len(liveRegistry.recent) > liveRecent {
		liveRegistry.recent = append([]*liveRow(nil), liveRegistry.recent[len(liveRegistry.recent)-liveRecent:]...)
	}
	liveRegistry.Unlock()
}

// snapshot 给页面一条当前状态的记录：静默按最后一次**内容**到现在算，
// 只有心跳的请求仍然算"在等"。
func (l *liveRow) snapshot(now int64) LiveRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := l.rec
	if l.ended {
		return rec
	}
	switch {
	case rec.ContentAt == 0:
		rec.Phase = "waiting"
	case now-rec.ContentAt >= liveQuiet.Milliseconds():
		rec.Phase = "quiet"
	default:
		rec.Phase = "receiving"
	}
	return rec
}

// LiveSnapshot 是 /api/gateway/recv 的内容：进行中的请求（旧的在前）和最近结束的
// （新的在前），加上取快照的时间，好让页面自己算"多久没动静"。
func LiveSnapshot() (int64, []LiveRequest, []LiveRequest) {
	now := time.Now().UnixMilli()
	liveRegistry.Lock()
	open := make([]*liveRow, 0, len(liveRegistry.open))
	for _, row := range liveRegistry.open {
		open = append(open, row)
	}
	recentRows := append([]*liveRow(nil), liveRegistry.recent...)
	liveRegistry.Unlock()

	active := make([]LiveRequest, 0, len(open))
	for _, row := range open {
		active = append(active, row.snapshot(now))
	}
	sort.Slice(active, func(i, j int) bool { return active[i].Started < active[j].Started })
	recent := make([]LiveRequest, 0, len(recentRows))
	for i := len(recentRows) - 1; i >= 0; i-- {
		recent = append(recent, recentRows[i].snapshot(now))
	}
	return now, active, recent
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
