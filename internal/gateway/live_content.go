package gateway

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/yetone/magpie/internal/provider"
)

// 接收监控要显示"这条回复写到哪了"，其中 tokens 只能估算：厂商的 usage
// 往往在最后一个事件才来。估算的原料是**内容字节**——正文、推理、工具调用
// 的名字与参数——而不是写给客户端的全部字节：SSE/JSON 封装随事件数涨，同一个
// 模型的同一条回复，封装能差出几十个百分点，拿它换算 tokens 会把"事件多"
// 当成"写得多"。
//
// 所以这一层按客户端协议（也就是下游协议）逐事件数内容：翻译过的回复也数，
// 因为数的是客户端真正收到的那些字。它只读、不改字节，也不缓存正文：
// 一个事件解析完就丢掉，留下一个长度。
//
// 不认识的事件、字段或格式一律算 0 内容——宁可少算，不可算错。

// contentCap 是整包（非 SSE）能攒下的上限：超过就不再数，估算按已有内容来。
// 与 sniff.go 的 8 MiB 同一个量级，普通回复远到不了。
const contentCap = 8 << 20

// liveContentCounter 数一条回复的内容字节。
//
// 它按需要认识协议：第一次写入时看响应头决定是不是 SSE（没有头就看第一笔
// 字节像不像事件流），之后一直按那一种读。
type liveContentCounter struct {
	proto provider.Protocol
	// known: sse 已经定下来了
	known bool
	sse   bool
	// buf: 还没成整行的字节（SSE），或整包（非 SSE）
	buf []byte
	// data: 正在攒的这一条 SSE 事件的 data 行
	data []byte
	// over: 整包太大，不再攒
	over  bool
	total int64
}

// add 吃一笔下游字节，返回这一笔新增的内容字节数。
func (c *liveContentCounter) add(proto provider.Protocol, contentType string, p []byte) int64 {
	if c == nil || len(p) == 0 {
		return 0
	}
	if !c.known {
		c.begin(proto, contentType, p)
	}
	before := c.total
	if !c.sse {
		if c.over || len(c.buf)+len(p) > contentCap {
			c.over, c.buf = true, nil
			return 0
		}
		c.buf = append(c.buf, p...)
		return 0 // 整包只在结束时数一次
	}
	c.buf = append(c.buf, p...)
	for {
		i := bytes.IndexByte(c.buf, '\n')
		if i < 0 {
			break
		}
		c.line(bytes.TrimSuffix(c.buf[:i], []byte{'\r'}))
		c.buf = c.buf[i+1:]
	}
	if len(c.buf) > 1<<20 { // 一行长得不像话，不要它
		c.buf, c.data = nil, nil
	}
	return c.total - before
}

// begin 定下这是不是 SSE：响应头说了算，头里没有就看第一笔字节的样子。
func (c *liveContentCounter) begin(proto provider.Protocol, contentType string, p []byte) {
	c.proto, c.known = proto, true
	switch {
	case strings.Contains(strings.ToLower(contentType), "text/event-stream"):
		c.sse = true
	case strings.Contains(strings.ToLower(contentType), "json"):
		c.sse = false
	default:
		// 没有头（少见）：事件流的第一笔一定以 data:/event:/: 开头
		c.sse = bytes.HasPrefix(p, []byte("data:")) || bytes.HasPrefix(p, []byte("event:")) || bytes.HasPrefix(p, []byte(":"))
	}
}

func (c *liveContentCounter) line(line []byte) {
	if len(line) == 0 {
		c.event()
		return
	}
	rest, ok := bytes.CutPrefix(line, []byte("data:"))
	if !ok {
		return
	}
	rest = bytes.TrimPrefix(rest, []byte{' '})
	if len(c.data)+len(rest)+1 > 1<<20 {
		c.data = nil
		return
	}
	if len(c.data) > 0 {
		c.data = append(c.data, '\n')
	}
	c.data = append(c.data, rest...)
	if json.Valid(bytes.TrimSpace(c.data)) {
		c.event()
	}
}

// event 数一条事件，然后清空——正文不留在内存里。
func (c *liveContentCounter) event() {
	if len(c.data) > 0 {
		c.total += int64(liveContentOf(c.proto, bytes.TrimSpace(c.data)))
		c.data = nil
	}
}

// flush 收尾：整包回复在这里数一次，残留的最后一条事件也补上。
func (c *liveContentCounter) flush() int64 {
	if c == nil {
		return 0
	}
	if c.sse {
		if len(c.buf) > 0 {
			c.line(bytes.TrimSuffix(c.buf, []byte{'\r'}))
			c.buf = nil
		}
		c.event()
		return c.total
	}
	if len(c.buf) > 0 && !c.over {
		c.total += int64(liveContentOf(c.proto, bytes.TrimSpace(c.buf)))
		c.buf = nil
	}
	return c.total
}

// liveContentOf 数一个事件（或整包）里内容的字节数，按协议取字段。
func liveContentOf(proto provider.Protocol, b []byte) int {
	if len(b) == 0 || b[0] != '{' {
		return 0
	}
	switch proto {
	case provider.Chat:
		return liveChatContent(b)
	case provider.Responses:
		return liveResponsesContent(b)
	case provider.Gemini:
		return liveGeminiContent(b)
	default:
		return liveAnthropicContent(b)
	}
}

// 一条 Chat 的增量：delta 里的正文、推理与工具调用；整包回复在 message 里。
type liveChatDelta struct {
	Content          json.RawMessage    `json:"content"`
	ReasoningContent string             `json:"reasoning_content"`
	Reasoning        string             `json:"reasoning"`
	ToolCalls        []liveChatToolCall `json:"tool_calls"`
}

type liveChatToolCall struct {
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func liveChatContent(b []byte) int {
	var v struct {
		Choices []struct {
			Delta   liveChatDelta `json:"delta"`
			Message liveChatDelta `json:"message"`
		} `json:"choices"`
		// 有的中转把增量放在顶层
		Delta liveChatDelta `json:"delta"`
	}
	if json.Unmarshal(b, &v) != nil {
		return 0
	}
	n := liveChatDeltaLen(v.Delta)
	for _, ch := range v.Choices {
		n += liveChatDeltaLen(ch.Delta) + liveChatDeltaLen(ch.Message)
	}
	return n
}

func liveChatDeltaLen(d liveChatDelta) int {
	n := len(d.ReasoningContent) + len(d.Reasoning) + liveContentLen(d.Content)
	for _, tc := range d.ToolCalls {
		n += len(tc.Function.Name) + len(tc.Function.Arguments)
	}
	return n
}

// liveContentLen 数 content 字段：字符串，或 [{"type":"text","text":"…"}] 这种分片。
func liveContentLen(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	switch raw[0] {
	case '"':
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return len(s)
		}
	case '[':
		var parts []struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(raw, &parts) == nil {
			n := 0
			for _, p := range parts {
				n += len(p.Text)
			}
			return n
		}
	}
	return 0
}

// Responses 的流式事件只认 *.delta：*.done 会把同一段正文再说一遍，数了就翻倍。
func liveResponsesContent(b []byte) int {
	var v struct {
		Type  string          `json:"type"`
		Delta json.RawMessage `json:"delta"`
		Item  struct {
			Type string `json:"type"`
			Name string `json:"name"`
		} `json:"item"`
	}
	if json.Unmarshal(b, &v) != nil {
		return 0
	}
	if strings.HasSuffix(v.Type, ".delta") {
		return liveContentLen(v.Delta)
	}
	// 工具调用开始时只有名字（参数随 delta 来）
	if v.Type == "response.output_item.added" && v.Item.Type == "function_call" {
		return len(v.Item.Name)
	}
	return 0
}

// Anthropic 的增量在 content_block_delta 里：正文、思考、工具参数的 JSON 片段。
func liveAnthropicContent(b []byte) int {
	var v struct {
		Type  string `json:"type"`
		Delta struct {
			Text        string `json:"text"`
			Thinking    string `json:"thinking"`
			PartialJSON string `json:"partial_json"`
		} `json:"delta"`
		ContentBlock struct {
			Type string `json:"type"`
			Name string `json:"name"`
		} `json:"content_block"`
		// 整包回复
		Content []struct {
			Type     string          `json:"type"`
			Text     string          `json:"text"`
			Thinking string          `json:"thinking"`
			Name     string          `json:"name"`
			Input    json.RawMessage `json:"input"`
		} `json:"content"`
	}
	if json.Unmarshal(b, &v) != nil {
		return 0
	}
	n := len(v.Delta.Text) + len(v.Delta.Thinking) + len(v.Delta.PartialJSON)
	if v.ContentBlock.Type == "tool_use" {
		n += len(v.ContentBlock.Name)
	}
	for _, c := range v.Content {
		n += len(c.Text) + len(c.Thinking)
		if c.Type == "tool_use" {
			n += len(c.Name) + len(c.Input)
		}
	}
	return n
}

// Gemini 的每个分片只带新写的那几个 part。
func liveGeminiContent(b []byte) int {
	var v struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text         string `json:"text"`
					FunctionCall struct {
						Name string          `json:"name"`
						Args json.RawMessage `json:"args"`
					} `json:"functionCall"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if json.Unmarshal(b, &v) != nil {
		return 0
	}
	n := 0
	for _, c := range v.Candidates {
		for _, p := range c.Content.Parts {
			n += len(p.Text)
			if p.FunctionCall.Name != "" || len(p.FunctionCall.Args) > 0 {
				n += len(p.FunctionCall.Name) + len(p.FunctionCall.Args)
			}
		}
	}
	return n
}
