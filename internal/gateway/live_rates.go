package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yetone/magpie/internal/appdir"
)

// 接收监控的 tokens 估算：厂商的 usage 常常到最后一个事件才到，在那之前
// 页面上只能写"估算"。这一层学「这家上游的这个模型，每输出一个 token
// 大约写多少内容字节」，按 up 家 + 模型分组学。
//
// 三条纪律，都是"不编数"：
//   - 只从**完整成功、且厂商真的报了输出 tokens**的请求上学（无 usage、
//     失败、被截断的都不学，见 liveRow.finish）；
//   - 估算永远不当样本，否则误差会自己繁殖；
//   - 样本不够（默认 3 组）就不估：页面写"待校准"，而不是 0，也不是猜。
//
// 数字只存在本机配置目录：没有正文、没有提示词、没有账号，只有
// provider+model 与两个累计值。
const (
	liveRateMin  = 3   // 少于这么多组就不估
	liveRateKeys = 256 // 最多记这么多组（上游 + 模型），超了丢最旧的
)

// rateSample 是一组"内容字节 → 输出 tokens"的累计。存累计而不是逐条样本：
// 估的是比例，累计和就够了，也不必把每次请求都留在文件里。
type rateSample struct {
	Bytes  int64 `json:"bytes"`
	Output int64 `json:"output"`
	Seen   int   `json:"seen"`
	At     int64 `json:"at"` // 最后一次学的时间，用来丢最旧的组
}

type tokenRates struct {
	mu     sync.Mutex
	m      map[string]rateSample
	loaded bool
}

// liveRates 是网关里唯一的一份：学是请求结束时的低频动作，估是热路径上的
// 一次乘除，共用一个锁不会有争用。
var liveRates = &tokenRates{m: map[string]rateSample{}}

// liveRatesFile 是它落盘的位置。变量而非常量：测试指向临时目录，
// 绝不写进用户的真实配置。
var liveRatesFile = func() string { return filepath.Join(appdir.Config(), "receive-tokens.json") }

// key 是分组的键：同一个模型换个上游就是另一组（不同厂商的分词与封装都不同）。
func rateKey(provider, model string) string {
	if provider == "" || model == "" {
		return ""
	}
	return provider + "/" + model
}

// estimate 把内容字节换算成输出 tokens，样本不够时返回 0（页面说"待校准"）。
func (t *tokenRates) estimate(provider, model string, content int64) int {
	key := rateKey(provider, model)
	if key == "" || content <= 0 {
		return 0
	}
	t.mu.Lock()
	t.load()
	s, ok := t.m[key]
	t.mu.Unlock()
	if !ok || s.Seen < liveRateMin || s.Bytes <= 0 || s.Output <= 0 {
		return 0
	}
	// 向下取整：宁可少说，不要多说
	return int(content * s.Output / s.Bytes)
}

// learn 记一组样本：content 是这条回复的内容字节，output 是厂商报的输出
// tokens（含推理）。调用方保证这是一条完整成功、数字可信的回复。
func (t *tokenRates) learn(provider, model string, content int64, output int) {
	key := rateKey(provider, model)
	if key == "" || content <= 0 || output <= 0 {
		return
	}
	now := time.Now().UnixMilli()
	t.mu.Lock()
	t.load()
	s := t.m[key]
	s.Bytes += content
	s.Output += int64(output)
	s.Seen++
	s.At = now
	t.m[key] = s
	t.prune()
	err := t.save()
	t.mu.Unlock()
	_ = err // 写不下去就只留内存：估算还能用，重启后重新学
}

// prune 丢掉最旧的组，别让文件无限长。
func (t *tokenRates) prune() {
	for len(t.m) > liveRateKeys {
		oldest, idle := "", int64(1<<62)
		for k, s := range t.m {
			if s.At < idle {
				oldest, idle = k, s.At
			}
		}
		delete(t.m, oldest)
	}
}

// load 只在第一次用到时读文件；读坏了就当没有（重新学就是了）。
func (t *tokenRates) load() {
	if t.loaded {
		return
	}
	t.loaded = true
	path := liveRatesFile()
	if path == "" {
		return
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var f struct {
		Groups map[string]rateSample `json:"groups"`
	}
	if json.Unmarshal(b, &f) != nil {
		return
	}
	for k, v := range f.Groups {
		if v.Bytes > 0 && v.Output > 0 && v.Seen > 0 {
			t.m[k] = v
		}
	}
}

// save 原子替换：先写同目录的临时文件再 rename。Magpie 读半截文件的
// 事故已经出过一次（plugins.json），这里不重犯。
func (t *tokenRates) save() error {
	path := liveRatesFile()
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(struct {
		Groups map[string]rateSample `json:"groups"`
	}{t.m})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".receive-tokens-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

// resetRates 只给测试用：把学到的清空（并把标记设为已加载，免得读真实文件）。
func resetRates() {
	liveRates.mu.Lock()
	liveRates.m = map[string]rateSample{}
	liveRates.loaded = true
	liveRates.mu.Unlock()
}
