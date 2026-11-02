// Package recv is the Receive monitor page: every request the gateway is
// carrying now, how much of its reply has arrived, and how long ago.
//
// It keeps no counters of its own: the ones it reads are kept by the
// gateway as each reply is written (internal/gateway/live.go), so watching
// costs a few numbers per write and changes not a byte of what an agent
// gets. That is why nothing has to point its base URL elsewhere.
package recv

import "github.com/yetone/magpie/internal/pluginrt"

func init() {
	pluginrt.Register(pluginrt.Manifest{
		ID: "recv", API: "^1", Requires: []string{"window", "gateway"},
		Name:   "Receive monitor",
		Sub:    "Every request the gateway is carrying now: how much of its reply has arrived, and how long ago",
		Parts:  []pluginrt.Part{{Slot: "nav.page", Key: "recv"}},
		Client: []string{"i18n.js", "recv.css", "client.js"},
		// 和「网关」「用量」这些页一样算必需：它是网关运行时的一页读数，
		// 不是可选功能；关掉它页面会消失，而监控本身不产生任何流量。
		Essential: true,
	})
}
