// 接收监控页：网关此刻在收哪些回复、各自收到多少、多久没动静。
//
// 数据只来自身源 GET /api/gateway/recv（internal/gui/trace.go）。网关在把
// 每个响应字节写给客户端时顺手记一笔（internal/gateway/live.go），所以客户端
// 不用改 base URL，也没有 WebSocket：页面每 500ms 问一次，看不见时一次都不问。
//
// 这里只显示网关确实记下的数字：没有百分比（没人知道还要多久）、没有估算的
// token（厂商的 usage 到了才有），静默也只是"没有新字节"，不是失败。
magpie.plugin("recv", (ctx) => {
  const EVERY = 500;

  // 行按 id 复用：每次重建 DOM 会把读者的滚动位置和正在读的那一行弄丢
  const rows = new Map();
  let main = null;
  let ui = {};
  let alive = false, timer = null, busy = false;

  const bytes = (n) => {
    if (!n) return "0 B";
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
    return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
  };

  // 秒数取整：0.4 秒和 12 秒用同一个说法，读者不用换算
  const secs = (then, now) => (then ? Math.max(0, Math.round((now - then) / 1000)) : 0);

  const phaseText = (rec, now) => {
    switch (rec.phase) {
      case "receiving": return t("Receiving");
      case "quiet": return t("Quiet for {n} s", { n: secs(rec.contentAt, now) });
      default: return t("Waiting for the first content");
    }
  };

  const endText = (reason) => {
    switch (reason) {
      case "complete": return t("Done");
      case "failed": return t("Failed");
      case "interrupted": return t("Interrupted");
      default: return reason;
    }
  };

  function rowFor(rec) {
    let row = rows.get(rec.id);
    if (row) return row;
    const el = document.createElement("div");
    el.className = "recv-row";
    const cell = (cls) => {
      const s = document.createElement("span");
      s.className = cls;
      el.append(s);
      return s;
    };
    row = {
      el,
      dot: cell("recv-dot"),
      model: cell("recv-model"),
      where: cell("recv-where"),
      phase: cell("recv-phase"),
      bytes: cell("recv-bytes"),
      tokens: cell("recv-tokens"),
      recent: cell("recv-recent"),
    };
    rows.set(rec.id, row);
    return row;
  }

  function fill(row, rec, now, ended) {
    // 供应商和 Agent 是"谁在答"，模型是"答的什么"：模型缺失时说未知，不猜
    row.model.textContent = rec.model || t("Model unknown");
    row.where.textContent = [rec.provider, rec.agent].filter(Boolean).join(" · ");
    row.dot.dataset.phase = ended ? rec.endReason : rec.phase;
    row.phase.textContent = ended ? endText(rec.endReason) : phaseText(rec, now);
    row.bytes.textContent = bytes(rec.bytes);
    // 厂商的 usage 到了就写实数，没到就写估算（带 ≈）；连样本都还没有时
    // 什么也不写——0 会被读成"没写东西"，而那正是这一页要说的事。
    row.tokens.textContent = rec.output ? t("{n} tokens", { n: rec.output })
      : rec.estimate ? t("≈ {n} tokens", { n: rec.estimate }) : "—";
    row.recent.textContent = ended
      ? t("ended {n} s ago", { n: secs(rec.ended, now) })
      : rec.contentAt ? t("{n} s since the last content", { n: secs(rec.contentAt, now) }) : "—";
  }

  function draw(d) {
    const now = d.now || Date.now();
    const active = Array.isArray(d.active) ? d.active : [];
    const recent = Array.isArray(d.recent) ? d.recent : [];

    const kept = new Set();
    for (const rec of active) {
      kept.add(rec.id);
      const row = rowFor(rec);
      fill(row, rec, now, false);
      ui.activeList.append(row.el);
    }
    // 最近结束的新的在前：先摆完再删，同一个元素被移到这一头也不会闪
    for (const rec of recent) {
      kept.add(rec.id);
      const row = rowFor(rec);
      fill(row, rec, now, true);
      ui.endedList.append(row.el);
    }
    for (const [id, row] of rows) {
      if (!kept.has(id)) {
        row.el.remove();
        rows.delete(id);
      }
    }

    const quiet = active.filter((r) => r.phase === "quiet").length;
    ui.count.textContent = t("{n} receiving", { n: active.length - quiet });
    ui.quiet.textContent = quiet ? t("{n} quiet", { n: quiet }) : "";
    ui.empty.hidden = active.length + recent.length > 0;
    ui.activeSection.hidden = active.length === 0;
    ui.endedSection.hidden = recent.length === 0;
    ui.records.textContent = t("{n} records", { n: recent.length });
  }

  async function load() {
    if (busy) return;
    busy = true;
    // 网关重启或网络抖动时，一个悬挂的请求会让 busy 永远为真，这一页从此
    // 定格在旧数字上——看上去像"没有新请求"，其实是页面自己不问了。
    // 所以每次询问都有上限：超时就当这次没问到，下一轮照常再问。
    const ctl = new AbortController();
    const cut = setTimeout(() => ctl.abort(), 4000);
    try {
      const res = await fetch("/api/gateway/recv", {
        headers: { accept: "application/json" },
        signal: ctl.signal,
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      draw(await res.json());
      ui.note.textContent = "";
    } catch (err) {
      // 说"取不到数字"，而不是显示 0 条：断连和没有请求不是一回事
      ui.note.textContent = t("The gateway's numbers aren't reachable right now: {why}", {
        why: ctl.signal.aborted ? "timeout" : String(err?.message || err),
      });
    } finally {
      clearTimeout(cut);
      busy = false;
    }
  }

  // 只在页面看得见时问：藏起来的页面每秒两次地取一屏没人看的数字没有意义
  const inSight = () => alive && main && !main.hidden && !document.hidden;
  async function tick() {
    timer = null;
    if (!alive) return;
    if (inSight()) await load();
    if (alive) timer = setTimeout(tick, EVERY);
  }
  const start = () => {
    if (alive) return;
    alive = true;
    tick();
  };
  const stop = () => {
    alive = false;
    if (timer) clearTimeout(timer);
    timer = null;
  };

  const html = `
  <div class="recv">
    <div class="recv-head">
      <span class="recv-count" id="recvCount"></span>
      <span class="recv-quiet" id="recvQuiet"></span>
      <span class="recv-grow"></span>
      <span class="recv-note" id="recvNote" role="status"></span>
    </div>
    <div class="recv-empty" id="recvEmpty" hidden>
      <div class="recv-empty-title" id="recvEmptyTitle"></div>
      <div class="recv-empty-sub" id="recvEmptySub"></div>
    </div>
    <div class="recv-section" id="recvActiveSection" hidden>
      <div class="recv-section-title" id="recvActiveTitle"></div>
      <div class="recv-list" id="recvActive"></div>
    </div>
    <div class="recv-section" id="recvEndedSection" hidden>
      <div class="recv-section-title" id="recvEndedTitle"></div>
      <div class="recv-list" id="recvEnded"></div>
    </div>
    <div class="recv-foot">
      <span id="recvFootNote"></span>
      <span class="recv-grow"></span>
      <span id="recvRecords"></span>
    </div>
  </div>`;

  // 页签交给内核：它会新建 <main id="view-recv">，把 html 放进去，
  // 之后由内核按 open/refresh/leave 调我们
  ctx.slots.add("page", {
    key: "recv",
    title: "Receive monitor",
    html,
    mount(node) {
      main = node;
      const byId = (id) => node.querySelector("#" + id);
      ui = {
        count: byId("recvCount"), quiet: byId("recvQuiet"), note: byId("recvNote"),
        empty: byId("recvEmpty"), records: byId("recvRecords"),
        activeSection: byId("recvActiveSection"), endedSection: byId("recvEndedSection"),
        activeList: byId("recvActive"), endedList: byId("recvEnded"),
      };
      byId("recvEmptyTitle").textContent = t("Nothing is going through the gateway right now");
      byId("recvEmptySub").textContent = t("Every request an agent sends through magpie shows up here while its reply is arriving.");
      byId("recvActiveTitle").textContent = t("Receiving");
      byId("recvEndedTitle").textContent = t("Recently ended");
      byId("recvFootNote").textContent = t("Quiet is not failure");
      document.addEventListener("visibilitychange", () => {
        if (inSight()) load();
      });
    },
    open() { start(); },
    refresh() { if (alive) load(); },
    leave() { stop(); },
  });
});
