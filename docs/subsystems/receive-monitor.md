# Receive monitor

The Routing page answers *how much of a reply has arrived, and how long ago did
the model last write something* for every request the gateway is still carrying.
It reads nothing from the client and changes nothing in the reply: the numbers
come from counting the bytes the gateway itself writes back.

## Responsibilities and boundaries

In scope:

- the three numbers a request still out shows in the Routing page — bytes
  received, estimated output tokens, and the age of the last content — carried
  on its `Route` (`internal/gateway/trace.go`);
- the estimate for output tokens until the vendor reports them.

Out of scope:

- a page, a nav entry or a snapshot endpoint of its own: the numbers live where
  the request already is;
- the request body, headers, keys or any reply text kept anywhere: content bytes
  are counted and dropped, never stored;
- a percentage or an ETA: nobody knows how long a reply will take;
- history across restarts: nothing about a request is kept past its `Route`
  (the learned token rates do survive, see below);
- requests answered by the Codex backend chain (`internal/gateway/codex_backend.go`,
  its own hold writer), which does not pass the capture writer these numbers come
  from.

## Source of truth

| Fact | Source |
| --- | --- |
| Counting a request: bytes, content, clock | `internal/gateway/live.go` |
| Content bytes per protocol | `internal/gateway/live_content.go` |
| Learned tokens per content byte | `internal/gateway/live_rates.go` |
| The write that every downstream byte passes | `captureResponseWriter.Write`, `internal/gateway/capture.go` |
| Registration and finishing a request | `serve` and `attempt`, `internal/gateway/gateway.go` |
| The three fields on a route | `(*trace).liveBytes`, `internal/gateway/trace.go` |
| Routing page rendering | `liveCells`, `paintLive`, `livePaint`, `internal/gui/assets/plugins/routing/client.js` |

## Runtime path

1. `serve` builds a `Call`, then `liveBegin(inflightOf(ctx), model, from)` starts
   the counter and puts it in the request context (`withLive`). `from` is the
   client's protocol, which decides how content is counted. The counter is also
   handed to `captureResponseWriter`, which wraps the writer for that request.
2. `attempt` calls `live.routed(provider, model)` for the try that is about to
   answer, so the numbers are learned and estimated for the provider that actually
   served it after any fallback. `unprefixed(model)` drops the `provider/` prefix.
3. Every `Write` on the capture writer calls `observe(p, Content-Type)`:
   - `len(p)` always adds to the byte total;
   - `commentOnly(p)` (a write that is nothing but SSE comment lines and blanks)
     adds bytes but never moves the content clock;
   - otherwise the counter splits SSE frames (or takes a whole JSON body) and
     adds the length of the *content* it carries — text, reasoning, tool names and
     arguments, per protocol — to the content total.
4. Each write tells the request's route what it counted (`tellRoute`: the running
   totals plus the estimate), so the row and the seat caption move while the reply
   is still out.
5. The deferred `finish(reason, call.Usage.Output)` flushes the split-frame buffer
   and learns a rate sample whenever the vendor reported tokens and the try did not
   fail — a client that went away does not disqualify the reply: usage in hand
   means the vendor finished it (agents close the connection after the last content
   event, before the tail events).

## Tokens: reported, estimated, or unknown

- The vendor's `usage` is the only exact number and always wins. It usually lands
  in the last event, so it arrives as the reply ends and the row switches from
  `≈` to the real count.
- Until then the estimate is `content × rate`, where the rate is the tokens per
  content byte learned for that provider and model: `receive-tokens.json` in
  Magpie's config directory keeps one accumulator per pair (`bytes`, `output`,
  `seen`), learns from replies that carried endpoint-reported usage and did not
  end `failed` — `interrupted` counts, since usage in hand means the vendor
  finished the reply (leaving early may cost the sample its last few content
  bytes, which biases the rate slightly low) — and is written atomically through
  a temp file and a rename. Up to 256 pairs are kept; the least recently seen are
  dropped.
- Under `liveRateMin` = 3 learned replies a pair has no rate: the tokens cell is
  not shown at all — the row keeps its size and its age rather than guessing.
- The estimate is marked `≈` everywhere; the reported number never is.

## Constraints worth knowing

- **Bytes include framing and heartbeats.** The byte total is what the client
  received: SSE `data:`/`event:` framing and the gateway's own `: keepalive`
  comments count. A reply quiet for 40 s still grows, by about 12 bytes per
  keepalive.
- **Content bytes are the words only.** Content counts the text, reasoning and
  tool payload inside the frames, so its ratio to bytes is the framing overhead;
  the estimate is built on content, never on bytes.
- **Heartbeats are not content.** A translated protocol's JSON keepalive is not
  recognised as one — separating it would mean understanding bodies on this path,
  which is exactly what this seam avoids.
- **Split writes are counted once.** A UTF-8 character or an SSE frame may arrive
  in two writes; the counter buffers the tail and only counts whole lines, so the
  same reply counted in 7-byte slices and in one piece gives the same number.
- **Nothing is stored per request.** The counter lives and dies with the request;
  only the learned rates persist.
- **Cheap on the hot path.** Per write: a length add, a clock read, a comment
  check, and — for content — splitting at line ends and one `json.Unmarshal` per
  event, bounded by `contentCap` (8 MiB) per reply.

## In the Routing page

A request still out shows its progress twice, both read from the same route:

| Where | Looks like | When |
| --- | --- | --- |
| **A** — the request row's numbers, right of the model | `63 B · ≈ 8 tokens · 29 秒前` | the row is still out; the cells disappear when it ends, where the row already has its own full row of numbers |
| **D** — the seat caption in the stage diagram | `回答中… · 12.4 KiB · ≈ 116 tokens · 6 秒前` | the diagram is explaining this very request; another request using the same account never shows someone else's numbers |

- Silence is said without the verb: after 15 s with no content the seat caption
  drops 「回答中…」 and only the numbers stay, with the account's dot turned amber.
  It did not fail; it is quiet.
- The row's numbers are built in `liveCells` and patched in place (`paintLive`)
  instead of rebuilding the row: rows are only remade when what they *say*
  changes (#3731), and rebuilding one every second would flash the list and move
  the scroll position. A silence wakes no long poll (`liveBytes` wakes on
  writes), so the page's once-a-second tick repaints the still-out rows itself
  (`livePaint`): the age goes on moving while nothing arrives.
- Nothing here is counted twice: `(*trace).liveBytes` writes the same bytes,
  `contentAt` and estimate the row shows, and only wakes the long poll at most
  every `liveWake` (250 ms).
- A reply that has ended keeps its size, in the two places that have room for it:
  the row's numbers gain a last column `收到 58.5 KiB` (from `requestMetrics`, so it
  sits with the duration, tokens and speed of the same reply), and the seat caption
  stops saying only 「回答了这个请求」 and ends with `· 58.5 KiB · 142 tokens`
  (the vendor's count, since the estimate no longer matters). The `6 秒前` drops
  with the request: it says how long the model has been quiet, which a finished
  reply no longer is.

| Field on a `Route` | Meaning |
| --- | --- |
| `bytes` | what the agent has received of this reply so far — framing and keepalives included |
| `contentAt` | epoch ms of the last content (a heartbeat comment doesn't move it) |
| `estTok` | estimated output tokens from the learned rate; absent until that provider and model learned one |

## Verification

```sh
go test -mod=readonly -tags nogui ./internal/gateway -run 'TestLive|TestContent|TestRates|TestCommentOnly' -v
go test -mod=readonly -tags nogui ./internal/cli -run TestEveryRouteHasACommandOrIsNamed
go test -mod=readonly -tags nogui ./internal/gui -run 'TestBuiltin|TestCoreBudget'
go build -mod=readonly -tags nogui .
```

`TestLiveWritesReachTheRoute` and `TestRouteGetsTheEstimateOnlyOnceItIsCalibrated`
hold the Routing page's side of the seam: bytes reaching the row reach its route
too, a heartbeat adds bytes without moving `contentAt`, a route begun after the
first write still gets the running total, and `estTok` stays empty until the
learned rate exists.

`TestContentBytesPerProtocol` pins what counts as content in each protocol, and
`TestContentCounterSurvivesSplitWrites` pins that a split write counts once.
`TestRatesLearnFromRepliesTheVendorFinished` keeps failed and usage-less
replies out of the learning set, and lets an interrupted reply in when the
vendor reported its tokens.

`TestLiveCountsARealStream` drives a real request through `Server.Handler()` with
a fake upstream that stalls mid-stream, and asserts the route's bytes equal the
bytes the client received. Both places were checked in `magpie web` against a
fake streaming provider: a live row growing byte by byte, the seat caption
counting up, the amber dot after 15 s of silence with the byte count still rising
from keepalives, and the ended row showing the vendor's tokens.
