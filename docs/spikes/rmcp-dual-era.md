# rmcp dual-era spike

**Date:** 2026-08-11 · **Branch:** `mespencer/rmcp-spike` · **Base:** `95fe6a9`
**Status:** spike + docs only, no production code

`docs/research/mcp-2026-audit.md` §6 closes with the one recommendation in the audit
that rested on documentation rather than measurement:

> **rmcp's dual-era behaviour was read from docs, not exercised.** The
> `StreamableHttpServerConfig` semantics above are quoted from the published API docs;
> no Horde spike has yet stood up an rmcp server and driven it with both eras.

This is that spike. Everything below is a wire observation from this machine on
**2026-08-11**, captured by a byte-level TCP tee that parses no HTTP. Transcripts are in
`docs/spikes/rmcp-dual-era/transcripts/`; each finding names the file that produced it.

## Verdict

**Legacy-first on rmcp 3.1.2 is sound. The plan of record survives, with one config
correction and two gaps to write down.**

The audit's central claim is confirmed as measured fact: one `StreamableHttpService`
serves `2025-06-18`, `2025-11-25`, and `2026-07-28` concurrently on a single endpoint,
selecting per request from how the client opens, exactly as the spec's era model
describes. Transcript `05` shows all three revisions and three real clients against one
process.

Four things change what Horde should build, none of them fatal:

1. **`legacy_session_mode` must be `true`.** rmcp's default (`false`) is not merely
   "no sessions" — it costs legacy clients their `GET`/SSE stream (answered `405`) and
   throws away the `initialize` handshake's `clientInfo`. §3.
2. **Origin validation is off by default and must be turned on.** `allowed_origins`
   defaults to empty, which *disables* the check the spec says servers MUST perform.
   A forged `Origin` was served normally. §6.
3. **Codex *can* be intercepted for MCP tool calls** — via `app-server`, not `exec`.
   This partially reverses ADR-0005's "Codex cannot be pre-authorized". §4.
4. **Both harnesses have a working 2026-07-28 client already.** Codex ships it behind
   the `mcp_2026_07_28` feature flag; enabling it produced a genuine modern-era client.
   The audit's largest unknown now has a concrete answer. §5.

No rmcp gap was found that forces hand-rolling. The SDK's modern-era conformance is
better than the audit assumed — `server/discover`, `CacheableResult`, and the full
SEP-2243 header enforcement are all correct out of the box.

---

## 1. What was stood up

`spikes/rmcp-dual-era/` is one binary that is server, tee, and tools:

```
client ──▶ tee (--listen 8931) ──▶ hyper + rmcp StreamableHttpService (ephemeral port)
             │
             └─▶ transcript file
```

The tee is deliberately dumb — it forwards TCP reads and writes them to the transcript
without parsing HTTP, so no framing detail can be laundered into something tidier than
what crossed the socket. A tower layer in front of the rmcp service logs every inbound
request's full header set to stderr (`SERVER-SAW`), which is how "the header reached the
socket" is distinguished from "the header reached rmcp".

Three tools: `echo`, `add`, and `whoami`. `whoami` is the load-bearing one — it answers
from `ctx.extensions.get::<http::request::Parts>()`, so a successful call proves the
per-agent identity headers reached the *handler*.

**Version confirmed.** `Cargo.toml` pins `rmcp = "=3.1.2"`; `cargo tree -p rmcp` and
`Cargo.lock` both resolve `3.1.2`. The server also prints constants that come from the
dependency itself:

```
RMCP-LATEST 2025-11-25
RMCP-KNOWN-VERSIONS 2024-11-05,2025-03-26,2025-06-18,2025-11-25,2026-07-28
```

**First surprise, minor but load-bearing:** `ProtocolVersion::LATEST` in rmcp 3.1.2 is
**`2025-11-25`, not `2026-07-28`**. A server built on rmcp's defaults advertises the
legacy revision in `get_info()`. That is convenient for Horde's legacy-first plan, but it
is the opposite of what "implements the stable 2026-07-28 specification" suggests, and it
means the served revision is a deliberate choice rather than something inherited.

## 2. Claude Code 2.1.228 — legacy round-trip confirmed

Transcript `01` / `02`. The real installed binary (`claude --version` → `2.1.228`), driven
with `--strict-mcp-config --mcp-config` and an isolated `--settings`.

`initialize` negotiates `2025-11-25`, as the audit predicted:

```
POST /mcp HTTP/1.1
User-Agent: claude-code/2.1.228 (sdk-cli)
Authorization: Bearer horde-agent-42-token
x-horde-agent: agent-42

{"method":"initialize","params":{"protocolVersion":"2025-11-25",
 "capabilities":{"roots":{"listChanged":true},"elicitation":{}},
 "clientInfo":{"name":"claude-code","title":"Claude Code","version":"2.1.228",…}},
 "jsonrpc":"2.0","id":0}
```

`tools/list` and `tools/call` both round-trip. The `echo` call returned verbatim, and the
agent printed the result:

```
{"method":"tools/call","params":{"name":"echo","arguments":{"message":"HORDE_WIRE_OK"},
 "_meta":{"claudecode/toolUseId":"toolu_01Cmjro9RSR11YQx36ss7QEf","progressToken":2}},…}
→ {"result":{"content":[{"type":"text","text":"HORDE_WIRE_OK"}],"isError":false}}
```

Claude Code also emits a `claudecode/toolUseId` in `_meta` on every `tools/call` — a
free correlation handle between the endpoint's view of a call and the harness's own
tool-use id. Worth capturing in the endpoint's audit log.

## 3. `legacy_session_mode` — the config correction

This is the finding that changes the build. rmcp's default is `false`; the audit recorded
the default but not its consequences. Same client, same server binary, one flag apart:

| | `legacy_session_mode=false` (default) | `legacy_session_mode=true` |
|---|---|---|
| `GET /mcp` (the SSE stream Claude Code opens) | **`405 Method Not Allowed`** | `200` + SSE priming (`id: 0`, `retry: 3000`) |
| `Mcp-Session-Id` | not minted | minted, echoed on every later request |
| `clientInfo` visible to the handler | **`rmcp` / `3.1.2`** — rmcp's own default | `claude-code` / `2.1.228` |
| Server→client notification channel | **none** | the GET stream |
| `initialize` required before `tools/list` | **no** — any legacy request is served cold | yes |

The 405, from transcript `01`:

```
GET /mcp HTTP/1.1
Accept: text/event-stream
mcp-protocol-version: 2025-11-25
x-horde-agent: agent-42

HTTP/1.1 405 Method Not Allowed
allow: POST
```

Claude Code tolerates it — it opened a second connection and carried on to `tools/list`,
and the turn completed normally, so this is not a crash. But three consequences matter:

- **No push channel.** With no GET stream there is nowhere to send
  `notifications/tools/list_changed`. Horde's endpoint changes an agent's toolset as the
  agent's state changes (§5), and under the default config it cannot tell the agent.
- **Client identity is lost.** The handler sees rmcp's placeholder `Implementation`, not
  the client's. Per-agent identity must therefore ride headers — which is already Horde's
  plan, but it is now a hard requirement rather than a preference. Do not build anything
  on `clientInfo` for legacy clients.
- **No handshake gate.** A legacy request is served without `initialize` ever happening.
  Any authentication has to be enforced by Horde's own middleware on every request; there
  is no "session established" state to lean on.

**Recommendation: set `legacy_session_mode = true`.** It costs cross-instance session
state (mitigable with `session_store`) and buys back the push channel, the client
identity, and a handshake. Per the rmcp doc note — confirmed in transcript `04` — this
flag does not touch the modern path: `2026-07-28` requests are still served statelessly.

## 4. Codex 0.147.0 — legacy round-trip, and an ADR-0005 correction

Transcript `03`. `codex --version` → `codex-cli 0.147.0`. `initialize` at **`2025-06-18`**,
confirming the audit:

```
POST /mcp HTTP/1.1
user-agent: codex-mcp-client/0.147.0
x-horde-agent: agent-42
authorization: Bearer horde-agent-77-token

{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18",
 "capabilities":{"elicitation":{"form":{},"url":{}}},
 "clientInfo":{"name":"codex-mcp-client","title":"Codex","version":"0.147.0"}}}
```

Codex opens a GET stream (accepted under `legacy_session_mode=true`) and closes the
session with `DELETE /mcp` — the only client of the three that cleans up.

**`codex exec` cannot call MCP tools.** Every attempt failed identically, and *no byte of
the call reached the server*:

```
{"type":"item.completed","item":{"type":"mcp_tool_call","server":"spike","tool":"whoami",
 "error":{"message":"user cancelled MCP tool call"},"status":"failed"}}
```

The wire for that run shows `initialize`, `notifications/initialized`, `GET`, `tools/list`,
`DELETE` — and no `tools/call`. This is invariant across `approval_policy` values
(`never`, default), and across `--disable tool_call_mcp_elicitation` and
`--disable guardian_approval`.

**Via `app-server` it works, and the reason matters.** Codex routes every MCP tool call
through a client-facing approval request:

```
SERVER-REQUEST mcpServer/elicitation/request
  {"serverName":"spike","mode":"form",
   "_meta":{"codex_approval_kind":"mcp_tool_call","persist":["session","always"],
            "tool_description":"Report the protocol version and HTTP headers…",
            "tool_params":{}}}
```

`codex exec` never declares the capability to answer it, so the call is auto-cancelled.
The app-server client does; answering `{"action":"accept"}` (an MCP elicitation result —
**`action`, not the `decision` that sandbox/exec approvals use**; the wrong shape produces
`failed to deserialize McpServerElicitationRequestResponse: missing field 'action'`) lets
the call through, and the round-trip completes.

**This is a correction to ADR-0005.** That ADR concluded from six exec-mode configurations
that Codex produces "zero client-facing approval requests" and therefore cannot be
pre-authorized, and Horde's placement rule (DESIGN §10) was built on that. For *MCP tool
calls specifically* it is no longer true: `mcpServer/elicitation/request` carries the
server name, tool name, tool description and full arguments before the call runs, and
supports `persist: ["session","always"]`. That is exactly the interception point §10
wants. It does not extend to shell/exec approvals, which this spike did not re-probe — so
ADR-0005's finding stands for those and is now scoped rather than reversed.

Driver: `spikes/rmcp-dual-era/drive_codex.py`.

## 5. The modern era — served correctly, and codex already speaks it

### 5.1 Hand-driven 2026-07-28 against the same server

Transcript `04`, via `spikes/rmcp-dual-era/probe_modern.sh`. No `initialize`; version,
capabilities, and identity in per-request `_meta`, with the SEP-2243 routing headers.

`server/discover` — mandatory in this revision, and rmcp implements it:

```
POST /mcp   MCP-Protocol-Version: 2026-07-28   Mcp-Method: server/discover
{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{
  "io.modelcontextprotocol/protocolVersion":"2026-07-28", …}}}

→ {"result":{"resultType":"complete",
   "supportedVersions":["2024-11-05","2025-03-26","2025-06-18","2025-11-25","2026-07-28"],
   "capabilities":{"tools":{}},"ttlMs":0,"cacheScope":"private",
   "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"horde-rmcp-dual-era-spike",…}}}}
```

A modern `tools/call` round-trips, and `whoami` confirms the handler sees both the modern
protocol version and the identity headers:

```
"protocol_version":"2026-07-28", "client_name":"horde-modern-probe",
"headers":[…["authorization","Bearer horde-agent-42-token"],["x-horde-agent","agent-42"],
           ["mcp-protocol-version","2026-07-28"],["mcp-method","tools/call"],
           ["mcp-name","whoami"]…]
```

Note that on the modern path `clientInfo` *is* available per request (from `_meta`),
unlike the stateless legacy path in §3.

**Header/body mismatch enforcement is complete and spec-correct** — the `-32020` the audit
quoted from the spec, implemented:

| Probe | Result |
|---|---|
| `Mcp-Method: tools/list` + body `tools/call` | `-32020` "Mcp-Method header `tools/list` does not match body method `tools/call`" |
| `Mcp-Name: echo` + body `name: whoami` | `-32020` "Mcp-Name header `echo` does not match body value `whoami`" |
| `tools/call` with no `Mcp-Name` | `-32020` "missing required Mcp-Name header for `tools/call`" |
| `_meta` version but no `MCP-Protocol-Version` header | `-32020` "request _meta protocolVersion requires MCP-Protocol-Version header" |
| Routing headers but no `_meta` | `-32602` "request _meta is missing or has malformed required fields: …protocolVersion, …clientCapabilities" |
| `GET /mcp` with `MCP-Protocol-Version: 2026-07-28` | `405` — correct; the stream is removed in this revision |

That last row is the important interaction with §3: `legacy_session_mode=true` restores
the GET stream **only** for legacy clients. A modern client still gets `405`, which is
what the spec requires. The flag is genuinely era-scoped, as documented.

`stateless_protocol_metadata_required` was left at its default (`false`) throughout, and
rmcp still rejected modern requests with missing metadata — the strict flag tightens rules
for requests that do not self-identify as modern, and is not needed to get correct
2026-07-28 behaviour.

### 5.2 Codex already ships a 2026-07-28 client

`codex features list` includes:

```
mcp_2026_07_28                       under development  false
```

Enabling it (`--enable mcp_2026_07_28`, per-invocation, never persisted) turns codex into
a genuine modern-era client. Transcript `06`, first bytes:

```
POST /mcp HTTP/1.1
user-agent: codex-mcp-client/0.147.0
mcp-method: server/discover
mcp-protocol-version: 2026-07-28
authorization: Bearer horde-agent-77-token
x-horde-agent: agent-42

{"jsonrpc":"2.0","id":0,"method":"server/discover","params":{"_meta":{
  "io.modelcontextprotocol/protocolVersion":"2026-07-28",
  "io.modelcontextprotocol/clientInfo":{"name":"codex-mcp-client","title":"Codex","version":"0.147.0"},
  "io.modelcontextprotocol/clientCapabilities":{"elicitation":{"form":{},"url":{}}}}}}
```

No `initialize`. `server/discover` and `tools/list` both served correctly by rmcp, with
the custom headers intact. **The audit's biggest unknown — "when will either harness speak
2026-07-28" — is answered for Codex: the code path is written and shipping, one flag away,
in the version installed today.** It is off by default and marked under development, so
this is a signal about timing, not something to depend on.

*Honest limitation:* this run did not complete a modern `tools/call`. After `tools/list`
the turn stalled with no further wire traffic and was stopped. Whether that is the feature
flag's immaturity, the elicitation path interacting with the modern era, or unrelated
model-side latency is **not established**. The modern `tools/call` proof in §5.1 comes
from the hand-driven probe, not from codex.

## 6. Per-agent identity and the security defaults

### 6.1 Headers arrive everywhere, on both harnesses

Confirmed on the wire *and* in the server's own view (`SERVER-SAW`), on **every** request
including `initialize` and the GET stream:

- **Claude Code** — `"headers":{"Authorization":"Bearer …","X-Horde-Agent":"agent-42"}` in
  the `--mcp-config` entry. Both present on `initialize`, `notifications/initialized`,
  `GET /mcp`, `tools/list`, and every `tools/call`. Re-confirms the audit §2.3 result
  against an rmcp server.
- **Codex** — richer than the audit recorded. Beyond `--bearer-token-env-var`, the config
  supports **arbitrary headers**: `http_headers` and `env_http_headers` on a streamable-HTTP
  server entry. Not exposed as `codex mcp add` flags, but read from `config.toml` and
  confirmed on the wire:

  ```toml
  [mcp_servers.spike]
  url = "http://127.0.0.1:8931/mcp"
  bearer_token_env_var = "HORDE_AGENT_TOKEN"
  http_headers = { "X-Horde-Agent" = "agent-42" }
  ```

  `codex mcp get` renders `http_headers: X-Horde-Agent=*****` (values are masked in
  output, which is a nice property for a token). Both the bearer token and the custom
  header appeared on every request including `GET` and `DELETE`.

So the per-agent token *and* an explicit agent identifier can ride plain headers on both
harnesses. No OAuth on the internal surface, as the audit concluded.

### 6.2 Origin validation is off by default — fix this in config

The audit said rmcp's `allowed_hosts`/`allowed_origins` "cover 1 and 2 directly, so this
is configuration rather than code". Half right, and the half that is wrong is the one that
fails open:

| Probe | Default config | Result |
|---|---|---|
| `Host: evil.example.com` | `allowed_hosts = ["localhost","127.0.0.1","::1"]` | **`403 Forbidden`** — "Host header is not allowed" |
| `Origin: https://evil.example.com` | `allowed_origins = []` | **`200 OK`, request served** |

`allowed_origins` defaulting to empty means Origin validation is *disabled*, not
permissive-by-default-then-checked. The spec's clause is a MUST, and the DNS-rebinding
scenario it defends against — a browser on the same machine reaching a loopback MCP
endpoint — is exactly Horde's deployment. **Horde must populate `allowed_origins`
explicitly.** The `Host` default is correct and needs nothing.

## 7. Dual-era on one endpoint — the headline claim, measured

Transcript `05`. One server process, one port, three real clients in sequence:

```
protocol versions negotiated on this ONE instance
   4  "protocolVersion":"2025-06-18"                             (codex)
   2  "protocolVersion":"2025-11-25"                             (Claude Code)
   9  io.modelcontextprotocol/protocolVersion":"2026-07-28"      (modern probe)

user agents
   5  claude-code/2.1.228 (sdk-cli)
   5  codex-mcp-client/0.147.0
  12  curl/8.7.1

methods
   3  initialize        2  notifications/initialized    1  server/discover
   7  tools/list        6  tools/call
```

All three completed tool calls (`ERA_LEGACY_CLAUDE`, `ERA_LEGACY_CODEX`, and the modern
`whoami`). `initialize` selected legacy semantics; modern `_meta` selected stateless
semantics; the same process served both without restart or reconfiguration.

**The audit's §1.3 quotation from the spec, and §3's reading of the rmcp docs, are both
confirmed as behaviour.**

## 8. What this means for the endpoint plan

Unchanged from the audit's §5:

- Legacy-era first, rmcp 3.1.2 pinned exactly. Both harnesses are legacy-era by default
  and a modern-only endpoint would still ship zero tools.
- Streamable HTTP over loopback, per-agent token in `Authorization`.
- Agent handles stay Horde's own server-minted handles.
- Tasks, MRTR, `subscriptions/listen` stay deferred.

Amended by this spike:

1. **`legacy_session_mode = true`** in the endpoint's `StreamableHttpServerConfig`, for
   the push channel and the handshake. Not the rmcp default. (§3)
2. **Populate `allowed_origins`.** The default disables the check. (§6.2)
3. **Never read `clientInfo` for legacy clients** — carry agent identity in headers only,
   and validate it on every request rather than at handshake. (§3)
4. **Serve `2025-11-25` deliberately.** rmcp's `ProtocolVersion::LATEST` is already
   `2025-11-25`, so the legacy-first choice is the path of least resistance — but write it
   down as a choice, because the constant will move. (§1)
5. **Codex MCP-tool interception is available via `app-server`.** Worth a follow-up
   before the placement rule in DESIGN §10 is finalised, since ADR-0005's blanket
   "Codex cannot be pre-authorized" no longer holds for MCP tools. (§4)
6. **Watch `mcp_2026_07_28` on each codex release.** When it flips to default-on, codex
   becomes a modern client and the endpoint's modern half stops being theoretical. (§5.2)

## 9. Unknowns this spike did not close

- **Modern `tools/call` from a real harness client.** Codex's flagged modern path stalled
  after `tools/list` (§5.2). Cause unknown. The modern tool-call path is proven only by
  hand-driven probe.
- **Claude Code's 2026-07-28 timeline.** Unlike codex, no feature flag was found. Nothing
  in this spike moves the audit's "unresolved" on that.
- **Concurrency and multiplexing.** Every run here was one agent at a time. The audit's
  standing risk — many concurrent agents over legacy sessions — is untouched, and
  `legacy_session_mode = true` makes it *more* relevant, since each agent now holds a
  server-side session and a long-lived GET stream. This is the natural next spike.
- **`session_store`.** Not exercised. Relevant only if the endpoint is ever more than one
  process.
- **Codex shell/exec approvals at 0.147.0.** Still two releases stale per ADR-0005; this
  spike re-probed only the MCP tool-call path.
- **Claude Code's `"type":"sse"` transport.** Still untested, as in the audit.
- **`json_response`.** Left `false` throughout; every response here was `text/event-stream`
  even for trivial request/response tools. Whether `json_response = true` is worth it for
  the endpoint's latency is unmeasured.
