# MCP 2026-07-28 pre-implementation audit

**Date:** 2026-08-11 · **Branch:** `mespencer/mcp-audit` · **Status:** research only, no code

DESIGN.md §9 says: *"Verify the concrete harness-client and SDK support matrix when the
endpoint/hub are built, not from memory."* The per-agent MCP endpoint (§5) is the next
wave, so this is that verification.

Everything below is either quoted from a current primary source or **measured on this
machine on 2026-08-11**. Claims taken from memory are marked as unknowns in §5 rather
than asserted.

## Method

Spec claims come from `modelcontextprotocol.io` and the SEP PRs. Harness claims come from
a purpose-built probe MCP server that logs every byte a client sends, run against the
locally installed `claude` and `codex` binaries. The probes are throwaway scripts, not
committed; each finding below names the exact observation that produced it. Binary
string-grepping was used only to *generate* hypotheses — every claim about harness
behaviour is backed by an observed wire trace, because the strings turned out to be
misleading (both binaries contain `2026-07-28` and `server/discover`; neither client
emits either).

---

## 1. The 2026-07-28 revision

### 1.1 Stateless core — confirmed, with more removed than §9 assumes

The revision is **final**, published 2026-07-28 after a ten-week RC window. Not a draft.

DESIGN.md §9's summary is accurate as far as it goes:

| §9 claim | Verdict | Detail |
|---|---|---|
| No `initialize` handshake | Confirmed | `initialize`/`notifications/initialized` removed (SEP-2575) |
| Version + capabilities ride `_meta` | Confirmed | `io.modelcontextprotocol/protocolVersion`, `…/clientCapabilities`, `…/clientInfo` per request; servers return `…/serverInfo` in each result's `_meta` |
| No protocol sessions | Confirmed | `Mcp-Session-Id` and the session concept removed (SEP-2567) |
| Cross-call state as server-minted handles | Confirmed | Servers needing cross-call state "use explicit, server-minted handles passed as ordinary tool arguments" |

Four further removals §9 does **not** mention, each of which costs the endpoint work:

1. **`server/discover` is mandatory.** Servers **MUST** implement it. It is the only way a
   client learns supported versions, capabilities, and identity up front. This is a new
   required RPC, not an optional one.
2. **The GET/SSE stream is gone**, along with `resources/subscribe`. Server-to-client
   change notifications now ride a single long-lived POST response stream opened by
   `subscriptions/listen`, with the client opting in to specific notification types.
3. **Server-initiated requests are gone entirely.** Sampling, elicitation, and `roots/list`
   are replaced by **Multi Round-Trip Requests (MRTR)** (SEP-2322): the server returns an
   `InputRequiredResult` (`resultType: "input_required"`) carrying `inputRequests`, and the
   *client re-issues the original request* with `inputResponses` attached. A server may
   never push a JSON-RPC request to a client on any stream.
4. **All results carry a required `resultType`** (`"complete"` or `"input_required"`), and
   list/read results must carry `ttlMs` + `cacheScope` (`CacheableResult`, SEP-2549).

Also: `ping`, `logging/setLevel`, and `notifications/roots/list_changed` are removed; SSE
resumability (`Last-Event-ID`) is removed; **Roots, Sampling, and Logging are deprecated
outright** (SEP-2577); HTTP+SSE is reclassified Deprecated (SEP-2596). Streamable HTTP
POSTs must now carry `MCP-Protocol-Version`, `Mcp-Method`, and (for
`tools/call`/`resources/read`/`prompts/get`) `Mcp-Name` headers, and a server that reads
the body **MUST** reject header/body mismatches with `-32020 HeaderMismatch`.

The deprecation window is a **twelve-month minimum**, formalized as policy (SEP-2596) —
this is the number §9 alludes to, and it is now a governance commitment rather than a
guess.

**Sources:** [Key Changes](https://modelcontextprotocol.io/specification/2026-07-28/changelog) ·
[Announcement](https://blog.modelcontextprotocol.io/posts/2026-07-28/) ·
[Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http) ·
[stdio](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio) ·
[Versioning](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning)

### 1.2 Tasks extension — a good fit, but not for assignments

Tasks moved **out of core into an official extension** (`io.modelcontextprotocol/tasks`,
SEP-2663). The redesign replaced the blocking `tasks/result` with polling `tasks/get`,
added `tasks/update` for client→server input, removed `tasks/list`, and lets servers
return task handles **unsolicited** — no per-request opt-in flag.

Shape:

- Client declares `io.modelcontextprotocol/tasks` in its per-request
  `clientCapabilities.extensions`; server advertises the same via `server/discover`.
- Server decides *per request* whether to return a `CreateTaskResult`
  (`resultType: "task"`) with `taskId`, status, `ttlMs`, `pollIntervalMs`. The task must be
  durably created before the response is sent.
- Lifecycle: `working` → `input_required` → terminal (`completed` | `failed` | `cancelled`).
- `tasks/get` polls; `tasks/update` answers `inputRequests`; `tasks/cancel` is
  **cooperative** — the server acknowledges intent and may still finish normally.
- Optional `notifications/tasks` push, consumed via `subscriptions/listen`, carrying full
  task state so no extra `tasks/get` is needed.
- A server **MUST NOT** return a task to a client that did not declare the extension.

**Does it fit long-running agent assignments?** Partly, and the distinction matters.

It fits `wait_for_agent` almost exactly: a blocking wait for a result that may take an
hour, over a transport with timeouts, resumable after a client restart, cancellable. That
is the problem Tasks was built for, and `input_required` even maps onto Horde's
human-approval escalation.

It does **not** fit an assignment's *lifetime*. Horde's agents outlive any task: they go
idle with context retained and are revived by a later message (§5). A `taskId` has a
`ttlMs` and three terminal states; a Horde agent has `running → waiting → idle → archived`
and can leave a terminal-looking state when someone sends it another message. Modelling an
agent as a task would put the agent's identity inside an extension's expiry policy.

The right split: **agent handles stay Horde's own server-minted handles** (which is exactly
what §1.1's "explicit, server-minted handles passed as ordinary tool arguments" endorses),
and **Tasks is an optional encoding for the blocking verbs** — `wait_for_agent`, and any
hub-proxied upstream tool that is itself slow. Defer it: no harness client we care about
declares the extension today (§2), and a server must not hand a task to a client that
didn't ask.

**Sources:** [Tasks overview](https://modelcontextprotocol.io/extensions/tasks/overview) ·
[ext-tasks](https://github.com/modelcontextprotocol/ext-tasks) ·
[SEP-2663](https://github.com/modelcontextprotocol/modelcontextprotocol/pull/2663)

### 1.3 Dual-revision serving — official guidance exists, and it endorses §9

This was the open question, and the answer is better than expected. The spec defines an
explicit **era model** and blesses dual-era servers:

> **Modern**: protocol versions that convey version, identity, and capabilities as
> per-request metadata (revision `2026-07-28` and later). **Legacy**: protocol versions that
> establish a session with an `initialize` handshake (`2025-11-25` and earlier).
> **Dual-era**: an implementation that supports both.

And the server-side rule, which is precisely the design DESIGN.md §9 already committed to:

> A dual-era **server** selects its behavior from how the client opens:
> - A request carrying modern per-request `_meta` is served statelessly according to this
>   revision.
> - An `initialize` request selects legacy semantics, scoped to the stdio process (stdio)
>   or the session (HTTP), as specified by the negotiated legacy protocol version.
>
> A dual-era server **MAY** serve both eras concurrently on the same endpoint or process.

So §9's "an `initialize` request selects the old flow; a `_meta`-versioned request the new"
is not an invention — it is the specified discriminator, verbatim.

The compatibility matrix confirms the consequence that drives §3's recommendation:

| Client | Server | Outcome |
|---|---|---|
| Modern | Modern | Works |
| Modern | Legacy | **Fails** |
| Dual-era | Modern | Works |
| Dual-era | Legacy | Works |
| Legacy | Modern | **Fails** — "Legacy clients have no fall-forward mechanism" |
| Legacy | Dual-era | Works |
| Legacy | Legacy | Works |

Detection is a **client-side** responsibility (probe `server/discover` on stdio; attempt a
modern request and inspect the `400` body on HTTP), and era is a property of the server to
be cached for the server's lifetime. A server's only obligations are to pick behaviour from
the opener and — if modern-only — to *name its supported versions in the error it returns
to `initialize`*, because a legacy client cannot recover and that message may be the user's
only diagnostic.

**Source:** [Versioning §Backward Compatibility](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning)

---

## 2. Harness MCP-client support — measured, not remembered

**Installed on this machine (2026-08-11), both the newest available release:**

| Binary | Version | Latest published | Path |
|---|---|---|---|
| `claude` | **2.1.228** | 2.1.228 (npm `latest`, published 2026-08-11; `stable` tag is 2.1.221) | `/Users/myles/.local/share/claude/versions/2.1.228` |
| `codex` | **codex-cli 0.147.0** | `rust-v0.147.0` (2026-08-07) | `/opt/homebrew/bin/codex` |

ADR-0003 spiked 2.1.226 and ADR-0005 spiked 0.145.0, so both are one to two releases behind
what is installed now.

### 2.1 Headline: both harnesses are legacy-era clients. Neither speaks 2026-07-28.

This is the single most important finding in the audit, and it inverts the risk §9
anticipated.

**Claude Code 2.1.228 over streamable HTTP** — first bytes on the wire:

```
POST /mcp   user-agent: claude-code/2.1.228 (sdk-cli)
{"method":"initialize","params":{"protocolVersion":"2025-11-25",
 "capabilities":{"roots":{"listChanged":true},"elicitation":{}},
 "clientInfo":{"name":"claude-code","version":"2.1.228",…}}}
POST /mcp   mcp-protocol-version: 2025-11-25   → notifications/initialized
GET  /mcp   accept: text/event-stream          ← the stream removed in 2026-07-28
POST /mcp   mcp-protocol-version: 2025-11-25   → tools/list
```

No `server/discover`. No `_meta` protocol version. No `Mcp-Method`/`Mcp-Name` headers. It
opens the GET SSE stream that 2026-07-28 deleted.

**Claude Code 2.1.228 over stdio** — identical era: `initialize` at `2025-11-25`, then
`notifications/initialized`, then `tools/list`. No `server/discover` probe.

**Codex 0.147.0 over streamable HTTP** — older still:

```
POST /mcp   user-agent: codex-mcp-client/0.147.0
{"method":"initialize","params":{"protocolVersion":"2025-06-18",
 "capabilities":{"elicitation":{"form":{},"url":{}}},
 "clientInfo":{"name":"codex-mcp-client","version":"0.147.0"}}}
```

**Codex pins `2025-06-18`** — a full two revisions behind Claude Code. It did not open a GET
stream, but it is unambiguously legacy-era.

Neither client declared `io.modelcontextprotocol/tasks` (or any `extensions` field at all).

### 2.2 Neither client falls forward to a modern-only server

Tested directly against a modern-only probe that rejects `initialize` with
`-32022 UnsupportedProtocolVersionError` naming `["2026-07-28"]`, exactly as §1.3 requires:

Claude Code sent `initialize`, received the 400, and **stopped**. One request, no
`server/discover` retry, no error surfaced to the agent — the server was silently dropped
and the turn continued with no tools. This is the matrix's "Legacy client / Modern server →
Fails" cell, reproduced on our exact harness.

**Consequence: a 2026-07-28-only endpoint would ship zero tools to every agent today.** Not
degraded — zero, silently. Dual-era is not a compatibility nicety for the deprecation
window; for the next several months it is the *only* path that works at all, and the legacy
half is the half that carries all the traffic.

### 2.3 Transports, both directions

| | Claude Code 2.1.228 | Codex 0.147.0 |
|---|---|---|
| stdio client | Yes (verified) | Yes (`codex mcp add <name> -- <cmd>`) |
| Streamable HTTP client | Yes (verified) | Yes (`codex mcp add <name> --url`, verified) |
| Legacy HTTP+SSE client | `"type":"sse"` config accepted; not exercised here | Not offered by `codex mcp add` |
| Spec revision spoken | `2025-11-25` | `2025-06-18` |
| Custom headers on HTTP | **Yes, verified** | `--bearer-token-env-var`, plus OAuth (`codex mcp login`, `--oauth-client-id`, `--oauth-resource`) |

The header result matters for Horde's per-agent token. Configuring
`{"type":"http","url":…,"headers":{"Authorization":"Bearer …","X-Horde-Agent":"agent-42"}}`
put both headers on **every** request, including `initialize` and the GET stream. The
per-agent token can ride a plain header; no OAuth is needed on the internal surface (§4).

### 2.4 `--permission-prompt-tool` and `--mcp-config` since 2.1.226

`--mcp-config <configs...>` and `--strict-mcp-config` are unchanged and documented in
`--help`.

**`--permission-prompt-tool <tool>` still exists and still works at 2.1.228, but it is no
longer listed in `--help`.** It is now a hidden flag: grepping `claude --help` for
"permission" returns only `--permission-mode`, `--dangerously-skip-permissions`, and
`--allow-dangerously-skip-permissions`, yet invoking `claude --permission-prompt-tool` with
no argument still errors with `option '--permission-prompt-tool <tool>' argument missing`.

Verified end to end: with a stdio MCP server exposing `permission_prompt`, and
`--permission-prompt-tool mcp__perm__permission_prompt`, a `Bash` call arrived as

```json
{"tool_name":"Bash",
 "input":{"command":"sw_vers -productVersion","description":"Get macOS product version"},
 "tool_use_id":"toolu_015Jfjzgbu2aCPxsPB38mGHe"}
```

and returning `{"behavior":"deny","message":"HORDE_PROBE_DENIED"}` blocked the command; the
agent reported the denial verbatim. ADR-0003's mechanism is intact at 2.1.228.

**One load-bearing caveat discovered while verifying this, which ADR-0003 does not record.**
The first attempt saw *no* permission request at all: `echo hello` ran and the prompt tool
was never called, even though the MCP server was connected and `tools/list` had been served.
The cause was ambient permission settings — the user's own `settings.json` allowlist
already permitted the call, so it never reached the prompt tool. It only fired once the run
was isolated with `--settings` pointing at `{"permissions":{"allow":[],"deny":[],
"defaultMode":"manual"}}` and `--permission-mode manual`.

**`--permission-prompt-tool` is not a universal interceptor. It sees only the calls that
ambient settings have not already decided.** For Horde's §10 chain to be the real authority,
the worker must own the settings Claude Code reads — pass an explicit `--settings` with a
known-empty allowlist per agent, and never inherit the operator's `~/.claude/settings.json`.
A silently-inherited allowlist is a permission bypass that produces no error and no event.

**Sources:** local `claude --version` / `codex --version` / `claude --help` /
`codex mcp add --help`; probe wire traces (this machine, 2026-08-11) ·
[npm @anthropic-ai/claude-code](https://registry.npmjs.org/@anthropic-ai/claude-code) ·
[openai/codex releases](https://github.com/openai/codex/releases) ·
[Codex MCP docs](https://developers.openai.com/codex/mcp)

---

## 3. Rust SDK (rmcp)

**Current: `rmcp` 3.1.2, published 2026-08-07. MSRV 1.88** (Horde pins 1.95.0 — fine).
19.7M downloads; 3.0.0 shipped the same day as the spec.

README, verbatim: *"This SDK implements the stable MCP `2026-07-28` specification while
remaining fully compatible with the `2025-11-25` release and earlier versions."* Named
2026-07-28 features implemented: server discovery & negotiation, transport-neutral
subscriptions, tasks, response caching, MRTR, and the standard HTTP routing headers.

**Dual-era serving is built in and is the default.** `StreamableHttpServerConfig` carries:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `legacy_session_mode` | `bool` | `false` | Create + keep a session per request; enables SSE priming for client reconnection |
| `json_response` | `bool` | `false` | When `legacy_session_mode` is off, prefer `application/json` for simple request/response tools |
| `allowed_hosts` | `Vec<String>` | **loopback only** | Inbound `Host` validation |
| `allowed_origins` | `Vec<String>` | empty | Inbound `Origin` validation (DNS-rebinding defence) |
| `stateless_protocol_metadata_required` | `bool` | `false` | Require per-request protocol signals on stateless POSTs |
| `sse_keep_alive`, `sse_retry` | `Option<Duration>` | `None` | SSE ping / retry hints |
| `session_store` | `Option<Arc<dyn SessionStore>>` | `None` | External store for cross-instance session recovery |
| `max_request_body_bytes` | `usize` | default | POST body cap |

And the decisive doc note on `legacy_session_mode`:

> "Only applies to legacy protocol versions (< 2026-07-28). Per SEP-2567, sessions are
> removed from the `2026-07-28` version, so requests negotiating that version are always
> served statelessly regardless of this setting."

So one `StreamableHttpService` serves both eras on one endpoint, choosing per request. The
`legacy_session_mode` flag only decides whether the *legacy* half gets sessions.

Relevant features on 3.1.2: `transport-streamable-http-server`,
`transport-streamable-http-server-session`, `transport-io` (stdio),
`transport-child-process` (the hub's client side), `transport-worker` (in-process, for
tests), `transport-streamable-http-client-unix-socket`, `auth`, `elicitation`,
`request-state`, `macros`, `tower`.

**Gaps that would force hand-rolling: none identified for the endpoint.** Server-side
2026-07-28 stateless, legacy sessioned, dual-era on one endpoint, stdio, streamable HTTP,
Tasks, MRTR, and subscriptions are all in-SDK. It is a `tower` service, so it composes with
whatever Horde already terminates mTLS on. The client half (`transport-child-process` +
streamable-HTTP client) covers the §9 hub's transport-omnivorous requirement, though the
*multiplexing* many agents onto one upstream session — ADR-0004's acknowledged risk — is
Horde's own logic and not something any SDK provides.

Two caveats worth pricing in: the crate went 3.0.0 → 3.1.2 in eleven days (four releases),
so the API is moving fast and should be pinned exactly and upgraded deliberately; and the
`unwrap()`/`expect()` prohibition in this repo's rules applies to our glue, not the
dependency, so error mapping at the seam needs to be explicit.

**Sources:** [rust-sdk README](https://github.com/modelcontextprotocol/rust-sdk) ·
[crates.io/api/v1/crates/rmcp](https://crates.io/crates/rmcp) ·
[StreamableHttpServerConfig](https://docs.rs/rmcp/3.1.2/rmcp/transport/streamable_http_server/tower/struct.StreamableHttpServerConfig.html)

---

## 4. Authorization changes vs. a localhost, mTLS-wrapped endpoint

**The internal surface can ignore the OAuth/OIDC work. This is explicit, not inferred:**

> Authorization is **OPTIONAL** for MCP implementations. When supported:
> - Implementations using an HTTP-based transport **SHOULD** conform to this specification.
> - Implementations using an STDIO transport **SHOULD NOT** follow this specification, and
>   instead retrieve credentials from the environment.
> - Implementations using alternative transports **MUST** follow established security best
>   practices for their protocol.

The OAuth machinery — Client ID Metadata Documents replacing (now-deprecated) Dynamic
Client Registration, RFC 9207 `iss` validation, `application_type` in DCR, credentials
keyed by issuer, RFC 8707 resource indicators, RFC 9728 Protected Resource Metadata — is
all scoped to *clients authorizing against third-party protected MCP servers*. Horde's
per-agent endpoint is a loopback resource behind mTLS with a per-agent bearer token minted
by Horde itself. There is no authorization server, no browser, no user consent step, and no
third party. The §4 changes land on the **hub's client side** if and when it consumes a
remote OAuth-protected user server (Linear et al.), which is where the single-auth story in
ADR-0004 already puts them — not on the agent-facing endpoint.

**What is not optional, and applies to Horde regardless:** the Streamable HTTP transport's
own security clauses.

1. Servers **MUST** validate the `Origin` header and respond `403` when present and
   invalid — DNS-rebinding defence. A browser on the same machine can otherwise reach a
   loopback MCP endpoint.
2. Servers **SHOULD** bind only to `127.0.0.1`, not `0.0.0.0`.
3. Servers **SHOULD** implement proper authentication for all connections.

rmcp's `allowed_hosts` (loopback-only by default) and `allowed_origins` cover 1 and 2
directly, so this is configuration rather than code.

One deployment note: a **stdio** shim (the §9 universal fallback) sidesteps this entire
section — the spec tells stdio implementations to take credentials from the environment,
which is what a worker-spawned child process does natively.

**Sources:** [Authorization](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization) ·
[Streamable HTTP §Security](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http) ·
[Key Changes](https://modelcontextprotocol.io/specification/2026-07-28/changelog)

---

## 5. Recommendation

### Build first — the per-agent endpoint

1. **Build legacy-era first, on rmcp 3.1.2, pinned exactly.** Not 2026-07-28 first. Both
   harnesses that Horde ships adapters for are legacy-era, neither falls forward, and a
   modern-only endpoint delivers zero tools with no error (§2.2). Serve `2025-11-25` for
   Claude Code and `2025-06-18` for Codex; rmcp's dual-era `StreamableHttpService` gives
   both plus the modern path from one config, so this is a config decision, not an
   architecture fork. DESIGN.md §9's dual-revision plan is correct and is now also the
   spec's own guidance (§1.3) — no change needed to the design.
2. **Streamable HTTP over loopback + mTLS, per-agent token in an `Authorization` header.**
   Verified to work on Claude Code (§2.3) and supported on Codex via
   `--bearer-token-env-var`. Set `allowed_hosts` to loopback and populate `allowed_origins`;
   do not skip `Origin` validation because it is "only localhost" (§4).
3. **Own the harness's permission settings.** Pass an explicit `--settings` with an empty
   allowlist and `--permission-mode manual` per Claude agent, alongside
   `--permission-prompt-tool` pointed at the endpoint. Without this the §10 chain is
   silently bypassed for anything the ambient config already allows (§2.4). This is the
   highest-value finding in the audit and should be treated as a correctness requirement,
   not a hardening nicety.
4. **Keep the stdio shim on the roadmap but not on the critical path.** Both harnesses
   speak HTTP with headers today, so the shim is insurance for a future client, not a
   launch blocker. rmcp's `transport-io` makes it cheap when needed.
5. **Agent handles are Horde's own opaque server-minted handles**, passed as ordinary tool
   arguments. This is both what §5 already implies and what the stateless core explicitly
   endorses (§1.1); it means the endpoint has no cross-call protocol state to lose when the
   modern path eventually becomes primary.

### What the hub needs (§9, ADR-0004)

6. **Client side stays transport-omnivorous**, as designed: `transport-child-process` for
   stdio user servers, streamable-HTTP client for remote ones, and legacy HTTP+SSE only if
   a specific integration demands it — note it is now formally Deprecated, so treat it as
   a long-tail concession with a twelve-month clock, not a target.
7. **The hub is where the OAuth work lands**, not the endpoint (§4) — CIMD-first with DCR
   as the compatibility fallback, `iss` validation, and credentials keyed by issuer.
8. **Multiplexing remains the risk**, unchanged by this audit and unhelped by the SDK.
   ADR-0004's `local` escape hatch stays the pressure valve.

### Defer

9. **Tasks extension** — no harness client declares it, and a server must not return a task
   to a client that did not. Revisit when a harness advertises
   `io.modelcontextprotocol/tasks`; the natural first use is `wait_for_agent`, not agent
   lifetime (§1.2).
10. **MRTR, `subscriptions/listen`, `server/discover`, cacheable results, `Mcp-Method`/
    `Mcp-Name` header emission** — all modern-path-only. rmcp implements them; Horde gets
    them by upgrading its served revision, not by writing code. No reason to exercise them
    before a client does.
11. **Sampling / Roots / Logging** — deprecated in this revision. Do not build on them.

### Docs updated alongside this audit

Both applied on this branch:

- **DESIGN.md §9 transport/spec policy** — the policy *shape* was already right (dual-era
  serving, the `initialize`-vs-`_meta` discriminator), so the amendment fixes the emphasis:
  legacy-era first and reaching back to `2025-06-18`, the zero-tools-silently failure mode,
  the modern era arriving free from pinned rmcp, and the instruction to re-verify harness
  support by wire trace rather than binary strings.
- **ADR-0003** — gains the ambient-settings caveat from §2.4. Its "verified 2026-08-09
  against 2.1.226" line stays accurate; what changes is that the permission-interception
  claim is now explicitly conditional on Horde owning `--settings`.

Both ADRs' version stamps remain one to two releases behind what is installed (§2).

---

## 6. Unknowns and honest gaps

- **When either harness will speak 2026-07-28 as a client.** Anthropic's announcement says
  only that support "is being rolled out across Claude products soon", with no version,
  date, or mention of Claude Code specifically. Both binaries contain `2026-07-28` and
  `server/discover` strings while emitting neither — that could be a vendored schema, an
  unshipped code path, or a feature flag. Unresolved; re-probe each release rather than
  reading the strings.
- **Whether Codex will move off `2025-06-18`.** It is two revisions behind and this audit
  found no roadmap signal. Codex is the constraint on how old the endpoint's legacy half
  must be able to go.
- **Claude Code's HTTP+SSE (`"type":"sse"`) path** was not exercised. The config key is
  accepted; behaviour is untested.
- **`claude mcp list` ignores `--mcp-config`.** Servers passed on the command line are not
  health-checked by that subcommand (it listed only ambient servers and never contacted the
  probe). Findings here came from `--print` runs instead. Not a Horde problem, but it will
  mislead anyone debugging the endpoint with `mcp list`.
- **Codex's client-side permission behaviour was not re-probed.** ADR-0005's finding (no
  client-facing approval requests) was verified at 0.145.0; this audit did not re-run those
  six configurations against 0.147.0. ADR-0005 asks for a re-check on each Codex upgrade,
  and that debt is now **two releases stale (0.145.0 → 0.147.0), carried into the endpoint
  wave**. It matters more than it looks: if the two under-development approval flags have
  since stabilized, ADR-0005's "Codex cannot be pre-authorized" consequence — and the
  placement rule built on it — would need revisiting rather than working around.
- **rmcp's dual-era behaviour was read from docs, not exercised.** The
  `StreamableHttpServerConfig` semantics above are quoted from the published API docs; no
  Horde spike has yet stood up an rmcp server and driven it with both eras. That spike is
  the obvious first task of the endpoint wave, and it is small.
- **`tools/call` under legacy sessions with many concurrent agents** — the multiplexing risk
  — was not measured at all. Out of scope here; it needs a load spike, not a doc.
