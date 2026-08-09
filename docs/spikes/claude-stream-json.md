# Spike W1c — driving Claude Code over stream-json

**Question.** [ADR-0003](../adr/0003-claude-via-stream-json.md) assumes a Rust worker can
supervise `claude` as a plain child process over `--input-format stream-json
--output-format stream-json`, with no Node sidecar and no Agent SDK. That is the
load-bearing unverified assumption in [DESIGN.md](../../DESIGN.md) §4, so it is the
first thing we build.

**Answer.** It holds. Every capability Horde needs is present on the wire, including
the one that worried us most — external permission interception. Details, evidence,
and the footguns that will bite the adapter are below.

- **CLI tested:** `claude` **2.1.226** (`claude --version` → `2.1.226 (Claude Code)`)
- **Platform:** macOS, Darwin 25.3.0
- **Date:** 2026-08-09
- **Models:** `claude-haiku-4-5-20251001` throughout, plus one `claude-sonnet-5` run
- **Also present on this machine:** `codex-cli 0.145.0` (recorded for the wave-2
  Codex spike; not exercised here)

Raw captures for every claim live in [`transcripts/`](transcripts/README.md); the
drivers are in [`spikes/harness/`](../../spikes/harness/) and the Rust proof-of-concept
in [`spikes/claude-probe/`](../../spikes/claude-probe/).

## Capability matrix

| # | Capability | Verdict | Notes |
|---|---|---|---|
| 1 | Basic turn | **Works** | `-p --input-format stream-json --output-format stream-json --verbose`. `--verbose` is required or the stream is collapsed. |
| 2 | Multi-turn on one process | **Works** | Many user messages on one stdin; one `result` each; context retained; stable session id. |
| 3 | Tool use | **Works** | `tool_use` block, matching `tool_result`, plus a `tool_use_result` sidecar with structured detail. |
| 4 | Permission prompting | **Works** — stdio **and** HTTP | Deny, allow, and **input rewrite** all observed. Server may stall ≥180 s with no timeout. |
| 5 | Resume / revival | **Works** | `--resume <id>` in a fresh process; context survives, session id preserved. |
| 6 | Usage extraction | **Works, with a trap** | `usage` is per-turn; `total_cost_usd`, `duration_api_ms`, and `modelUsage` are **cumulative per process**. |
| 7 | Interruption | **Works** — via control message only | `control_request`/`interrupt` aborts the turn and keeps the process alive. stdin-close and SIGINT do not do this. |
| 8 | System prompt + model | **Works** | `--append-system-prompt` and `--model` both honored in stream-json mode. |
| 9 | Config isolation | **Works, strictly** | A fresh `CLAUDE_CONFIG_DIR` is fully unauthenticated — no credential leak from the ambient config or keychain. |
| 10 | Codex CLI present | n/a | `codex-cli 0.145.0`. Recorded only. |

## Wire protocol as observed

### Input (stdin, newline-delimited JSON)

A user message:

```json
{"type":"user","message":{"role":"user","content":[{"type":"text","text":"..."}]}}
```

A control request (this is how you interrupt):

```json
{"type":"control_request","request_id":"horde-interrupt-1","request":{"subtype":"interrupt"}}
```

### Output (stdout, newline-delimited JSON)

Every line carries `type`, and almost every line carries `session_id` and `uuid`.
Observed `type` values:

- `system` with `subtype`: `init`, `thinking_tokens`, `status`, and — when the
  operator has hooks — `hook_started` / `hook_response`.
- `assistant` — an Anthropic-API `message` with `content` blocks (`thinking`,
  `text`, `tool_use`), plus `parent_tool_use_id`, `request_id`, `timestamp`.
- `user` — echoed back to carry `tool_result` blocks, with a `tool_use_result`
  sidecar holding structured detail (for `Write`: `filePath`, `content`,
  `structuredPatch`, `originalFile`, `userModified`).
- `result` — closes the turn. See below.
- `control_response` — `{"response":{"subtype":"success","request_id":...,"response":{"still_queued":[]}}}`.
- `rate_limit_event` — `{"rate_limit_info":{"status":"allowed","resetsAt":<unix>,"rateLimitType":"five_hour","overageStatus":...}}`. This is the adapter-reported headroom DESIGN.md §5 relies on for tier selection.
- `stream_event` — only with `--include-partial-messages`; wraps raw Anthropic
  streaming events (`message_start`, `content_block_start`, `content_block_delta`,
  `content_block_stop`, `message_delta`, `message_stop`) for token-level TUI output.

The `init` event advertises `capabilities`, which on 2.1.226 reads
`["interrupt_receipt_v1","interrupt_cancel_queued_v1","msg_lifecycle_v1"]`. That is
the honest place for the adapter to negotiate behavior across CLI versions rather
than pinning a version number.

### The `result` envelope

```json
{"type":"result","subtype":"success","is_error":false,"session_id":"...",
 "result":"HELLO_HORDE","total_cost_usd":0.0037,"num_turns":1,
 "duration_ms":3000,"duration_api_ms":7123,"stop_reason":"end_turn",
 "terminal_reason":"completed","api_error_status":null,
 "usage":{"input_tokens":10,"output_tokens":47,
          "cache_creation_input_tokens":12226,"cache_read_input_tokens":18976,
          "server_tool_use":{...},"service_tier":"standard"},
 "modelUsage":{"claude-haiku-4-5-20251001":{"inputTokens":539,"outputTokens":451,
          "cacheReadInputTokens":6398,"cacheCreationInputTokens":138,
          "costUSD":0.0037,"contextWindow":200000,"maxOutputTokens":32000,
          "canonicalModel":"claude-haiku-4-5","provider":"firstParty"}},
 "permission_denials":[],"ttft_ms":1417,"uuid":"..."}
```

## Findings that change how we build the adapter

### Permission interception is stronger than the design assumed

`--permission-prompt-tool` is **absent from `--help`** but fully functional. It is a
real, argument-taking option — `claude -p --permission-prompt-tool` errors with
`option '--permission-prompt-tool <tool>' argument missing`, whereas an invented flag
errors with `unknown option`. Being undocumented, it is a version-fragility risk worth
a conformance test, not a blocker.

The named MCP tool receives `{tool_name, input, tool_use_id}` and returns a
JSON-stringified decision in a text content block. Three behaviors verified:

- **Deny** — `{"behavior":"deny","message":"..."}`. The tool never runs, the model
  sees the message as an `is_error: true` tool_result, and the turn's `result`
  carries a structured `permission_denials` entry with the tool name, id, and the
  full attempted input. That gives Horde a first-class audit record for free.
- **Allow** — `{"behavior":"allow","updatedInput":{...}}`.
- **Rewrite** — the tool executes `updatedInput`, not what the model proposed. A
  `Write` of `ORIGINAL` came back out of the file as `REWRITTEN_BY_HORDE`. The
  permission chain can therefore *narrow* a call (clamp a path, strip a flag), not
  merely gate it — worth designing into DESIGN.md §10 rather than discovering later.

**The human-approval leg works.** A permission server that slept **180 seconds**
before answering was waited on without complaint: `duration_ms: 183805`,
`duration_api_ms: 4558`, turn completed successfully. No MCP or CLI timeout fired.
Approval-from-your-phone is viable with no polling or resume trickery.

**HTTP endpoints work, not just stdio.** The same interception succeeded against a
streamable-HTTP MCP server on `127.0.0.1`. Horde's one-endpoint-per-agent surface
(DESIGN.md §5) can be an HTTP service the worker exposes; it does not have to be a
stdio child per agent.

### Interruption: exactly one mechanism does what we need

| Mechanism | Turn aborted? | Process survives? | Session resumable? |
|---|---|---|---|
| `control_request` / `interrupt` | Yes | **Yes** | Yes |
| SIGINT | Yes | No — process exits | Yes |
| Close stdin | **No** — turn runs to completion | No — exits after | Yes |

Only the control message gives the abort-and-keep-steering behavior that DESIGN.md
§5 revival and the TUI's interrupt need. The result of an interrupted turn is
`subtype: "error_during_execution"`, `is_error: true`,
`terminal_reason: "aborted_streaming"`, with an `errors` array containing an
`[ede_diagnostic]` string. After the interrupt the same process accepted a new user
message and answered normally, and resuming the session in a fresh process showed
the model still had the partial work in context ("I was counting from 1 to 300…").

Closing stdin as an interrupt is the trap here: it looks like cancellation and is
actually "finish what you're doing, then exit."

### Usage accounting accumulates — diff it, do not sum it

Across three turns on one process:

| Turn | `usage.output_tokens` | `modelUsage.outputTokens` | `total_cost_usd` |
|---|---|---|---|
| 1 | 440 | 451 | 0.0037098 |
| 2 | 203 | 654 | 0.0063184 |
| 3 | 59 | 713 | 0.0077755 |

`usage.*` is **per-turn**. `modelUsage.*`, `total_cost_usd`, and `duration_api_ms`
are **cumulative for the process lifetime**, and reset when the process is replaced
(a `--resume` run starts its counters at zero). A worker that sums `total_cost_usd`
across results will over-bill quadratically; it must diff consecutive values within
a process and treat each process as a fresh accumulator.

`modelUsage` is also keyed by model and can hold **more than one entry even when
`--model` names a single model** — the `--model sonnet` run billed both
`claude-sonnet-5` and `claude-haiku-4-5-20251001`, because auxiliary work runs on a
small model. Per-account quota tracking must sum the map, not trust the requested
model name.

Casing is inconsistent and will silently zero your structs: `usage` members are
`snake_case`, `modelUsage` members are `camelCase` (`inputTokens`, `costUSD`). This
spike shipped that exact bug and caught it only because a token count read `0`.

### `is_error` is the truth, not `subtype`

An unauthenticated run returns `subtype: "success"` **with** `is_error: true`,
`terminal_reason: "api_error"`, and `result: "Not logged in · Please run /login"`
(process exit code 1). An adapter that branches on `subtype` alone will report a
failed agent as successful. Branch on `is_error` first, then `subtype`, then
`terminal_reason`.

### `system/init` is per-turn, not per-process

Each user message produces a fresh `init` event on one long-lived process — three
turns, three `init`s. It is a turn header, not a process handshake. An adapter that
treats a second `init` as a new process or a protocol violation will break on the
second turn.

### Session and config isolation

`CLAUDE_CONFIG_DIR` fully isolates an account. Pointed at an empty directory, the
CLI created its own `projects/`, `sessions/`, `.claude.json`, and `backups/` inside
it, and refused to run: `Not logged in · Please run /login`, exit code 1. **OAuth
credentials did not leak** from the ambient `~/.claude` or the macOS keychain, which
is exactly the property DESIGN.md §4 needs for multi-account workers — and it means
an unauthenticated account fails loudly at spawn rather than silently borrowing
another account's identity.

The consequence to design around: **sessions are stored under the config dir**, so a
session id is only resumable under the same `CLAUDE_CONFIG_DIR`. The account binding
is part of an agent's revival identity and must be journaled alongside the session
id, not inferred at revival time. This reinforces DESIGN.md §5's rule that a spawn
binding is held for the agent's life.

### Operator environment bleeds into the protocol

By default the CLI loads the user's settings, hooks, plugins, MCP servers, and
`CLAUDE.md`. The very first probe run emitted two `SessionStart` hook events and
injected an unrelated project's memory into the transcript. For agents Horde
supervises this is both a reproducibility and an isolation problem. All captures
here used `--setting-sources '' --strict-mcp-config`; the production adapter should
do the same and inject context explicitly. (`--bare` goes further but forces
`ANTHROPIC_API_KEY` auth, so it is unusable for subscription accounts.)

Related: with tools disabled the model may still *narrate* tool calls as plain text
(the probe printed `<write>` blocks that wrote nothing). Horde must never parse
assistant text for actions — only `tool_use` blocks are real.

## Footguns, collected

1. Closing stdin does not interrupt; it completes the turn.
2. `subtype: "success"` can carry `is_error: true`.
3. `total_cost_usd` / `modelUsage` / `duration_api_ms` are cumulative per process.
4. `modelUsage` is camelCase while `usage` is snake_case.
5. `modelUsage` may list models you did not request.
6. `system/init` repeats every turn.
7. `--permission-prompt-tool` is undocumented in `--help`; pin it with a conformance test.
8. `--verbose` is mandatory alongside `-p` for stream-json output.
9. Session ids are scoped to `CLAUDE_CONFIG_DIR`.
10. Ambient user settings/hooks/MCP load by default and pollute the stream.

## Verdict

**Proceed with stream-json. ADR-0003 is confirmed; no fallback is required.**

The reasoning: every capability on the critical path was demonstrated against the
real binary, and the one we considered most likely to force an Agent SDK shim —
external permission interception — turned out to be *richer* than the design
assumed, supporting deny, allow, input rewriting, HTTP transport, and multi-minute
stalls for human approval. Interrupt, resume, usage, model selection, prompt
overlays, and account isolation all work. Nothing needed the SDK.

**Fallback assessment, for the record.**

- *Agent SDK shim* — unnecessary, and now known to be strictly worse. The SDK is a
  wrapper over this same wire protocol, so it cannot expose a capability we failed
  to find here; it would only add a Node process per agent, a second version-skew
  surface, and a translation layer between us and the bytes we already parse
  cleanly. Keep it as a documented escape hatch, not a plan.
- *ACP* — remains the right seam for **other** harnesses (Codex et al., DESIGN.md §4)
  and should be pursued there on its own merits. Adopting it for Claude would mean
  giving up capabilities ACP does not model, most importantly the `updatedInput`
  rewrite and the structured `permission_denials` audit record.

**The real risk is not capability, it is stability.** `--permission-prompt-tool` is
undocumented, the control-message protocol is version-gated behind an `init`
capability list, and accounting-field casing is inconsistent enough to suggest it is
not a frozen contract. The mitigation is the conformance suite the golden transcripts
were captured for: replay them against the adapter in CI, and negotiate on the `init`
`capabilities` array rather than on a version string. That belongs in the wave-5
"Claude adapter productionized" work.

**Carried into wave 2 (adapter trait):** `resumable: true` for Claude; the adapter
trait's `interrupt` maps to a control message, not a signal; usage reporting needs a
per-process accumulator that diffs rather than sums; the account (config dir) is part
of an agent's revival identity.
