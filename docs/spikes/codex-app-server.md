# Spike W2c — driving Codex CLI for GPT-subscription agents

Verified 2026-08-09 against `codex-cli 0.145.0` on macOS arm64, authenticated
with a ChatGPT **Pro** subscription (`codex login status` → "Logged in using
ChatGPT"; `auth.json` holds no `OPENAI_API_KEY`). Every result below comes from
the golden transcripts in `codex-app-server/transcripts/`, produced by
`codex-app-server/drive.py`.

## Which surface

Codex 0.145.0 exposes four programmatic surfaces. Only one is a real adapter
seam:

| Surface | Verdict |
|---|---|
| `codex app-server --stdio` | **Chosen.** JSON-RPC over newline-delimited stdio, 89 client methods, 70 server notifications, 10 server→client requests. Schema is machine-generated (`codex app-server generate-json-schema`), so it can be diffed per Codex release. |
| `codex exec --json` | One-shot JSONL. No steering, no interrupt, no in-band approvals. Fine for a smoke test, not for an Agent that must be steered and revived. |
| `codex mcp-server` | Codex as an MCP server. Inverts the relationship — Horde would be the tool host, not the supervisor. Wrong direction. |
| ACP | Not implemented natively by Codex. Would mean an external bridge and a second protocol to version-skew against. Rejected. |

## Capability matrix

| Horde need (DESIGN §4/§5) | app-server mechanism | Status |
|---|---|---|
| `spawn` | `thread/start` (cwd, model, sandbox, approvalPolicy, developerInstructions) | **Verified** |
| `send` | `turn/start` | **Verified** |
| `stream` | `item/started` / `item/agentMessage/delta` / `item/completed` / `turn/completed` | **Verified** |
| `interrupt` | `turn/interrupt` | Declared, untested |
| Steering at turn boundary | `turn/steer` — a *distinct* method from interrupt | Declared, untested |
| `resume` (adapter flag `resumable`) | `thread/resume`, plus `thread/fork`, `thread/list`, `thread/read`; threads persist as rollout files under `~/.codex/sessions/` | Declared, untested |
| Usage per turn | `thread/tokenUsage/updated` | **Verified** |
| Headroom (provider-reported) | `account/rateLimits/updated` notification + `account/rateLimits/read` | **Verified** |
| Model selection | `model/list`, `model` on `thread/start` | **Verified** (list) |
| MCP | `mcpServerStatus/list`, `mcpServer/tool/call`, `config/mcpServer/reload`, `mcpServer/elicitation/request` | **Verified** (list) |
| **Permission interception** | `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval`, `applyPatchApproval`, `execCommandApproval` | **NOT reproducible — see below** |

### Streaming shape, verified

One turn ("reply with exactly HORDE_OK") produced, in order: `thread/started`,
`turn/started`, `item/started`+`item/completed` (userMessage), `item/started`,
three `item/agentMessage/delta` frames (`HOR`, `DE`, `_OK`), `item/completed`
(agentMessage, `phase: final_answer`), `thread/tokenUsage/updated`,
`account/rateLimits/updated`, `turn/completed` (durationMs 6101).

Token usage arrives as both `total` and `last` per turn, so unlike Claude's
stream-json there is **no cumulative-vs-per-turn diffing to do** — `last` is the
turn. Fields map onto `horde.v1.TokenUsage` almost exactly:

| Codex | `horde.v1.TokenUsage` |
|---|---|
| `inputTokens` | `input_tokens` |
| `outputTokens` | `output_tokens` |
| `cachedInputTokens` | `cache_read_input_tokens` |
| `cacheWriteInputTokens` | `cache_write_input_tokens` |
| `reasoningOutputTokens` | *(no field — see open questions)* |

### Headroom, verified

`account/rateLimits/read` returns `usedPercent`, `windowDurationMins` (10080 =
a 7-day window), `resetsAt`, `planType`, and a per-`limitId` breakdown. This is
`HEADROOM_SOURCE_PROVIDER_REPORTED` for free.

**Mismatch:** Codex reports a *percentage used*, not absolute counts.
`horde.v1.RateWindow` models `used`/`limit` as `uint64`. See open questions.

## The permission finding

This is the result that matters, and it is negative.

Horde's permission chain (DESIGN §10) assumes the orchestrator sees a proposed
tool call *before* it runs. Across **six** configurations, a client-facing
approval request never arrived — `0` inbound server→client requests in every
transcript:

| # | Sandbox | Approval policy | Probe | Outcome |
|---|---|---|---|---|
| 04 | read-only | untrusted | `echo` | Ran. Auto-approved, no request. |
| 05 | read-only | untrusted | write in cwd | Blocked by sandbox; model looped; no request. |
| 06 | read-only | granular (all flags true) | write in cwd | Same. |
| 07 | workspace-write | on-request | `curl` (network) | Command **failed exit 6** (DNS blocked). Denied, not escalated. |
| 08 | workspace-write | on-request | write in cwd | Succeeded exit 0 — inside the workspace, so no escape. |
| 09 | workspace-write | on-request | write to `/private/tmp` | Succeeded exit 0 — Codex's workspace-write permits tmp. |
| 10 | read-only | on-request + `--enable exec_permission_approvals --enable request_permissions_tool` | write in cwd | No request. |

The explanation is in `codex features list`:

```
exec_permission_approvals            under development  false
request_permissions_tool             under development  false
guardian_approval                    stable             true
```

The two features that route exec/permission approvals to the client are
**under development and off**, and enabling them per-invocation did not change
the behaviour in probe 10. What ships today is `guardian_approval`: Codex
decides autonomously via its own sandbox tiers and trust model, and the client
is *told after the fact* through `item/started [commandExecution]`.

### Trap: project trust silently disables approvals

Probes 04–06 were additionally invalid because `thread/start` **writes a
`[projects."<cwd>"] trust_level = "trusted"` entry into `~/.codex/config.toml`**
for the thread's cwd, and a trusted project skips approvals wholesale. The
user's config already trusted `/Users/myles`, which covers most real
workspaces. Any future test of this area must run in a path Codex has not
recorded trust for, and must expect its config to be mutated as a side effect.

## Reproducing

```
codex app-server generate-json-schema --out <dir>      # protocol, no tokens spent
python3 drive.py freeprobes                            # models, rate limits, MCP, profiles
python3 drive.py turn                                  # one full streamed turn
python3 drive.py approval decline write on-request read-only flags
```

`drive.py` denies every approval by default and starts threads read-only, so a
probe cannot mutate anything outside its scratch cwd without the harness saying
yes.
