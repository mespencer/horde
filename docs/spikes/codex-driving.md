# Spike W2c — driving the Codex CLI for GPT-subscription agents

**Exact version: `codex-cli 0.145.0`**, macOS 26.3.1 arm64. Auth: `codex login
status` → "Logged in using ChatGPT", `auth.json` has `auth_mode` set and
`OPENAI_API_KEY: null`; `account/read` reports `{"type":"chatgpt","planType":
"pro"}`. So every result below is a genuine **subscription** result, not an
API-key one.

Evidence: `transcripts/` (README indexes all 14). Harness: `drive.py` (Python,
protocol exploration) and `spikes/codex-probe/` (Rust, proves the chosen surface
drives from a Rust supervisor).

## VERDICT: native app-server protocol

Use **`codex app-server --stdio`** — JSON-RPC 2.0 over newline-delimited stdio.

| Candidate | Verdict |
|---|---|
| **`codex app-server --stdio`** | **Chosen.** 89 client methods, 70 server notifications, 10 server→client requests. Persistent, multi-turn, streaming. Schema is machine-generated (`codex app-server generate-json-schema --out <dir>`), so each Codex upgrade is a diff rather than a surprise. Verified drivable from Rust. |
| `codex exec --json` | **Rejected as the driving path.** One-shot, and its event stream is far thinner — 4 event types, `item.completed` only, **no streaming deltas**. Horde could not render live agent output. Useful as a smoke test. |
| `codex mcp-server` | **Rejected.** Makes Codex the MCP server and Horde the tool host — inverts supervision. |
| ACP | **Rejected.** Not implemented natively by codex 0.145.0. Would need an external bridge plus a second protocol to version-skew against, for no capability the app-server lacks. |

## Capability matrix

| # | Capability | Answer | Evidence |
|---|---|---|---|
| 1 | Headless/programmatic turn | **Yes, two ways.** app-server `thread/start`+`turn/start`; or `codex exec --json`. exec emits only `thread.started` / `turn.started` / `item.completed` / `turn.completed` (dot-separated, no envelope), with usage on `turn.completed` and **no deltas**. | `12`, `03` |
| 2 | Persistent protocol mode | **Yes** — app-server is exactly that, and is strictly richer than repeated `exec`. | `01`–`03` |
| 3 | Tool-use observation | **Yes**, live. `item/started` / `item/completed` with `type: commandExecution` carry the full command line, cwd, `processId`, parsed `commandActions`, status and exit code; `item/commandExecution/outputDelta` streams output. | `04`, `09` |
| 4 | **Approval interception** | **NO — see below.** The critical capability, and it does not work. | `04`–`10` |
| 5 | Resume | **Yes.** `codex exec resume <thread-id>` in a *fresh process* recalled the token from the prior process; input tokens went 17 072 → 34 175, consistent with history replay. Threads persist as rollout JSONL under `~/.codex/sessions/`. app-server has `thread/resume`, `thread/fork`, `thread/list`. Adapter flag `resumable` = **true**. | `13` |
| 6 | Usage / limits | **Yes, both.** `thread/tokenUsage/updated` gives `last` **and** `total` per turn — no cumulative diffing needed, unlike Claude. `account/rateLimits/read` + `account/rateLimits/updated` give provider-reported headroom. | `03`, `02` |
| 7 | Interrupt | **Yes, with two traps.** `turn/interrupt` stopped the turn (`thread/status/changed` → `idle`), but **394 further deltas arrived after the interrupt was sent**, and **no `turn/completed` or `turn/aborted` was emitted**. An adapter that waits for a turn terminal event after interrupting will hang. | `11` |
| 8 | Prompt injection + model selection | **Yes.** `thread/start` takes `baseInstructions` (replaces the base system prompt) and `developerInstructions` (adds a developer message) — the two seams Horde's prompt overlays need. Model/tier: `model`, `modelProvider`, `serviceTier`, `personality`, plus `model/list` for discovery and a `config` escape hatch. | schema, `02` |
| 9 | Config isolation per account | **Yes — `CODEX_HOME`.** The direct analogue of `CLAUDE_CONFIG_DIR`: it locates both config and credentials. `--ignore-user-config` skips `config.toml` but the docs state "auth still uses `CODEX_HOME`", so **`CODEX_HOME` is the account boundary** and one Provider Account = one `CODEX_HOME`. `-p/--profile` layers `$CODEX_HOME/<name>.config.toml` for variants within an account. *Documented only; no account switching performed.* | `codex exec --help` |
| 10 | Sandbox modes | `SandboxMode` = `read-only` \| `workspace-write` \| `danger-full-access`, set per thread. Observed: network is blocked in every tier (curl → exit 6); `workspace-write` permits writes in cwd **and** `/private/tmp`. **Horde should set this explicitly per spawn and never use `danger-full-access`**; since approvals do not reach Horde (#4), this flag *is* the enforcement. | `07`–`09` |

## The permission finding (#4)

Horde's permission chain (DESIGN §10) assumes the orchestrator sees a proposed
tool call before it runs. It cannot, for Codex. Six configurations produced
**zero** client-facing approval requests:

| Sandbox | Policy | Probe | Outcome |
|---|---|---|---|
| read-only | untrusted | `echo` | ran, auto-approved |
| read-only | untrusted | write in cwd | blocked, model looped |
| read-only | granular (all true) | write in cwd | blocked, model looped |
| workspace-write | on-request | `curl` | **failed exit 6** — denied, not escalated |
| workspace-write | on-request | write in cwd | succeeded exit 0 (permitted) |
| workspace-write | on-request | write to `/private/tmp` | succeeded exit 0 (permitted) |
| read-only | on-request + both flags | write in cwd | no request |

`codex features list` explains it:

```
exec_permission_approvals            under development  false
request_permissions_tool             under development  false
guardian_approval                    stable             true
```

The two features that route exec/permission approvals to a client are
**under development and off by default**, and enabling both per-invocation did
not change the behaviour. What ships is `guardian_approval`: Codex adjudicates
against its own sandbox tiers and project-trust model, then tells the client
after the fact via `item/started [commandExecution]`.

There is no `--permission-prompt-tool` equivalent. Re-check both flags on every
Codex upgrade.

### Project trust can silently disable approvals

During the first three probes, `~/.codex/config.toml` gained
`[projects."<cwd>"] trust_level = "trusted"` entries for the two scratch
directories the spike used, and a trusted project skips approvals wholesale.
The user's config already trusts `/Users/myles`, which covers most real
workspaces.

**Caveat on the trigger:** later `thread/start` calls against the *same*
directories (the Rust probe, the interrupt probe) did **not** re-add the
entries, and `codex exec` never added any. So Codex writes project trust under
conditions this spike did not isolate — it is not a reliable per-`thread/start`
side effect. What is established: Codex may write to its own config outside
Horde's control, and a trusted cwd changes approval behaviour. Both entries the
spike caused were removed and the removal verified by diff.

## Reproducing

```
codex app-server generate-json-schema --out <dir>   # whole protocol, no tokens spent
python3 drive.py freeprobes                         # models, rate limits, MCP, profiles
python3 drive.py turn                               # one streamed turn
python3 drive.py interrupt                          # turn/interrupt semantics
cd spikes/codex-probe && cargo run -- /tmp/probe.jsonl   # the Rust path
codex exec --json --sandbox read-only -C <dir> "..."     # the thin one-shot schema
```

`drive.py` starts threads read-only and declines every approval by default.
Note that `codex exec` requires its flags **before** the `resume` subcommand;
`codex exec resume <id> --sandbox read-only` fails with
`error: unexpected argument '--sandbox' found`.

## What this means for Horde

- Codex agents get a **coarser permission story than Claude agents**. Sandbox
  tier plus cwd, chosen at `thread/start`, are the real controls. This makes the
  orchestrator-level sandbox profile (§10, "sequenced later") more urgent for
  Codex than for Claude.
- `AdapterCapability` needs a value meaning **"cannot pre-authorize tool
  calls"**, so permission modes and placement can refuse to put a high-trust
  assignment on a Codex agent. The enum currently implies every adapter is
  gateable.
- `horde.v1.RateWindow` models `used`/`limit` as `uint64`; Codex reports
  `usedPercent` with `windowDurationMins`/`resetsAt` and no absolute counts.
- `horde.v1.TokenUsage` has no field for `reasoningOutputTokens`, which both
  Codex surfaces report separately.
- Interrupt handling must key on `thread/status/changed → idle` and tolerate a
  tail of deltas, not wait for `turn/completed`.
