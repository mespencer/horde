# W2c transcripts — driving Codex

Raw wire recordings from spike W2c ([findings](../../codex-app-server.md)).
Recorded against **codex-cli 0.145.0** on macOS 26.3.1 arm64 on 2026-08-09,
under a ChatGPT Pro subscription login.

Files 01–11 and 14–16 record the **app-server** protocol: each line is
`{"dir": "in"|"out", "t": <unix seconds>, "msg": <JSON-RPC frame>}`, where `out`
is client→Codex. Files 12–13 record `codex exec --json`, whose output is plain
JSONL with no envelope — a different and much thinner schema.

| File | What it shows |
|---|---|
| `01-initialize.jsonl` | `initialize` handshake; NDJSON framing confirmed |
| `02-free-probes.jsonl` | `model/list`, `account/rateLimits/read`, `account/read`, `mcpServerStatus/list`, `permissionProfile/list` — everything answerable without a model turn |
| `03-basic-turn.jsonl` | One full streamed turn: deltas, `thread/tokenUsage/updated`, `account/rateLimits/updated`, `turn/completed` |
| `04-approval-echo-untrusted.jsonl` | `echo` under read-only + untrusted → ran, auto-approved, **no approval request** |
| `05-approval-write-untrusted.jsonl` | in-cwd write, read-only + untrusted → blocked, model looped, no request |
| `06-approval-write-granular.jsonl` | same with granular policy (all flags true) → no request |
| `07-approval-network-onrequest.jsonl` | `curl` under workspace-write + on-request → **failed exit 6** (DNS blocked); denied, not escalated |
| `08-approval-write-onrequest-workspacewrite.jsonl` | in-cwd write → succeeded exit 0 (permitted, so no escape) |
| `09-approval-outside-workspace.jsonl` | write to `/private/tmp` → succeeded exit 0 (tmp is permitted) |
| `10-approval-with-experimental-flags.jsonl` | `--enable exec_permission_approvals --enable request_permissions_tool` → still no request |
| `11-interrupt.jsonl` | `turn/interrupt` mid-turn: 394 deltas still arrived afterwards, turn ended via `thread/status/changed → idle` with **no** `turn/completed` |
| `12-exec-json.jsonl` | `codex exec --json` — only 4 event types, no streaming deltas |
| `13-exec-resume.jsonl` | `codex exec resume <id>` in a fresh process — context survived, input tokens doubled from history replay |
| `14-rust-probe.jsonl` | the same turn driven from Rust by `spikes/codex-probe` |
| `15-steer.jsonl` | `turn/steer` mid-turn: same turn id returned, and **two** `final_answer` items in one turn |
| `16-thread-resume.jsonl` | app-server `thread/resume` across processes — recalled `HORDE_OK`, and replayed the prior turn's usage event |

Redaction: account email, account/installation ids, and the scratch session path
are replaced; third-party MCP tool schemas are elided to a count. No credentials
appear — `auth.json` was never read into a transcript.
