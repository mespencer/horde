# W2c transcripts — driving Codex

Raw wire recordings from the W2c spike (`codex-cli 0.145.0`, macOS arm64,
ChatGPT Pro login). Findings and the verdict live in `../codex-driving.md`.

Files 01–11 and 14 record the **app-server** protocol via `drive.py` / the Rust
probe: each line is `{"dir": "in"|"out", "t": <unix seconds>, "msg": <JSON-RPC
frame>}`, where `out` is client→Codex. Files 12–13 record **`codex exec
--json`**, whose output is plain JSONL with no envelope — a different and much
thinner schema.

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
| `11-interrupt.jsonl` | `turn/interrupt` mid-turn: 394 deltas still arrived afterwards, and the turn ended via `thread/status/changed → idle` with **no** `turn/completed` |
| `12-exec-json.jsonl` | `codex exec --json` — only 4 event types, no streaming deltas |
| `13-exec-resume.jsonl` | `codex exec resume <id>` in a fresh process — context survived, input tokens doubled from history replay |
| `14-rust-probe.jsonl` | the same turn driven from Rust by `spikes/codex-probe` |

Redaction: account email, account/installation ids, and the scratch session path
are replaced; third-party MCP tool schemas are elided to a count. Nothing here
contains credentials — `auth.json` was never read into a transcript.
