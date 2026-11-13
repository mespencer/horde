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
| `17-mcp-elicitation-accept.jsonl` | **codex-cli 0.147.0, 2026-08-13** (W5b, `drive_mcp.py`): MCP tool call under granular `mcp_elicitations: true` → `mcpServer/elicitation/request` arrives **before** the call runs, carrying `_meta.tool_params`; answered `{"action":"accept","content":{}}`; the paired `.mcp-stub.jsonl` proves the first byte reached the MCP server only after the answer |
| `18-mcp-elicitation-decline.jsonl` | same flow answered `{"action":"decline"}` → **no byte reaches the MCP server**, `item/completed [mcpToolCall]` reports `status: "failed"`, `error.message: "user rejected MCP tool call"` |
| `19-mcp-elicitation-onrequest.jsonl` | the negative control: under plain `approvalPolicy: "on-request"` **no elicitation is ever routed** — the thread parks at `status: active, activeFlags: ["waitingOnApproval"]` and the turn never completes. Granular `mcp_elicitations` is required, not optional |

Files 20–33 are the **exec/shell approval re-probe** (`drive_approvals.py`),
recorded 2026-08-19 against **codex-cli 0.148.0** (downloaded package binary)
and **0.147.0** (the installed Homebrew cask), both under the same ChatGPT
login. Each run gets a fresh throwaway cwd under `/private/tmp` and a fresh
`CODEX_HOME` holding only a copy of `auth.json`, so the user's MCP servers,
hooks, and per-project trust are out of the loop (a `-c mcp_servers={}` override
was tried first and does **not** remove configured servers — see the two 20-*
runs). Every turn is bounded at 60s; a config that never completes is recorded
as a HANG, not waited out. The `dir:"meta"` lines carry the config and the
distilled finding.

| File | ver | config | observed |
|---|---|---|---|
| `20-exec-user-onrequest-ro-vscode-write.jsonl` | 0.148 | reviewer=user, on-request, read-only, client=`codex_vscode`, write, **decline** | `item/commandExecution/requestApproval` **before** exec; declined → `status:"declined"`, no file written |
| `21-exec-user-onrequest-ro-horde-write.jsonl` | 0.148 | same but client=`horde-worker` | identical — client name is **not** a gate; approval fired, declined, blocked |
| `22-exec-unset-onrequest-ro-horde-write.jsonl` | 0.148 | reviewer **unset**, on-request, read-only, write, decline | approval fired, declined, blocked — `approvalsReviewer` defaults to `user` |
| `23-exec-autoreview-onrequest-ro-horde-write.jsonl` | 0.148 | reviewer=**`auto_review`**, on-request, read-only, write, decline | **NO client request. Codex's own subagent approved the escalation and the write RAN (`exitCode:0`) under a read-only sandbox.** The security hazard |
| `24-exec-user-untrusted-ro-horde-echo.jsonl` | 0.148 | reviewer=user, **untrusted**, read-only, echo, decline | no request; a trivially-safe `echo` is auto-approved and runs. `untrusted` still asks nothing for safe reads |
| `25-exec-user-granular-ro-horde-write.jsonl` | 0.148 | reviewer=user, **granular** (Horde's shipping policy), read-only, write, decline | `item/commandExecution/requestApproval` before exec (`reason:"command failed; retry without sandbox?"`), declined, blocked |
| `26-exec-user-onrequest-ww-horde-escape.jsonl` | 0.148 | on-request, **workspace-write**, write to `$HOME`, decline | approval fired for the out-of-workspace escape, declined, blocked |
| `27-exec-user-onrequest-ww-horde-net.jsonl` | 0.148 | on-request, workspace-write, `curl`, decline | sandboxed attempt runs first and **fails exit 6** (DNS blocked); codex then asks to retry outside sandbox; declined. `exec_done_before_ask` is the failed in-sandbox try, not a gate miss |
| `28-filechange-user-onrequest-ro-horde-patch.jsonl` | 0.148 | on-request, read-only, **apply_patch**, decline | `item/started[fileChange]` carries the full diff, then **parks at `waitingOnApproval` forever** — no `item/fileChange/requestApproval` is routed |
| `29-…-write-accept.jsonl` | 0.148 | on-request, read-only, write, **accept** | approval fired; **accepted → write RAN `exitCode:0`**. Proves accept is honored end-to-end |
| `30-…-write-amend.jsonl` | 0.148 | on-request, read-only, write, **acceptWithExecpolicyAmendment** | amendment accepted, write ran; a **second turn** re-ran the same command and completed — the amendment shape deserializes and is honored |
| `31-filechange-user-granular-ro-horde-patch.jsonl` | 0.148 | **granular**, read-only, apply_patch, decline | same park as 28 — granular does not unpark file-change; the fileChange approval never reaches the client in either policy |
| `32-0147-exec-user-onrequest-ro-horde-write.jsonl` | **0.147** | on-request, read-only, write, decline | **NO request — parks at `waitingOnApproval`.** At 0.147 `on-request` does not route exec approvals |
| `33-0147-exec-user-granular-ro-horde-write.jsonl` | **0.147** | **granular**, read-only, write, decline | `item/commandExecution/requestApproval` **does** fire and decline blocks it. Granular already routed exec at 0.147; only `on-request` changed at 0.148 |
| `34-0167-exec-user-granular-ro-horde-write.jsonl` | **0.167** | granular, read-only, write, decline | Exec approval unchanged: asked before the command, declined, blocked. The request gains `kind:"command"`; every notification gains `emittedAtMs` |
| `35-0167-…-write-accept.jsonl` | 0.167 | same, accept | Accepted, the write ran |
| `36-0167-filechange-user-granular-ro-horde-patch.jsonl` | 0.167 | granular, read-only, apply_patch, decline | **`item/fileChange/requestApproval` now routes** (28/31 parked): the patch rides `item/started [fileChange].changes`, the request names the item; declined → item `declined` |
| `37-0167-…-patch-accept.jsonl` | 0.167 | same, accept | Accepted, the patch applied |
| `38-0167-userinput-user-granular-ro-horde-ask.jsonl` | 0.167 | `--enable default_mode_request_user_input` | `item/tool/requestUserInput` with id/header/question/options; answered `{"answers":{"colour":{"answers":["Red"]}}}`, the model replied "Red" |
| `39-0167-permissions-user-granular-ro-horde-perms.jsonl` | 0.167 | `--enable request_permissions_tool`, accept | `item/permissions/requestApproval` for `{"network":{"enabled":true}}`; granting it (scope `turn`) let the following exec — itself approved — reach the network |
| `40-0167-exec-user-onrequest-ro-horde-write.jsonl` | 0.167 | on-request, read-only, write, decline | Same as 34: plain `on-request` routes exec too |
| `41-0167-basic-user-granular-ro-horde-two-turns.jsonl` | 0.167 | granular, two plain turns | The baseline: `turn/completed` per turn, per-call usage readings |

Files 17–19 were recorded by `drive_mcp.py` with `mcp_stub.py` as the upstream
MCP server (configured per-process via `-c mcp_servers.…` overrides, so the
user's `config.toml` is never touched — verified unchanged by diff). They
re-establish, with preserved bytes, the interception point the rmcp dual-era
spike measured and lost (docs/spikes/rmcp-dual-era.md §4). The `_meta` also
carries `tool_params_display`, which the lost captures never recorded. Note the
elicitation params carry the **turn id and server name but not the tool name**;
the tool's identity comes from the `item/started [mcpToolCall]` that precedes
the request in the same turn.

Files 34–41 (W12c, 2026-11-13) were recorded by `drive_approvals.py` against
codex-cli 0.167.0 on Linux, after two driver fixes: frames are read on a
thread (the old `select()` + `readline()` lost buffered frames until the next
write — `emittedAtMs` showed codex had sent them on time), and an idle status
no longer ends the turn before its `turn/completed`. Their scratch paths are
rewritten to `/tmp/horde-w12c/…`, and the installation id, host name and user
agent are scrubbed.

Redaction: account email, account/installation ids, and the scratch session path
are replaced; third-party MCP tool schemas are elided to a count. No credentials
appear — `auth.json` was never read into a transcript. In files 20–33 the
`installationId`, `serverName` (hostname), `userAgent`, and the temp
`CODEX_HOME` path are likewise scrubbed. The `auth.json` copied into the probe
`CODEX_HOME` is not part of any transcript.
