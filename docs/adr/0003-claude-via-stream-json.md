# Drive Claude Code via stream-json, not the Agent SDK

The Agent SDK is TypeScript/Python, which would force a Node sidecar per Claude agent inside a Rust worker. The SDK is largely a wrapper over `claude --input-format stream-json --output-format stream-json`, so the worker speaks that protocol directly to the `claude` binary: turn streaming, `--resume` for revival, usage in result messages, and permission prompting via `--permission-prompt-tool` pointed at the horde MCP endpoint. This keeps workers pure Rust and treats Claude like every provider: a supervised child process with a wire protocol.

**Status: verified 2026-08-09** by spike against claude 2.1.226 (see `docs/spikes/claude-stream-json.md` and its golden transcripts). All ten capability probes passed; permission interception additionally supports input rewrite (`updatedInput`) and HTTP MCP servers. The Agent SDK fallback is rejected — it wraps this same protocol and can only add a process and a version-skew surface. Key adapter constraints discovered: cumulative-vs-per-turn usage fields must be diffed; `modelUsage` may bill models beyond the requested one; interrupt is a control message (not a signal, and stdin-close does not interrupt); session ids are only resumable under the same `CLAUDE_CONFIG_DIR`, so account identity must be journaled with the session id; `system/init` arrives per turn; `is_error` is authoritative over `subtype`.

**Permission interception is conditional, not unconditional** (re-verified 2026-08-11 against claude 2.1.228; see `docs/research/mcp-2026-audit.md`). Ambient `settings.json` allowlists decide a tool call *before* `--permission-prompt-tool` ever sees it, so an allowed call is executed with no prompt, no denial, and no event — a silent bypass of the §10 chain rather than an error. For that chain to be the real authority, the worker **must own the permission settings the harness reads**: pass an explicit `--settings` with empty allow/deny lists and `--permission-mode manual` per agent, and never inherit the operator's `~/.claude/settings.json`. The flag itself is intact at 2.1.228 but has become hidden-but-functional — no longer listed in `--help`, still accepted and still delivering `{tool_name, input, tool_use_id}`.

**Superseded in part by ADR-0010:** `--settings` alone is not enough — Claude merges allow lists across every settings source — so every launch also passes `--setting-sources ''` and `--strict-mcp-config` and runs under a Worker-owned `CLAUDE_CONFIG_DIR`.

## Amendment, 2026-11-13 (W12c): permission prompts move to stdio; versions are classified

Re-probed against claude **2.1.323** (`spikes/harness/drive_stdio.py`,
transcripts `12a`–`17`). The transport decision stands — stream-json over the
CLI, no Node Agent SDK, no ACP — and nothing in the measured protocol drifted:
multi-turn, `--resume`, the interrupt control message (`aborted_streaming`,
process still usable), cumulative usage and the MCP permission-tool route all
behave as W1c recorded. Two additions are new on the wire: `system/init` now
carries `claude_code_version`, and a `--permission-prompts host|none` flag
exists beside `--permission-prompt-tool`. `--permission-mode` lists `manual`,
which the init reports as `permissionMode: "default"`.

**The permission route changes.** `--permission-prompt-tool stdio` delivers
each prompt on stdout as `{"type":"control_request","request_id":…,
"request":{"subtype":"can_use_tool","tool_name":…,"input":…,"tool_use_id":…,
"permission_suggestions":[…]}}` and waits for `{"type":"control_response",
"response":{"subtype":"success","request_id":…,"response":{"behavior":
"allow","updatedInput":…}}}` (or `"deny"` with a `"message"`) on stdin — the
exchange the Agent SDK's `canUseTool` rides, so this is the SDK's own seam,
without the SDK. Measured: allow; deny (the message is the agent's tool
error, and the result envelope lists the denial); a narrowing `updatedInput`
(the narrowed content was written); a 90-second stall answered with no
timeout; a session `updatedPermissions` grant honoured on the next turn;
`AskUserQuestion` arriving as a prompt whose allow carries `answers` keyed by
question text. The Worker now launches with the stdio route whenever a chain
is wired and the binary is 2.1.323 or newer; the MCP approve tool remains the
fallback for 2.1.226–2.1.322, where only it was measured. `--settings` with
empty lists and `--permission-mode manual` ride both routes, as before.

Horde never sends `updatedPermissions`: the chain's remember-for-session is
exact-call memory, and Claude's grants are tool- or mode-wide, so mirroring
one would widen what runs unasked. Every control request is answered (an
unhandled subtype with an error, a stray prompt with a denial), and an
interrupt denies any parked prompt before it is sent.

**Versions are classified.** Spawned binaries are probed (`--version`) and
classified against a support table: below 2.1.226 is refused (the Worker
stops at startup; a spawn returns `RefusedVersion`), 2.1.226–2.1.323 is
supported, anything newer is untested and journaled as a warning on each
agent's first turn. `horde doctor` reports the same verdict.
