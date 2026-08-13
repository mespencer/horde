# Drive Codex via app-server JSON-RPC, and treat its permission layer as opaque

Codex's `app-server` (JSON-RPC over stdio, `codex app-server --stdio`) is the
adapter seam for GPT-subscription agents. It maps onto the adapter trait almost
term for term — `thread/start` / `turn/start` / notification stream /
`turn/interrupt` / `turn/steer` / `thread/resume` — and it even splits steering
from interruption the same way Horde does. Its protocol is machine-generated
(`generate-json-schema`), so version skew is diffable per Codex release rather
than discovered in production. `codex exec --json` is too thin (no steer, no
interrupt, no resume), `codex mcp-server` inverts the supervision relationship,
and ACP is not native and would add a bridge plus a second skew surface.

**Status: verified 2026-08-09** against codex-cli 0.145.0 under a ChatGPT Pro
subscription (see `docs/spikes/codex-app-server.md` and its transcripts).
Streaming, per-turn token usage, and provider-reported rate-limit headroom all
work and need no reconstruction — `thread/tokenUsage/updated` reports `last`
per turn, so unlike Claude there is nothing to diff.

**The exception, and it is load-bearing:** Horde cannot intercept Codex tool
calls before they run. The protocol declares five approval requests, but
`exec_permission_approvals` and `request_permissions_tool` are under-development
feature flags that are off by default, and six configurations — including
enabling both flags explicitly — produced zero client-facing approval requests.
What ships is `guardian_approval`: Codex adjudicates against its own sandbox
tiers and project-trust model and notifies the client afterwards via
`item/started [commandExecution]`.

**Scoped 2026-08-11 — see the amendment at the end of this ADR.** Every probe
behind that conclusion was a *shell* command (`echo`, a write, `curl`), so what it
established is that shell/exec approvals do not reach the client. No MCP server was
in the loop. MCP tool calls *can* be intercepted.

## Consequences

- Codex agents get a **coarser permission story than Claude agents**. Horde's
  deterministic-deny / deterministic-allow / judge / human chain (§10) cannot
  gate a Codex tool call; the enforcement that actually applies is Codex's own
  sandbox tier, chosen by Horde at `thread/start`. *(Scoped by the 2026-08-11
  amendment: true for shell/exec, no longer true for MCP tool calls.)*
- Sandbox tier and `cwd` become the real permission controls for this provider,
  which makes the orchestrator-level sandbox profile (§10, "sequenced later")
  more urgent for Codex than for Claude.
- The adapter must advertise this honestly. `AdapterCapability` needs a value
  meaning "cannot pre-authorize tool calls" so placement and permission modes
  can refuse to put a high-trust assignment on a Codex agent.
- Codex may write to `~/.codex/config.toml` outside Horde's control, recording a
  cwd as a trusted project — and a trusted cwd disables approvals for it. The
  spike observed this for two directories but could not reproduce it on later
  `thread/start` calls against the same paths, so the trigger is not isolated.
  Workers must not assume an unmodified Codex config between spawns.
- Interrupt is not a clean stop: `turn/interrupt` ends the turn via
  `thread/status/changed → idle` and emits no `turn/completed`, while already-
  generated deltas keep arriving. An adapter awaiting a turn terminal event
  after interrupting will hang.
- `CODEX_HOME` is the Provider Account boundary — it locates credentials as well
  as config, so one account is one `CODEX_HOME`, mirroring `CLAUDE_CONFIG_DIR`.
- Re-check the two feature flags on each Codex upgrade; when they stabilize,
  this ADR should be revisited rather than worked around.

## Amendment, 2026-08-11: the exception is scoped to shell/exec, not to MCP tools

The rmcp dual-era spike (`docs/spikes/rmcp-dual-era.md` §4) drove codex-cli
**0.147.0** through `app-server` against a live MCP endpoint and measured an
interception point this ADR concluded did not exist. **For MCP tool calls
specifically, the "cannot intercept before they run" finding above is
superseded.** For shell and exec approvals it stands, unre-probed.

Codex routes every MCP tool call through a client-facing request:

```
SERVER-REQUEST mcpServer/elicitation/request
  {"serverName":"spike","mode":"form",
   "_meta":{"codex_approval_kind":"mcp_tool_call","persist":["session","always"],
            "tool_description":"Report the protocol version and HTTP headers…",
            "tool_params":{}}}
```

It carries the server name, tool name, tool description and the full arguments
**before the call runs**, and it supports `persist: ["session","always"]`. That is
precisely the seam DESIGN §10's deterministic/judge/human chain wants, so for MCP
tools a Codex agent is no coarser than a Claude one. Two details are load-bearing
for whoever implements it:

- **The answer's field is `action`, not `decision`.** The reply is an MCP
  elicitation result — `{"action":"accept"}` / `{"action":"decline"}` — and *not*
  the `decision` field that Codex's sandbox and exec approvals use. Sending the
  approval shape produces `failed to deserialize
  McpServerElicitationRequestResponse: missing field 'action'`.
- **`codex exec` cannot answer it at all.** Exec mode never declares the
  capability, so an MCP tool call made from `codex exec` is auto-cancelled
  outright — not merely unapproved: no byte of the call reaches the MCP server,
  and no `approval_policy` or `--disable` combination the spike tried changes
  that. Interception by this route therefore requires `app-server`, which is the
  transport this ADR already chose.

**What still stands, and what is owed.** The spike re-probed only the MCP tool-call
path. Shell and exec approvals were not exercised, so the six-configuration result
above remains Horde's only measurement of them and every consequence in this ADR
continues to hold there — the sandbox tier and `cwd` are still the real enforcement,
and `AdapterCapability` still needs to say so, now worded per call kind rather than
per provider. That measurement was taken against **0.145.0** and has not been
repeated since; the spike ran **0.147.0** and did not touch it. The "re-check the two
feature flags on each Codex upgrade" consequence above is two releases overdue, and
re-probing exec/shell approvals at 0.147.0 is the outstanding debt.

## Implementation note, 2026-08-13 (W5b): measured, captured, and built

The production adapter (`crates/horde-worker/src/codex.rs` over the
`horde-adapter-codex` codec) implements this ADR as amended. What W5b measured
while building it, against **codex-cli 0.147.0**:

- **The lost elicitation captures are restored.** Transcripts 17–18
  (`docs/spikes/codex-app-server/transcripts/`, recorded by `drive_mcp.py`
  against the committed `mcp_stub.py` MCP server) preserve the accept and
  decline flows in bytes, including the stub's own wire log proving no byte of
  a declined call ever reaches the MCP server and an accepted call reaches it
  only after the answer.
- **Granular `mcp_elicitations: true` is required, not optional.** Under plain
  `approvalPolicy: "on-request"` no elicitation is routed and the thread parks
  at `status: active, activeFlags: ["waitingOnApproval"]` forever
  (transcript 19). The adapter therefore always starts threads with the
  granular policy the captures verified.
- **`exec_permission_approvals` and `request_permissions_tool` are still
  "under development, false" at 0.147.0** (`codex features list`, 2026-08-13),
  so the shell/exec half of this ADR stands. This discharges the flag
  re-check; a full six-configuration exec re-probe at 0.147.0 remains open.
- **The capability is now worded per call kind**, as this ADR asked:
  `Capability::PreAuthorizeMcpToolCalls` and
  `Capabilities::permission_enforcement_for(ToolCallKind)`. The kind-blind
  answer stays `SandboxAtSpawn` — the floor — and codex deliberately does not
  advertise the wire's blanket `TOOL_CALL_GATING`, so placement cannot land
  high-trust exec on it. A per-kind wire advertisement is left for the
  placement seam that would read it.
- Sandbox tiers are derived from the agent's permission mode by worker-side
  `SandboxPolicy` (unknown or absent modes fail closed to `read-only`;
  `danger-full-access` is unrepresentable), and the chosen tier plus its
  reason is journaled on the agent's first turn.
