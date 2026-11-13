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
work and need no reconstruction. *(The usage half of that sentence — "`last`
per turn, nothing to diff" — was wrong; see the 2026-10-30 amendment at the
end of this ADR.)*

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

## Amendment, 2026-08-19: exec approvals route on current codex; the exception is now scoped to file changes

The outstanding debt is paid. The exec/shell re-probe (`drive_approvals.py`,
transcripts 20–33) ran the configuration matrix against codex-cli **0.148.0**
and **0.147.0** under fresh per-run `CODEX_HOME`s and throwaway cwds, every
turn bounded. **For shell/exec commands, the "cannot intercept before they run"
finding is superseded on any codex this project supports.** What was measured:

- `item/commandExecution/requestApproval` reaches the client **before** the
  command executes: at 0.147.0 under the granular approval policy this adapter
  always sends (transcript 33; plain `on-request` still parks there,
  transcript 32), and at 0.148.0 under either policy (20–22, 25–27). The
  request carries `reason`, the raw `command`, `cwd`, codex's parse in
  `commandActions`, a `proposedExecpolicyAmendment` argv, and an advisory
  `availableDecisions` list — advisory, because transcript 25's list omits
  `decline` and declining still blocked the command.
- The decision vocabulary is honored end to end: `decline` completes the exec
  item as `declined` with no exit code (20–22, 25–27), `accept` runs it
  (exit 0, transcript 29), `acceptWithExecpolicyAmendment` echoing codex's own
  argv runs it **and** persists a rule a second turn no longer asked about
  (transcript 30), and `acceptForSession` is the same vocabulary's
  session-scoped grant. `cancel` withdraws the question.
- **`approvalsReviewer: "auto_review"` is a security hazard, not a feature.**
  Under it codex's own review subagent approved the escalation and the write
  RAN under a read-only sandbox with no client-facing request at all
  (transcript 23) — the entire §10 chain bypassed by codex reviewing itself.
  `user` is the measured default when unset (transcript 22), and the client
  name is not a gate (20 vs 21).
- **File-change approvals never route.** An `apply_patch` under a tier that
  blocks it emits `item/started [fileChange]` with the full diff and then
  parks at `waitingOnApproval` forever, under `on-request` and granular alike
  (transcripts 28, 31). The park is stable — the thread idles rather than
  corrupts — and `turn/interrupt` remains the recovery.
- A correction the re-probe forced on the original spike: transcript 06's own
  bytes (0.145.0, granular) contain one exec approval, routed 182 seconds
  after the exec item started, which W2c's analysis wrote off as "no request".
  The zero-of-six claim above was an analysis error for that one
  configuration; the conclusion it fed — do not build on 0.145's routing —
  still stands, because a three-minute internal retry loop before the
  question is not interception worth advertising.

What is built on this (W9c):

- The adapter surfaces an exec approval as a `ProposedToolCall` under the
  fixed tool name **`exec`**, carrying `command` (codex's parse of the inner
  command when unambiguous, else the raw invocation), `raw_command` (when the
  two differ), `cwd`, and `reason` — so §10's deterministic rules match
  command prefixes naturally, and the same chain that gates Claude's calls
  gates these: deterministic deny/allow, the judge seam, human escalation via
  the inbox. A chain-held approval holds the turn, exactly as with Claude;
  codex tolerates the parked request indefinitely (the transcript 25/28
  shape), and an interrupt answers anything still parked with `cancel` before
  `turn/interrupt` — an unanswered request would park the thread even as the
  turn died.
- The decision mapping, journaled per decision with the encoding used:
  chain DENY → `decline`; ALLOW → `accept`; ALLOW with remember-for-session →
  `acceptForSession` (never the amendment, which would outlive the session);
  ALLOW whose narrowing restates the proposal verbatim →
  `acceptWithExecpolicyAmendment` echoing codex's proposed argv when one was
  offered, plain `accept` otherwise; a narrowing that actually rewrites
  arguments is refused (`RewritesToolInput` stays unclaimed), never downgraded
  to running the un-narrowed original.
- The reviewer is pinned: the codec sends `approvalsReviewer: "user"` as a
  constant no configuration reaches, and the worker's config parse refuses
  `auto_review`/`guardian` by name, citing transcript 23.
- The capability is **version-gated per call kind**. The worker probes the
  launched binary (`codex --version`) at adapter construction;
  `PreAuthorizeExecCommands` is claimed only at 0.147+, and
  `permission_enforcement_for(ExecCommand)` answers `PerCall` only then.
  `ToolCallKind` now separates `ExecCommand` from `FileChange`: file changes
  keep `SandboxAtSpawn` on every version, so the kind-blind floor — and the
  wire advertisement, which still carries no blanket `TOOL_CALL_GATING` —
  are unchanged. The per-kind claims graduate to the wire when placement
  learns to match per call kind, not before.

The consequences above are re-scoped accordingly: "Codex agents get a coarser
permission story" is now true only of **file changes** on 0.147+, and of
everything on older CLIs. The sandbox tier and `cwd` remain the enforcement
floor beneath the chain either way, and the tier-choice journaling stands.

## Amendment, 2026-10-30: usage readings are per model call, not per turn

The status paragraph's claim that `thread/tokenUsage/updated` "reports `last`
per turn, so … there is nothing to diff" was an artefact of the first
recordings: transcripts 03 and 14 are one-call turns, so their single reading
looked like the turn. Codex sends **one reading per model call**. `last` is
that call; `total` is the thread's running sum. Transcript 33 (0.147.0) has six
readings in one turn — `last.totalTokens` 18 411, 18 683, 18 942, 19 153,
19 311, 19 395, summing exactly to the final `total` of 113 895 — and every
recording with a tool call has two or more (04, 08, 09, 15, 17, 18, 20–27, 29,
30). The adapter as built kept the first reading of each turn and so billed
18 411 for transcript 33's turn: an undercount of 2–6× on exactly the turns
that do work.

What the codec does now: it differences the monotonic thread `total` through
the same `UsageAccumulator` the Claude adapter uses, adding each reading's
delta to the open turn. The resume trap needs no separate dedupe under this
shape: the reading `thread/resume` replays (transcript 16, the previous turn's
id and `total`) seeds the baseline and bills nothing, so the new turn's first
reading counts only its own tokens. A reading for any turn other than the open
one still advances the baseline, so it is never billed later either. A reading
missing `total` (shape drift) falls back to its own `last`, which is one call by
definition. Conformance tests pin the recorded totals of the multi-reading
transcripts.

## Amendment, 2026-11-13 (W12c): file changes route at 0.167; every prompt type is surfaced; versions are classified

Re-probed against codex-cli **0.167.0** on Linux (`drive_approvals.py` runs
34–41, recorded under a fresh `CODEX_HOME` holding only a copy of
`auth.json`). The transport decision stands.

- **Exec approvals: unchanged.** `item/commandExecution/requestApproval`
  routes before the command runs under granular and `on-request` alike (34,
  35, 40); decline blocks it, accept runs it. The request gained a `kind:
  "command"` field. Every notification gained `emittedAtMs`.
- **File-change approvals now route.** The park transcripts 28 and 31
  recorded at 0.148 is gone: `item/fileChange/requestApproval` arrives after
  `item/started [fileChange]` (which carries the patch in `changes`) and
  before the patch applies; decline completes the item as `declined`, accept
  applies it (36, 37). The request names only the item, so the assembler
  joins the patch from the started item. `Capability::PreAuthorizeFileChanges`
  is claimed from 0.167 — the first measured version, not a guessed earlier
  one — and with all three per-kind claims the kind-blind floor of a probed
  0.167 binary is now `PerCall`. The wire still carries no blanket
  `TOOL_CALL_GATING` for codex; per-kind claims graduate when placement
  matches per kind.
- **Sandbox-widening requests and questions route behind their feature
  flags.** With `request_permissions_tool` enabled,
  `item/permissions/requestApproval` asks for a profile (`{"network":
  {"enabled":true}}`) and granting exactly that let the command run (39);
  with `default_mode_request_user_input`, `item/tool/requestUserInput` asks
  questions with options and takes `{"answers":{id:{"answers":[…]}}}` (38).
  Both flags are still "under development, false" by default, as are
  `exec_permission_approvals`; Horde handles the requests when they come and
  does not enable the flags. `thread/start` now sends `request_permissions:
  true` in the granular policy so such requests reach Horde rather than
  codex's default refusal.
- **`serverRequest/resolved`** follows every answered request; one for a
  request Horde still holds means codex settled it itself, and the session
  drops it.
- **A probe-driver bug, not drift.** The 20–33 driver's `select()` +
  `readline()` reader left buffered frames unread until the next write, which
  at first read as turns hanging for three minutes (`emittedAtMs` showed the
  frames were sent on time); it also broke out of each turn at the idle
  status one frame before `turn/completed`. Both are fixed; 34–41 end with
  their terminal frames.

What is built on this: all five request kinds become permission events
(`exec`, `apply_patch`, `request_permissions`, `request_user_input`, and the
MCP `server/tool`), each answered in its own vocabulary, with a decision a
vocabulary cannot carry declined and journaled rather than downgraded.
Questions reach clients as questions (`ProposedToolCall.questions`) and are
answered with `PermissionDecision.answers`.

**Versions are classified.** Below 0.147.0 is refused (without exec routing
the chain cannot gate the agent's shell); 0.147.0–0.167.0 is supported;
newer is untested, journaled per agent and reported by `horde doctor`.
**Drift is diffable as this ADR promised:** the codec's declared protocol
surface is resolved against `generate-json-schema` into a committed snapshot
(`crates/horde-adapter-codex/schema/surface.json`), with an offline coverage
test and a live regenerate-and-diff test.
