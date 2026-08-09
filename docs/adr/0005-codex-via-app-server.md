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

## Consequences

- Codex agents get a **coarser permission story than Claude agents**. Horde's
  deterministic-deny / deterministic-allow / judge / human chain (§10) cannot
  gate a Codex tool call; the enforcement that actually applies is Codex's own
  sandbox tier, chosen by Horde at `thread/start`.
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
