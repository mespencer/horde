# Adapters — the provider-harness seam

How Horde talks to a provider harness, what an adapter owes its caller, and how
to test against one without a provider login. The design rationale is
[DESIGN.md](../DESIGN.md) §4–5; this is the working reference for the code.

## The crates

| Crate | What it is |
|---|---|
| `horde-adapter` | The `Adapter`/`AgentSession` trait pair and the domain vocabulary every provider is normalized into — including the version support tables (`version`) every adapter classifies its binary against. No I/O, no provider knowledge. |
| `horde-adapter-claude` | Claude Code's `stream-json` protocol as a pure codec: wire types, turn assembly, usage normalization, the permission prompts on both routes (`can_use_tool` control requests on stdout, and the MCP approve tool), and the measured version range. |
| `horde-adapter-codex` | Codex's `app-server` JSON-RPC protocol as a pure codec: wire types, turn assembly, usage idempotence, every server request Horde adjudicates (MCP elicitations, exec, file-change and permissions approvals, questions), the sandbox-tier policy, the measured version range, and the declared protocol surface the drift check reads. |
| `horde-adapter-fake` | A scripted harness — a real alternate backend, not a mock — for orchestration tests. |
| `horde-testkit` | Golden-transcript loading and the shared conformance suite. |

The live halves — process supervision, stdio, account isolation — live in
`horde-worker` (`claude.rs`, `codex.rs` over `process.rs`), tested against the
`stub-harness` and `codex-stub-harness` binaries so the suite spawns real
processes without a login or a token.

## The seam

An `Adapter` is a factory bound to one provider **and one provider account**. It
advertises `Capabilities` and mints `AgentSession`s, either fresh (`spawn`) or
revived from a previous conversation (`resume`). A session is a live agent: send
messages in with `send`, pull events out with `next_event`, until an
`AgentEvent::TurnEnded` closes the turn.

```rust
let adapter = FakeAdapter::new(Script::echoing());
let mut session = adapter.spawn(&AgentSpec::default()).await?;
session.send(TurnInput::queued("hello")).await?;
while let Some(event) = session.next_event().await? {
    if let AgentEvent::TurnEnded(outcome) = event {
        println!("{:?} in {:?}", outcome.status, outcome.duration);
        break;
    }
}
```

Placement binds an agent to one account for its life, so the factory carries the
binding and the session carries the conversation. Permission answers land on the
session rather than a callback, because the decision may be made minutes later
on someone else's phone.

## Capabilities are data, and the permission layer queries them

Providers differ in ways that change what Horde may safely do with them, so
capability flags are values, not doc comments.

The load-bearing claims are `PreAuthorizeToolCalls` and its scoped siblings
`PreAuthorizeMcpToolCalls`, `PreAuthorizeExecCommands` and
`PreAuthorizeFileChanges`: whether Horde sees a proposed tool call **before**
it runs, and for which kinds. Claude Code has the broad claim (spike W1c,
re-measured on the stdio route at 2.1.323). Codex holds the scoped ones, each
from the first version it was measured on: MCP calls on every supported
version (transcripts 17–18), exec from 0.147 (25/33), file changes from 0.167
(36–37). The worker probes the binary's version before claiming any of them.

Ask `Capabilities::permission_enforcement_for(kind)` rather than reading flags
yourself; the kind-blind `permission_enforcement()` answers with the **floor**
across kinds, so a provider gated only for MCP can never be treated as safe
for high-trust exec:

| Answer | Meaning |
|---|---|
| `PerCall` | Every proposed call of this kind is offered to Horde first; the full chain of DESIGN.md §10 applies. |
| `SandboxAtSpawn` | The harness polices itself against a tier Horde chose at spawn and reports afterwards. Constrainable, not adjudicable — a high-trust assignment does not belong here. |
| `Unenforced` | Tool calls run unobserved; only placement and the workspace limit the blast radius. |

`SteerAtTurnBoundary` is separate from `InterruptibleTurn` because both harnesses
split them (Codex has `turn/steer` alongside `turn/interrupt`).

## Harness versions

Every claim above is a measurement against particular CLI versions, so each
adapter states its measurements as a `SupportTable` and the Worker classifies
the launched binary (`--version`, bounded) before an agent runs on it:

| Provider | Refused | Supported (recorded) | Untested |
|---|---|---|---|
| claude | below 2.1.226 | 2.1.226 – 2.1.323 | newer, or an unreadable banner |
| codex | below 0.147.0 | 0.147.0 – 0.167.0 | newer, or an unreadable banner |

*Refused* stops the Worker at startup with the version, the floor and the
fix, and a spawn refuses with `AdapterError::RefusedVersion` (a refusal, not
a process death). *Untested* runs, with a `harness version warning` journaled
on each agent's first turn — where its failures would land — and Claude's own
`system/init` reports `claude_code_version` per turn as a second opinion.
`horde doctor` prints the same verdict for every local harness, and the
Worker advertises the probed version in `HarnessCapability.version`.

The version also picks behavior: Claude prompts ride stdio from 2.1.323 (the
MCP tool route below it, where only that was measured), and codex's exec and
file-change claims arrive at 0.147 and 0.167 respectively.

## Spawn is not side-effect-free

`Adapter::spawn` makes no purity promise, and one provider actively breaks it:
Codex's `thread/start` writes the working directory into `~/.codex/config.toml`
as a trusted project, which changes how that harness behaves for every later
agent in the same directory. Do not design assumptions that an adapter leaves
the machine as it found it. Containing those effects — sandbox profiles around
the whole process (DESIGN.md §10) — is the Worker's job.

## What an implementation owes its caller

These are enforced by `horde_testkit::conformance`, and they are why a Worker can
trust an adapter it has never seen:

1. **Exactly one `TurnEnded` per message sent.** No message ends silently, and
   none ends twice. An interrupt is a *way for a turn to end*, not a way to skip
   ending it.
2. **`SessionEstablished` precedes any output**, so the identifier that makes an
   agent revivable reaches the journal before anything can be lost.
3. **Usage is a per-turn delta**, never a running total. Harnesses disagree about
   this and normalizing it is the adapter's job — see below.
4. **Capabilities are honest.** Asking for something unclaimed returns
   `AdapterError::Unsupported`; it never silently no-ops.
5. **Turn ids are unique and carried by every event of their turn**, and chunk
   indices start at zero and advance per channel, so a client can detect a gap.

## Usage: per-turn at the seam, however the harness reports it

The seam's contract is per-turn usage. The two harnesses reach it from opposite
directions, and each codec normalizes its own way:

- **Claude** reports `total_cost_usd` and `modelUsage` cumulatively for the
  lifetime of one child process. Spike W1c measured `0.0037 → 0.0063 → 0.0078`
  across three turns: a Worker that summed those would over-bill quadratically.
  `UsageAccumulator` does that subtraction in one audited place. A counter that
  moves backwards means the harness restarted, so it reads the value whole
  rather than saturating to zero.
- **Codex** reports `thread/tokenUsage/updated` with a `last` field that already
  *is* the turn, so it never touches the accumulator.

`TokenUsage::reasoning_output_tokens` is separate from `output_tokens` because
Codex counts it separately and folding it in would lose the distinction
irreversibly.

Headroom is a `Utilization`, not a pair of integers: Codex reports a percentage
with no absolute counts, and Claude reports only whether it is still serving.
Collapsing both into `used`/`limit` would mean inventing numbers. An absolute
limit of zero reads as *unknown*, never as exhausted.

Two Claude-specific traps the codec already absorbs: `modelUsage` is camelCase
where its sibling `usage` is snake_case (getting it wrong silently zeroes every
field), and `modelUsage` can list models you did not request, because auxiliary
work runs on a small model. It is nonetheless the billing truth, since `usage`
counts only the requested model.

## Permissions

`PermissionDecision` is `Allow { updated_input }` or `Deny { message }`. The
`updated_input` is not decoration: Claude Code executes the arguments the
permission tool returns, so the chain can **narrow** a call — clamp a path, drop
a flag — and not merely gate it. An adapter whose harness cannot do that must
report `Capability::RewritesToolInput` as unsupported and reject such a decision,
so a narrowing that failed to apply can never look like one that took effect.

For Claude the prompts arrive **on stdout** (W12c): `--permission-prompt-tool
stdio` makes the harness write a `control_request` with subtype
`can_use_tool` — tool name, input, `tool_use_id`, its own permission
suggestions — and wait for a `control_response` on stdin carrying
`{"behavior":"allow","updatedInput":…}` or `{"behavior":"deny","message":…}`.
It is the exchange the Agent SDK's `canUseTool` callback rides, so a live
session has one inbound channel again. Re-measured at 2.1.323 (transcripts
`12a`–`13`): allow, deny (the message is what the agent reads), a narrowing
`updatedInput` (the harness wrote the narrowed content), a 90-second stall
answered with no timeout, and a session-scoped `updatedPermissions` grant the
harness honoured. Horde deliberately **never sends `updatedPermissions`**:
the chain remembers the exact call, while Claude's grants are tool- or
mode-wide (its own suggestion for a Write is `setMode acceptEdits`), so
mirroring one would let calls the chain never saw run unasked. Every control
request is answered — a subtype Horde does not handle gets an error, a prompt
outside any turn a denial — and an interrupt denies any prompt still parked
before it interrupts. The W1c route — `--permission-prompt-tool
mcp__horde__approve`, prompts as MCP tool calls on the agent's endpoint — is
kept as the fallback for binaries older than 2.1.323, where only it was
measured; it still works at 2.1.323 (transcript `17`). Either way the worker
passes `--settings` with empty allow/deny lists and `--permission-mode
manual`, because ambient allowlists decide a call before the prompt tool ever
sees it (ADR-0003, ADR-0010).

### Questions

Some prompts are questions rather than permissions: Claude's
`AskUserQuestion` and codex's `item/tool/requestUserInput`. They ride the
same path — a `ProposedToolCall` with `questions` (id, header, question,
options, multi-select, free text allowed, secret) — and only a human settles
them: the chain escalates a question past every allow layer and the judge
(authored deny still applies), and the answer comes back as
`PermissionDecision::Answer` (`PermissionDecision.answers` on the wire). The
Server resolves loosely typed answers — a question's id or its 1-based
position, an option's label or its number — into canonical ones before
journaling, refuses an allow without answers on a question and answers on
anything else, and never remembers an answer. Clients answer with `horde
answer <request> 1=2` (or `horde approve --answer`); the TUI inbox answers a
single-question request with the number keys. Claude keys its answers by the
question text and reads one string per question (several choices joined with
`", "`); codex keys them by question id with a list per question.

For Codex the prompts arrive **on stdout** as server requests. At 0.167 five
methods become permission events (W12c, transcripts 34–41): MCP elicitations
(below), exec approvals (`exec`, ADR-0005's 2026-08-19 amendment), file-change
approvals (`apply_patch`, the patch joined from the started `fileChange`
item's `changes`, answered in the exec `decision` vocabulary), sandbox-widening
requests (`request_permissions`, granted exactly what was asked — for the
session when the decision remembers — or an empty profile on refusal), and
questions (`request_user_input`, see above). The last two only reach a client
when codex offers the agent those tools (`request_permissions_tool`,
`default_mode_request_user_input`, both off by default at 0.167). A
`serverRequest/resolved` notification for a request Horde still holds means
codex settled it itself, and the session drops it. Three measured facts shape
the MCP elicitation path (transcripts 17–19):

- The reply's field is `action` (`accept`/`decline`) — an MCP elicitation
  result, not the `decision` shape codex's exec approvals use. There is
  nowhere to carry a deny message (the agent sees the fixed string "user
  rejected MCP tool call") and nowhere to carry replacement arguments, which
  is why codex does not claim `RewritesToolInput`.
- The elicitation names the server but **not the tool**; the
  `item/started [mcpToolCall]` frame just before it carries the tool's name,
  arguments, and the id everything correlates by. The assembler does that
  join.
- Threads must be started with the granular
  `approvalPolicy: {granular: {mcp_elicitations: true, …}}` — under plain
  `"on-request"` nothing is routed and the thread parks at
  `waitingOnApproval` forever. The same finding generalizes: **every** server
  request must be answered, so the live session refuses anything it cannot
  adjudicate (deny-by-default) rather than leaving it pending.

Beneath the chain, Codex's floor is the **sandbox tier chosen at spawn**: a
call the tier permits runs without a request at all, and only escalations
past it are asked about. The worker derives the tier from the agent's
permission-mode name through a configured
`SandboxPolicy` — unknown and absent modes fail closed to `read-only`,
`danger-full-access` is unrepresentable — and journals the choice and its
reason on the agent's first turn as a `HarnessDiagnostic` chunk.

## Testing against an adapter

Use `FakeAdapter` for anything about orchestration. It is a real implementation
of the trait with a scripted backend; there is no expectation DSL and nothing in
it reimplements production logic.

```rust
let adapter = FakeAdapter::new(Script::of(vec![
    ScriptedTurn::saying("working on it").with_step(Step::AskPermission {
        tool_call_id: "call-1".to_owned(),
        tool_name: "Write".to_owned(),
        input: json!({"path": "/tmp/x"}),
    }),
]));

// Drive it under an explicit policy; the turn blocks until the decision lands.
let records = check_session_with(session.as_mut(), &["go"], &|_| {
    PermissionDecision::deny("blocked by policy")
}).await?;
```

`Step::Stall` makes an agent go quiet mid-turn; pause the clock with
`#[tokio::test(start_paused = true)]` so the wait is free. `Step::UseHordeTool`
models the orchestrator verbs of DESIGN.md §5 — a spawn-child intent reaches the
adapter as an ordinary tool call.

`Script::misbehaving(..)` makes the fake break the contract on purpose. That is
how the conformance suite's own checks are tested: five tests assert the suite
*rejects* an adapter that skips `SessionEstablished`, never ends a turn, ends one
twice, repeats a chunk index, or renames its session mid-conversation. A check
that has never been shown to fail is not known to work.

`with_capabilities` narrows what the fake claims, so a test can stand up the
honestly-terminal, non-resumable provider DESIGN.md §5 has to cope with and check
the orchestrator respects the limit instead of assuming every provider behaves
like Claude.

Two things keep the fake from becoming a convenient fiction: it accumulates the
script's per-turn tokens into a running total and differences them (the same path
a real harness forces), and its permission steps genuinely block the turn.

## Golden transcripts

`docs/spikes/transcripts/` holds real recorded `claude` sessions from spike
W1c (2.1.226) and the W12c re-probe (2.1.323, `12a`–`17`: the stdio route,
`AskUserQuestion`, and the baselines re-recorded);
`docs/spikes/codex-app-server/transcripts/` holds the `codex app-server`
sessions from spike W2c, the W5b elicitation captures (17–19), the exec
re-probe (20–33), and the W12c captures at 0.167 (34–41).
`ReplaySession` drives each codec from them, so everything above the transport
runs exactly as it would live — the production adapters swap the line source
for a child process's stdout and keep the rest. Replay is **lazy** and stalls
on an unanswered permission request exactly as a live harness would, which is
what lets the recorded elicitation sessions run under the conformance suite
with real answers.

Adding a claude transcript to its directory automatically enrols it in the
conformance suite; codex recordings are enrolled by the decode-everything test
in `horde-adapter-codex/tests/conformance.rs`.

Recordings can only prove the codec matched the CLI on the day they were taken.
The `live_smoke` tests drive the **real** binaries and are `#[ignore]`d, so
they never run in a normal suite. One command runs them all against the
installed harnesses:

```sh
scripts/live-smoke.sh            # or: scripts/live-smoke.sh claude|codex
```

It runs the codec-level smokes under the operator's own login (for claude:
a turn, a narrowed stdio prompt, an `AskUserQuestion` round trip, the version
verdict), the codex drift check, and — when `HORDE_LIVE_HARNESS_ROOT` names a
signed-in Worker-owned profile — the Worker-level smokes through the
production adapters. Run it when bumping a supported CLI version. If one
fails, capture a fresh transcript rather than loosening the parser to accept
both shapes.

**Codex drift.** `horde-adapter-codex::surface::SURFACE` declares every
method and field path the codec reads or sends; `schema/surface.json` is that
surface resolved against `codex app-server generate-json-schema` at the last
measured version (types and enum values per path). The offline test checks
the snapshot covers the declaration exactly with nothing `MISSING`; the live
test regenerates it from the installed codex and prints every path whose type
or vocabulary moved. `HORDE_UPDATE_CODEX_SURFACE=1` rewrites the snapshot once
the diff is reviewed.

## Revival identity

A session id alone cannot revive an agent. Sessions live *inside* the account
directory — `CLAUDE_CONFIG_DIR` for Claude, `CODEX_HOME` for Codex (which
holds credentials *and* the rollout files threads persist in) — so the
revival identity is the pair (session id, account binding). The Worker
journals both: the session id when the harness names it, and the account
inside the spawn command it keeps whole. Revival rebuilds the spec from that
spawn command, so the account directory is re-derived under the same state
directory the journal lives in — resumed under the directory it was recorded
under by construction. (The `RevivalIdentity::is_resumable_under` guard that
would have compared the two was never called and was deleted in W12c.)

Codex adds one revival trap the codec absorbs: `thread/resume` replays the
prior turn's `thread/tokenUsage/updated` before the new turn's, so usage is
deduplicated per turn or every revival double-bills (transcript 16).

## The Codex MCP endpoint rides `-c`, not `config.toml`

An agent's Horde MCP endpoint reaches codex as per-process
`-c mcp_servers.horde.…` overrides on the `app-server` command line.
Deliberately never a `config.toml` write: `CODEX_HOME` is shared by every
agent on the same Provider Account, and a config write from one spawn would
race every concurrent one — on top of codex already writing to that file
outside Horde's control (project trust, ADR-0005).

The Worker journals a third fact beside those two: **how many turns the
conversation has taken.** Turn ids are `{session}#{ordinal}` and journal
idempotency keys derive from them, so a revived session whose assembler
restarted at zero would re-mint keys the Server has already seen — and the
Server's dedupe would then discard the revived turns' events *while acking
them*. `AgentSpec::with_turns_taken` (the `turns-taken` provider option)
carries the count into `Adapter::resume`; every resumable adapter numbers its
next turn after it. The Worker passes one **past** the recorded count, because
a turn that died half-journaled has already spent its ordinal; turn ids need
uniqueness, not density, so the skipped ordinal costs nothing.

Usage needs no such reconciliation: a `--resume`d process reports cumulative
usage for *its own lifetime* (transcript `05-resume.jsonl` — the resumed turn
reports 51 output tokens, not the prior process's 713), so a fresh
`UsageAccumulator` per incarnation counts each turn exactly once.

## Steering and interrupt at the Worker

Steering is a delivery mode, not a message kind (DESIGN.md §11). The Worker's
pump holds a turn-boundary message that arrives mid-turn and sends it when the
turn ends — orchestrator-side queueing is the one mechanism that works against
every provider, which is why there is no boundary-steering capability to ask
for. An interrupt-mode message never waits: `AgentSession::send` aborts the
turn in flight first (for Claude, the control message — the only abort that
leaves the session usable), and the aborted turn still closes with its own
`TurnEnded(Interrupted)` ahead of the new turn's events.

## Failure taxonomy

`AgentLifecycleChanged.reason` is free text, so the Worker classifies every
agent failure under a stable prefix (`horde_worker::translate`): **process
death** (retry or revive here), **refusal** (rebind to another account or
provider), **malformed stream** (the protocol moved; fix the adapter). A live
Claude session reports EOF as the death it always is — reaping the child for
its exit status and keeping stderr — because the Worker never closes a live
agent's stdin.

## Known gaps

- The Worker-level Claude live smoke needs a signed-in Worker-owned profile
  (`HORDE_LIVE_HARNESS_ROOT`); the codec-level smokes run under the
  operator's login without one.
- Codex's `request_permissions` and `request_user_input` tools are off by
  default at 0.167, so those requests are handled when codex sends them but
  Horde does not turn the feature flags on.
- A codex question refused by the chain is answered with empty answers (the
  schema has no decline); how codex presents that to the model was not
  probed.
- The TUI answers only single-question requests with the number keys; a
  request asking several questions points at `horde answer` (the W12f TUI
  rebuild owns the full flow).

The AmendActiveTurn question that was left to the Codex productionization is
now answered inside the adapter rather than the pump: a `LiveCodexSession`
that receives a turn-boundary send while a turn is active steers it
(`turn/steer`, falling back to `turn/start` when the steer loses its race),
so the pump's boundary queueing remains the provider-agnostic default and
codex simply does something better with the same call.
