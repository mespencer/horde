# Adapters — the provider-harness seam

How Horde talks to a provider harness, what an adapter owes its caller, and how
to test against one without a provider login. The design rationale is
[DESIGN.md](../DESIGN.md) §4–5; this is the working reference for the code.

## The crates

| Crate | What it is |
|---|---|
| `horde-adapter` | The `Adapter`/`AgentSession` trait pair and the domain vocabulary every provider is normalized into. No I/O, no provider knowledge. |
| `horde-adapter-claude` | Claude Code's `stream-json` protocol as a pure codec: wire types, turn assembly, usage normalization, and the `--permission-prompt-tool` payloads. |
| `horde-adapter-codex` | Codex's `app-server` JSON-RPC protocol as a pure codec: wire types, turn assembly, usage idempotence, the MCP-elicitation permission seam, and the sandbox-tier policy. |
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

The load-bearing pair is `PreAuthorizeToolCalls` and its scoped sibling
`PreAuthorizeMcpToolCalls`: whether Horde sees a proposed tool call **before**
it runs, and for which kinds. Claude Code has the broad claim (spike W1c).
Codex has the scoped one — since the ADR-0005 amendment (measured again for
W5b, transcripts 17–18), every **MCP** tool call is routed through a
client-facing elicitation before a byte reaches the MCP server, while
**shell/exec** remains adjudicated internally against the sandbox tier: the
two routing feature flags are still off at 0.147.0.

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

For Claude the prompts arrive on the **MCP endpoint, not on stdout**, so a live
session merges two inbound channels. Both stdio and HTTP MCP servers work, and
the harness will wait — a 180-second stall was answered with no timeout, which is
what makes the human-approval layer viable.

For Codex the prompts arrive **on stdout**, as `mcpServer/elicitation/request`
server requests, and only for MCP tool calls. Three measured facts shape the
adapter (transcripts 17–19):

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

For exec, Codex's enforcement is the **sandbox tier chosen at spawn**. The
worker derives it from the agent's permission-mode name through a configured
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
W1c; `docs/spikes/codex-app-server/transcripts/` holds the `codex app-server`
sessions from spike W2c plus the W5b elicitation captures (17–19).
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
they never run in a normal suite:

```sh
cargo test -p horde-adapter-claude --test live_smoke -- --ignored --nocapture
cargo test -p horde-adapter-codex  --test live_smoke -- --ignored --nocapture
cargo test -p horde-worker         --test live_smoke -- --ignored --nocapture
```

Run them when bumping a supported CLI version. If one fails, capture a fresh
transcript rather than loosening the parser to accept both shapes.

## Revival identity

A session id alone cannot revive an agent. Sessions live *inside* the account
directory — `CLAUDE_CONFIG_DIR` for Claude, `CODEX_HOME` for Codex (which
holds credentials *and* the rollout files threads persist in) — so
`RevivalIdentity` pairs the id with the directory it was recorded under, and
`is_resumable_under` refuses a mismatch instead of surfacing an authentication
error that looks like an outage. Journal both halves together.

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

## Not here yet

Merging the Claude adapter's stdout with the MCP endpoint's permission
requests — the `--permission-prompt-tool` seam exists on `ClaudeAdapter`, and
wiring it to a live endpoint is the endpoint's increment. The Codex adapter
needs no such merge: its permission requests arrive on stdout.
