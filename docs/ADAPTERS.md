# Adapters — the provider-harness seam

How Horde talks to a provider harness, what an adapter owes its caller, and how
to test against one without a provider login. The design rationale is
[DESIGN.md](../DESIGN.md) §4–5; this is the working reference for the code.

## The crates

| Crate | What it is |
|---|---|
| `horde-adapter` | The `Adapter`/`AgentSession` trait pair and the domain vocabulary every provider is normalized into. No I/O, no provider knowledge. |
| `horde-adapter-claude` | Claude Code's `stream-json` protocol as a pure codec: wire types, turn assembly, usage normalization, and the `--permission-prompt-tool` payloads. |
| `horde-adapter-fake` | A scripted harness — a real alternate backend, not a mock — for orchestration tests. |
| `horde-testkit` | Golden-transcript loading and the shared conformance suite. |

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

The load-bearing one is `PreAuthorizeToolCalls`: whether Horde sees a proposed
tool call **before** it runs. Claude Code has it (spike W1c). Codex 0.145.0 does
**not** — spike W2c found that across six configurations no client-facing
approval request ever arrived, because the two features that would route them
are under development and off. Codex adjudicates against its own sandbox tiers
and tells the client afterwards.

Ask `Capabilities::permission_enforcement()` rather than reading flags yourself:

| Answer | Meaning |
|---|---|
| `PerCall` | Every proposed call is offered to Horde first; the full chain of DESIGN.md §10 applies. |
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

`docs/spikes/transcripts/` holds real recorded `claude` sessions from spike W1c.
`ReplaySession` drives the Claude codec from them, so everything above the
transport runs exactly as it would live — the production adapter swaps the line
source for a child process's stdout and keeps the rest.

Adding a transcript to that directory automatically enrols it in the conformance
suite; nothing needs listing by hand.

Recordings can only prove the codec matched the CLI on the day they were taken.
`tests/live_smoke.rs` drives the **real** binary and is `#[ignore]`d, so it never
runs in a normal suite:

```sh
cargo test -p horde-adapter-claude --test live_smoke -- --ignored --nocapture
```

Run it when bumping the supported CLI version. If it fails, capture a fresh
transcript rather than loosening the parser to accept both shapes.

## Revival identity

A session id alone cannot revive a Claude agent. Sessions live *inside*
`CLAUDE_CONFIG_DIR`, so `RevivalIdentity` pairs the id with the account
directory it was recorded under, and `is_resumable_under` refuses a mismatch
instead of surfacing an authentication error that looks like an outage. Journal
both halves together.

## Not here yet

Process supervision — spawning the binary, wiring stdio, isolating
`CLAUDE_CONFIG_DIR` per account, and merging stdout with the MCP endpoint's
permission requests — belongs to the Worker (wave 3) and the productionized
Claude adapter (wave 5). Keeping the protocol layer pure is what lets the
conformance suite replay recordings and mean something.
