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

## Usage: diff, never sum

`UsageAccumulator` turns a harness's running totals into per-turn deltas.

Claude Code reports `total_cost_usd` and `modelUsage` cumulatively for the
lifetime of one child process. Spike W1c measured `0.0037 → 0.0063 → 0.0078`
across three turns of one process: a Worker that summed those would over-bill
quadratically. A counter that moves backwards means the harness restarted, so
the accumulator reads it whole rather than saturating to zero.

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

## Not here yet

Process supervision — spawning the binary, wiring stdio, isolating
`CLAUDE_CONFIG_DIR` per account, and merging stdout with the MCP endpoint's
permission requests — belongs to the Worker (wave 3) and the productionized
Claude adapter (wave 5). Keeping the protocol layer pure is what lets the
conformance suite replay recordings and mean something.
