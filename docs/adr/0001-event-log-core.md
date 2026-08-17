# Per-session append-only event log as the resilience mechanism

"Resilient to spotty networks" is a state problem, not a transport problem: gRPC streams don't survive reconnects, and no transport makes attach, replay, crash recovery, and usage attribution fall out for free. We therefore make an append-only, sequence-numbered event log per session (SQLite on the server) the source of truth; all client and worker streams are "sync me from offset N," and partitioned workers journal locally and reconcile on reconnect. NATS/JetStream was considered and rejected for v1 as an infra dependency whose benefits only matter at multi-user scale.

## Consequences

- Reconnect, TUI scrollback, attach-to-any-agent, crash recovery, and tree-level usage attribution are one mechanism, not five features.
- Everything that matters must be an event (permission requests, spawns, usage, instantiation progress) — side channels are bugs.

## Scope of "side channels are bugs" (amended wave 7)

The rule governs **session facts**: anything a client, an auditor, or a
reconnecting component could need must reach the session log as an event. It is
not a ban on all non-log state — the server legitimately keeps non-durable,
non-session state beside the log (worker liveness, instance caches, CA key
material; the issuance and revocation *records* are still events).

The worker's journal database holds three **mutable** tables beside its
append-only journal — `applied_command` (the at-least-once dedupe watermark),
`agent` (revival identity: spawn command, provider session id, turn count), and
`meta` (sequence counters, journal identity). These are inside this ADR's scope
— they are load-bearing parts of the resilience mechanism — and they are ruled
**permitted, not side channels**, on two grounds:

1. **They carry no session fact around the log.** Each is worker-scoped
   operational state: a watermark over the command stream, counters, and the
   revival identity — which is worker-private *by design*, because the worker,
   not the server, revives agents. Nothing in them is a fact some other
   component would need to replay or audit and cannot get from the log.
2. **They cannot drift from the log.** Each mutation commits in the same
   transaction as the journal append or command application it accompanies;
   there is no ordering in which the table and the journal disagree after a
   crash.

The criterion for the next mutable table is the same two tests: a table that
holds a session fact the log lacks, or that can commit apart from the events it
describes, is a side channel and a bug.
