# Per-session append-only event log as the resilience mechanism

"Resilient to spotty networks" is a state problem, not a transport problem: gRPC streams don't survive reconnects, and no transport makes attach, replay, crash recovery, and usage attribution fall out for free. We therefore make an append-only, sequence-numbered event log per session (SQLite on the server) the source of truth; all client and worker streams are "sync me from offset N," and partitioned workers journal locally and reconcile on reconnect. NATS/JetStream was considered and rejected for v1 as an infra dependency whose benefits only matter at multi-user scale.

## Consequences

- Reconnect, TUI scrollback, attach-to-any-agent, crash recovery, and tree-level usage attribution are one mechanism, not five features.
- Everything that matters must be an event (permission requests, spawns, usage, instantiation progress) — side channels are bugs.
