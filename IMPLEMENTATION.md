# Horde — Implementation Plan

Working agreement for building Horde to the end state in [DESIGN.md](./DESIGN.md), with quality per [docs/TESTING.md](./docs/TESTING.md). The orchestrator plans, delegates, merges, and audits — it writes no implementation code. Teammates implement in worktrees.

## Ground rules (non-negotiable)

- **The gauntlet is the only enforcement mechanism** (local-only development: no PRs, no hosted CI). It must pass on `main` after every merge. All gates are **blocking from day one** — including mutants (no missed mutants) and CRAP (≤ 15) — because we start from zero and never grant grandfather exemptions.
- **No regressions on main, ever.** A merge that reds the gauntlet is reverted or fixed forward immediately; nothing else lands meanwhile.
- Rust rules: `unwrap()` never (including tests); `expect()` only in tests/benchmarks; clippy deny-list per docs/TESTING.md §4.
- Tests exercise production code — no test-side reimplementation, no mocking frameworks. Fakes are real alternate backends.
- Commit early and often on feature branches; branches are `mespencer/<slug>`; merges to main are real merges (no squash).
- Worktrees live in `.claude/worktrees/<slug>` (gitignored).

## Verification protocol

1. **Teammate, before handoff:** run the full gauntlet (and any suite relevant to the change) in the worktree; the final report must include the *actual command output* (gauntlet summary table, test counts), not a claim. A report that says "tests pass" without evidence is an incomplete report. Failures may not be explained away — a red instrument is either fixed or escalated as red.
2. **Orchestrator, at merge:** audit the report against the evidence, merge to main, re-run the gauntlet on main, and only then close the task. Semantic conflicts between parallel branches are caught here.

## Waves

Dependencies flow downward; items within a wave run in parallel worktrees.

| Wave | Item | Depends on | Status |
|---|---|---|---|
| 1 | **W1a** Workspace scaffold + gauntlet (all gates blocking, self-passing) | — | delivered |
| 1 | **W1b** Proto contracts (buf + tonic-prost `horde-proto`) | — | delivered |
| 1 | **W1c** Spike: drive `claude` via stream-json (ADR-0003; golden transcripts; verdict incl. fallback to Agent SDK/ACP) | — | delivered |
| 2 | Event log + server core (SQLite, sessions/agents/events, sync-from-offset) | W1a, W1b | delivered |
| 2 | Adapter trait + FakeAdapter + `horde-testkit` | W1a, W1c verdict | delivered |
| 2 | Spike: Codex CLI for GPT-subscription agents (prepare for all outcomes, like W1c) | W1a | delivered |
| 3 | Worker core (process supervision, journaling, worker↔server stream) | wave 2 | delivered |
| 3 | Agent MCP endpoint (spawn/assign/note/result, ancestry, payload auto-detect) | wave 2 | delivered |
| 3 | Client CLI verbs (new/attach headless, sessions, approve) | wave 2 | delivered |
| 4 | Scheduler/placement + workspaces (definitions/instances, checkout policies) | wave 3 | delivered |
| 4 | Git relay + share resolution (same-clone / same-worker / cross-worker) | wave 3 | delivered |
| 4 | Permission chain (deny → allow → judge → human; routing incl. tree path) | wave 3 | delivered |
| 4 | TUI (session view, attach, inbox; insta snapshots) | wave 3 | delivered |
| 5 | Claude adapter productionized (from W1c) + revival/steering/interrupt | wave 3–4 | delivered |
| 5 | Codex/GPT adapter productionized (from wave-2 spike) | wave 3–4 | delivered |
| 5 | Enrollment + mTLS everywhere | wave 4 | delivered |
| 6 | MCP hub + `local` servers; blob store; usage/tiers/headroom; OTLP | wave 5 | delivered |
| 7 | Fault-injection suite; Gherkin features; full e2e hardening; DESIGN.md conformance audit | all | delivered (audit fixes in flight) |
| 8 | Port forwards: expose/inject relayed through the server (ADR-0006) | wave 7 | delivered |
| 9 | **W9a** Interactive-auth relay: `horde worker login` remote PTY + ephemeral callback expose (DESIGN §4) | wave 8 | delivered |
| 10 | Session front door (ADR-0008, integrated at `c7b3c13`) and joining UX (ADR-0007: one `horde://join` token, `horde up`) | wave 9 | delivered |
| 11 | **W11a** Trust boundary: role-gated RPCs, worker identity from its certificate, origin-aware ingest and dead letters, narrowing capability gate (ADR-0009) — merged at `d3e6a4e` | wave 10 | delivered |
| 11 | **W11b** Harness isolation: worker-owned `CLAUDE_CONFIG_DIR`/`CODEX_HOME` profile, secrets off argv, process groups, path/git hardening (ADR-0010) — merged at `d38cff3` | wave 10 | delivered |
| 11 | **W11c** Adapters: codex usage by thread-total difference, single MCP gating, `wait_for_agent` redialing cursor (ADR-0005 amendment) — merged at `d562762` | wave 10 | delivered |
| 11 | **W11d** Delivery and liveness: worker lease and LOST agents, boot reconcile, buffered messages, terminal archive, position resume, `horde worker reset` (ADR-0011) — merged at `38b3c38` | W11a–c | delivered |
| 11 | **W11e** Worker robustness: journaled worker inbox, per-agent lanes, commands fail instead of the Worker, per-account login (ADR-0012) — merged at `5d7c193` | W11a–c | delivered |
| 11 | **W11f** Joining and identity: state under `~/.horde`, required server mode, state locks, guarded re-join, hardened `horde up`, canonical `horde/<tool>` names (ADR-0007, ADR-0004 amendments) — merged at `ca88af7` | W11a–c | delivered |
| 11 | **W11g** Server read path: in-memory session projection over a WAL read pool, one-transaction batch ingest, name filter (ADR-0013) — merged at `b3adf29` | W11d–f | delivered |
| 11 | **W11h** CLI/TUI usability: `horde doctor`, `agent status`, `workspace update`, `--remint`, JSON refusals, TUI fixes (ADR-0008 amendment) — merged at `47e2612`; testkit reaper fix at `22f2599` | W11g | delivered |
| 11 | **W11i** Docs refresh: CONFORMANCE, DESIGN/CONTEXT drift, README | W11h | delivered |
| 12 | W12a gamma mutation gate, W12b Docker multi-node harness, W12c harness modernization, W12d workspaces (ADR-0014), W12e config + permissions, W12f TUI rebuild, W12g Windows, W12h logs, W12i ssh bootstrap, W12j cleanup | wave 11 | in progress |

**Wave-11 incident — the testkit reaper.** Under cargo-mutants, a mutant that flipped `ppid == parent` in the testkit's descendant hunt made a test SIGKILL every process on the machine. The fix (`22f2599`) signals only process groups the testkit spawned, routes every signal through one guard (`crates/horde-server-testkit/src/reap.rs`) that refuses pid 0, 1, its own process, group, session and parent, and moves the harmless process-table parsing to `ps.rs`. **Rule:** code that sends real signals to targets chosen at runtime is excluded from mutation, because a mutant of it runs for real. `reap.rs` is the only entry under that criterion in `.cargo/mutants.toml`; every other exclusion there is a test fixture or a mutation equivalent to the original.

Wave contents will be refined as reality reports back; the dependency spine holds.

## Status

Tracked in the session task list (orchestrator-owned); this file records the plan and which waves have been delivered, not live task status.
