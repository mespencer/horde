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

| Wave | Item | Depends on |
|---|---|---|
| 1 | **W1a** Workspace scaffold + gauntlet (all gates blocking, self-passing) | — |
| 1 | **W1b** Proto contracts (buf + tonic-prost `horde-proto`) | — |
| 1 | **W1c** Spike: drive `claude` via stream-json (ADR-0003; golden transcripts; verdict incl. fallback to Agent SDK/ACP) | — |
| 2 | Event log + server core (SQLite, sessions/agents/events, sync-from-offset) | W1a, W1b |
| 2 | Adapter trait + FakeAdapter + `horde-testkit` | W1a, W1c verdict |
| 2 | Spike: Codex CLI for GPT-subscription agents (prepare for all outcomes, like W1c) | W1a |
| 3 | Worker core (process supervision, journaling, worker↔server stream) | wave 2 |
| 3 | Agent MCP endpoint (spawn/assign/note/result, ancestry, payload auto-detect) | wave 2 |
| 3 | Client CLI verbs (new/attach headless, sessions, approve) | wave 2 |
| 4 | Scheduler/placement + workspaces (definitions/instances, checkout policies) | wave 3 |
| 4 | Git relay + share resolution (same-clone / same-worker / cross-worker) | wave 3 |
| 4 | Permission chain (deny → allow → judge → human; routing incl. tree path) | wave 3 |
| 4 | TUI (session view, attach, inbox; insta snapshots) | wave 3 |
| 5 | Claude adapter productionized (from W1c) + revival/steering/interrupt | wave 3–4 |
| 5 | Codex/GPT adapter productionized (from wave-2 spike) | wave 3–4 |
| 5 | Enrollment + mTLS everywhere | wave 4 |
| 6 | MCP hub + `local` servers; blob store; usage/tiers/headroom; OTLP | wave 5 |
| 7 | Fault-injection suite; Gherkin features; full e2e hardening; DESIGN.md conformance audit | all |

Wave contents will be refined as reality reports back; the dependency spine holds.

## Status

Tracked in the session task list (orchestrator-owned); this file records the plan, not live status.
