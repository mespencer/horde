# Horde — Development Quality Suite

The goal: **agent-written code is trusted through the gates it passed, not by being read line by line.** Every instrument below exists to catch a specific way an agent (or a human) can produce code that looks right and isn't. Inspired by the `aii` gauntlet; adapted, not copied.

This defines the end state; the runner and blocking gates land with the first crate, measurement gates enter through the rollout ladder (§4).

## 1. Principles (instrument integrity)

Rules about the instruments themselves, each one paid for somewhere by a green result that wasn't earned:

- **A gate that could not run did not pass.** Tool errors (`Error`) fail the job for *every* gate, advisory or not; `Fail` is a verdict about code and only blocks for blocking gates. An unmeasured gate is a broken gate.
- **`skipped` is distinct from `pass`.** A zero must state which of its two meanings it has ("nothing to measure" vs "measured clean").
- **Missing rows are rendered, not dropped.** A gate silently vanishing is how a quality layer dies without anyone noticing.
- **Never silently skip on missing infrastructure.** If a test needs a container/binary and can't get it, the test *fails*. No degrade-to-green.
- **Tests exercise production code.** No test-side reimplementation of production logic; no mocking frameworks (mockall/wiremock are banned by convention). Fakes are *real alternate backends* (§3), mirrored against their production counterparts.
- **Positive controls run both ways.** A test proves it can go red: removing the behavior reds it, and a spurious trigger reds it too. Check counts, not just exit codes.
- **A mutation that stays green is a finding, not a relief.**
- **Gate definitions live in Rust, not YAML.** Tool, flags, exclusions, thresholds, and enforcement level are code in the gauntlet crate; CI and laptop run the same binary. If a flag wants to live in workflow YAML, it belongs in the gate. Tool versions are pinned (an upgrade silently moves a calibration baseline).
- **Tests that cannot yet reach a defect ship labeled, not deleted.**

## 2. Test pyramid

| Layer | Tooling | What it covers |
|---|---|---|
| Unit | inline `#[cfg(test)]`, cargo-nextest | Pure logic: scheduling decisions, permission-chain evaluation, config precedence resolution, tier selection strategies |
| Property | **proptest** (used, not just declared) | Invariants under generated inputs: event-log ordering/resume (sync-from-any-offset ≡ full replay), reconciliation idempotence, capability attenuation (child ⊆ parent always), precedence-chain resolution |
| Integration | `tests/*.rs` per crate, shared `horde-testkit` | Real components, transport collapsed to in-process where possible: server + SQLite, relay against real git repos, worker supervising a FakeAdapter |
| Multi-process e2e | `horde-testkit` process harness | Real `horde-server` + real `horde-worker`(s) + CLI client over real localhost gRPC/mTLS: spawn trees, assign/result flows, attach/replay, enrollment |
| Fault injection | testkit network proxy (toxiproxy-style, in-repo) | The spotty-network requirement is *tested*, not aspired to: sever/delay/flap worker↔server mid-turn, assert journaling + reconciliation, permission-block behavior |
| Behavioral (Gherkin) | **cucumber-rs** (pinned `=0.23.0`), workspace-root `features/*.feature`, one `features` test binary in `horde-cli` (W6e's one-binary rule) | Cross-component scenarios in domain language (CONTEXT.md terms): "Given a worker partitioned mid-assignment… the result arrives after reconnect." Living spec for the orchestration semantics; small, curated, not a parallel test universe. Steps drive the real `horde` binary, the client library, a real worker on the FakeAdapter, and the secured enrollment path; `harness = false`, so step code runs under the full lint deny-list — no `expect` anywhere in it |
| Adapter conformance | golden transcripts | Recorded real harness sessions (claude stream-json, ACP) replayed against adapters; the same suite runs against the FakeAdapter to prove the fake stays honest |
| TUI | **insta** snapshots over ratatui `TestBackend` | Every screen/zoom-level renders against fixed session fixtures; snapshot review is the UI diff |
| Proto contract | `buf lint` + `buf breaking` | Wire compatibility against the last released version — this *is* the version-skew guarantee (DESIGN §2) |
| Doc tests | rustdoc | Public APIs of library crates carry runnable examples |

## 3. The FakeAdapter (load-bearing)

Orchestration logic must be testable without burning tokens or needing provider logins. The **FakeAdapter** is a scripted provider harness — a real implementation of the adapter trait speaking the real wire protocol, driven by test-authored scripts ("emit these turns, request this permission, spawn a child, stall here"). It is a production-quality alternate backend in the aii `mem`-store sense: same trait, same observable behavior, mirrored against the adapter conformance suite so it can't drift from what real harnesses do. Every e2e and fault-injection test runs on it by default; a small tagged suite runs against real harnesses on demand.

## 4. Metrics gates and the gauntlet

A `horde-gauntlet` crate (zero workspace deps) owns the registry; `gauntlet run [gate…] [-p crate…]`, `gauntlet report`. Registry self-tests assert the exact gate list and each gate's enforcement level, so promoting a gate is a deliberate reviewable diff.

| Gate | Tool | Enforcement |
|---|---|---|
| coverage | cargo-llvm-cov | ladder |
| crap | cargo-crap, threshold **15** (untested ⇒ ≤3 branches; any fn ≤ CC 15) | ladder |
| duplication | jscpd | ladder |
| mutants | cargo-mutants, diff-scoped | ladder |
| unused deps | cargo-machete (dev-deps included — aii's proptest lesson) | **blocking** |
| module structure | cargo-modules: orphan files + cycle ratchet | **blocking** |
| fmt / clippy / tests | cargo fmt, clippy `-D warnings`, nextest | **blocking** (CI proper, not gauntlet rows) |
| proto contract | buf lint + breaking | **blocking** |

**Rollout ladder:** Observe (report only) → Ratchet (may not get worse; baselines committed in `baselines/`, bidirectional — a baselined debt that no longer exists also fails — updated only by hand-copied reviewable diff, never an `--update` flag) → Hard gate (absolute threshold chosen from evidence, not defaults). Structural gates skip the ladder: they aren't measurements needing calibration.

**Lints:** workspace-level clippy deny for `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro`. `clippy.toml`: `expect`/`panic` allowed in tests, **`unwrap` banned even in tests**. These are compile errors under `-D warnings`, deliberately not gauntlet rows (one place per policy).

## 5. Wiring

- **CI blocking workflow:** fmt, clippy, nextest (all features), buf lint/breaking, machete, module-structure. Runs per PR/push.
- **Quality gauntlet workflow:** builds the gauntlet binary once, fans out gate jobs from the artifact, aggregates into one report job (`if: always()`) that renders every gate — including crashed ones as `no result` — into the step summary. Delta baselines come from the last green run's artifact (distinct from committed ratchet baselines); missing baseline degrades to `n/a`, never a wrong delta.
- Ladder-stage measurement gates run **non-blocking per PR** (attribution beats nightly), promotable per the ladder.
- Local = CI: any CI number reproduces with `gauntlet run <gate>` at the pinned tool version.
- Gate documentation is part of the gate: a new check lands with its what/why/local-command/how-to-read section in this doc's companion (`docs/QUALITY_GATES.md`, created with the gauntlet crate) in the same commit.

## 6. Horde-specific must-cover list

The scenarios the suite exists to hold, mapped to layers: event-log resume equivalence (property), partition → journal → reconcile (fault injection + Gherkin), permission chain incl. judge fallback and tree-path routing (unit + e2e), capability attenuation (property), spawn-queued-with-reason semantics (integration), relay push/fetch across two workers (e2e), checkout policies incl. multi-repo worktree composition (integration), enrollment + revocation (e2e), stream-json adapter conformance (golden transcripts — the ADR-0003 spike grows into this suite), tier selection strategies (unit + property), TUI zoom levels and inbox (snapshots).
