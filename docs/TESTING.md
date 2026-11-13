# Horde — Development Quality Suite

The goal: **agent-written code is trusted through the gates it passed, not by being read line by line.** Every instrument below exists to catch a specific way an agent (or a human) can produce code that looks right and isn't. Inspired by the `aii` gauntlet; adapted, not copied.

This defines the end state; the runner and gates landed with the first crate, every gate blocking from day one (§4).

## 1. Principles (instrument integrity)

Rules about the instruments themselves, each one paid for somewhere by a green result that wasn't earned:

- **A gate that could not run did not pass.** Tool errors (`Error`) fail the job for *every* gate, advisory or not; `Fail` is a verdict about code and only blocks for blocking gates. An unmeasured gate is a broken gate.
- **`skipped` is distinct from `pass`.** A zero must state which of its two meanings it has ("nothing to measure" vs "measured clean").
- **Missing rows are rendered, not dropped.** A gate silently vanishing is how a quality layer dies without anyone noticing.
- **Never silently skip on missing infrastructure.** If a test needs a container/binary and can't get it, the test *fails*. No degrade-to-green.
- **Tests exercise production code.** No test-side reimplementation of production logic; no mocking frameworks (mockall/wiremock are banned by convention). Fakes are *real alternate backends* (§3), mirrored against their production counterparts.
- **Positive controls run both ways.** A test proves it can go red: removing the behavior reds it, and a spurious trigger reds it too. Check counts, not just exit codes.
- **A mutation that stays green is a finding, not a relief.**
- **Gate definitions live in Rust, not YAML.** Tool, flags, exclusions, thresholds, and enforcement level are code in the gauntlet crate; every machine runs the same binary. If a flag wants to live anywhere else, it belongs in the gate. Tool versions are pinned (an upgrade silently moves a calibration baseline).
- **Tests that cannot yet reach a defect ship labeled, not deleted.**

## 2. Test pyramid

| Layer | Tooling | What it covers |
|---|---|---|
| Unit | inline `#[cfg(test)]`, cargo-nextest | Pure logic: scheduling decisions, permission-chain evaluation, config precedence resolution, tier selection strategies |
| Property | **proptest** (used, not just declared) | Invariants under generated inputs: event-log ordering/resume (sync-from-any-offset ≡ full replay), tier selection strategies. Reconciliation idempotence and capability attenuation were listed here before their mechanisms existed; they return to this row with them (attenuation has no mechanism to test yet — DESIGN §5) |
| Integration | `tests/*.rs` per crate, shared `horde-testkit` | Real components, transport collapsed to in-process where possible: server + SQLite, relay against real git repos, worker supervising a FakeAdapter |
| Multi-process e2e | `horde-testkit` process harness | Real `horde-server` + real `horde-worker`(s) + CLI client over real localhost gRPC/mTLS: spawn trees, assign/result flows, attach/replay, enrollment |
| Multi-node (Docker) | `crates/horde-multinode` over `docker/multinode/compose.yaml`, `#[ignore]`d (§7) | The real binaries in separate containers with separate network identities: placement across workers, the Relay's mTLS door, cross-worker shares, worker kill/restart against the lease, network partitions, a second client machine |
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
| coverage | cargo-llvm-cov | **blocking** (the measurement must happen; no threshold yet — chosen from evidence, per QUALITY_GATES.md) |
| crap | cargo-crap, threshold **15** (untested ⇒ ≤3 branches; any fn ≤ CC 15) | **blocking** |
| duplication | jscpd | **blocking** |
| mutants | cargo-mutants, diff-scoped | **blocking** |
| unused deps | cargo-machete (dev-deps included — aii's proptest lesson) | **blocking** |
| module structure | cargo-modules: orphan files + cycle ratchet | **blocking** |
| fmt / clippy / tests | cargo fmt, clippy `-D warnings`, nextest | **blocking** (gauntlet rows like every other gate) |
| proto contract | buf lint + breaking | **blocking** |

**Rollout ladder:** Observe (report only) → Ratchet (may not get worse; baselines committed in `baselines/`, bidirectional — a baselined debt that no longer exists also fails — updated only by hand-copied reviewable diff, never an `--update` flag) → Hard gate (absolute threshold chosen from evidence, not defaults). As built, the ladder's lower rungs are unused: this workspace started from zero code, so every gate entered blocking with no debt to grandfather (QUALITY_GATES.md). The rungs remain in the code for gates added later against a tree that already has history. Structural gates skip the ladder either way: they aren't measurements needing calibration.

**Lints:** workspace-level clippy deny for `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro`. `clippy.toml`: `expect`/`panic` allowed in tests, **`unwrap` banned even in tests**. These are compile errors under `-D warnings`, deliberately not gauntlet rows (one place per policy).

## 5. Wiring

Development is local-only — no PRs, no hosted CI (IMPLEMENTATION.md's ground rules superseded the CI workflows this section once specified). The gauntlet binary is the only enforcement mechanism.

- **`gauntlet run` on `main` after every merge is the quality bar.** A red gauntlet on `main` is reverted or fixed forward immediately; nothing else lands meanwhile.
- **One binary, one number:** every result comes from `gauntlet run <gate>` at the pinned tool version. There is no second pipeline to drift from it, and no machine whose numbers mean something different.
- **Whole-tree mutant sweeps run at wave boundaries** (`gauntlet run --full`); merge-time runs are diff-scoped. QUALITY_GATES.md says why the sweep is pinned to waves and not a timer.
- Gate documentation is part of the gate: a new check lands with its what/why/local-command/how-to-read section in this doc's companion (`docs/QUALITY_GATES.md`, created with the gauntlet crate) in the same commit.

## 6. Horde-specific must-cover list

The scenarios the suite exists to hold, mapped to layers: event-log resume equivalence (property), partition → journal → reconcile (fault injection + Gherkin), permission chain incl. judge fallback and tree-path routing (unit + e2e), capability attenuation (property — once its mechanism lands, DESIGN §5), spawn-queued-with-reason semantics (integration), relay push/fetch across two workers (e2e), checkout policies incl. multi-repo worktree composition (integration), enrollment + revocation (e2e), stream-json adapter conformance (golden transcripts — the ADR-0003 spike grows into this suite), tier selection strategies (unit + property), TUI zoom levels and inbox (snapshots).

## 7. Multi-node over Docker

The multi-process e2e tier runs every process on one host, so every "machine" shares a loopback interface, a disk, and a clock. The multi-node tier runs Horde's real release binaries in separate containers, each with its own hostname and address on a bridge network, so multi-machine behaviour crosses a real network: name resolution, TLS by host name, connections that die without a FIN, disks that do not overlap.

### Topology

`docker/multinode/compose.yaml`, one project per scenario:

| Service | Runs | Notes |
|---|---|---|
| `server` | `horde-server --serve` on `0.0.0.0:7777`/`:7778`, the Relay's mTLS door on `:7779`, `--san server`, `--worker-lease 15` | State in a named volume; its bootstrap token is the root of trust |
| `worker-1` … `worker-3` | `horde-worker` on the fake adapter, `--workspace-root /work`, `--relay https://server:7779` | `worker-1` runs `fixtures/report.json`: an agent's first turn calls `report_result`; later turns echo |
| `client`, `client-2` | idle; the runner `exec`s `horde --json` in them | `client` joins with the bootstrap token, `client-2` with a code `client` mints |
| `origin` | `git daemon` serving `git://origin/atlas.git` (one seed commit) | The upstream every workspace clones |
| `worker-real` | profile `real` only: the host's `claude` and `codex` | See [Real harnesses](#real-harnesses) |

Trust is bootstrapped only through the operator flows, with mTLS throughout and no `--insecure`. `client` redeems the server's bootstrap token with `horde enroll join --token-file /dev/stdin`. The runner mints each worker's token with `horde worker enroll <name>` and writes it to the worker container's `/join/token` over `docker compose exec` stdin. The worker entrypoint (`docker/multinode/horde-node`) redeems it with `--join-file`. A restarted worker finds its identity and stored endpoint, and needs neither.

The image (`docker/multinode/Dockerfile`) is a two-stage build: `rust:1.95.0-slim-trixie` compiles the three binaries in release with BuildKit cache mounts for the registry and target directory, and `debian:trixie-slim` (plus `git`, `procps`, `ca-certificates`) runs them as uid 10001. Both base images are pinned by digest. `.dockerignore` keeps `target/`, `.claude/`, `.git/` and the harness's own files out of the context.

### Running

```sh
# Build the image (the scenarios also do this; it is a cache hit after the first time).
docker compose -f docker/multinode/compose.yaml build server

# Every fake-adapter scenario. They run in parallel, one compose project each.
cargo nextest run -p horde-multinode --test multinode_suite --run-ignored only
```

Every scenario is `#[ignore]`d, so the gauntlet's `test` gate never needs Docker. Asked for explicitly, a scenario with no Docker daemon fails; it never skips. Set `HORDE_MULTINODE_KEEP=1` to leave a project running after the test for a post-mortem (`docker compose -p hmn-<scenario>-<pid> logs`). A failing scenario prints every service's last 80 log lines. `HORDE_MULTINODE_IMAGE` builds and runs a differently tagged image, which is how a deliberately broken build is checked against the scenarios without touching the real tag.

Measured on the 32-core development host: a cold image build takes 3 min 51 s, and an incremental rebuild after a one-line worker change takes about 50 s. The six scenarios take 31 s wall-clock in parallel, between 9 s and 31 s each. The lease and partition scenarios are the slow ones, because they wait out real keepalives and a 15 s lease.

### Scenarios

| Scenario | File | Holds |
|---|---|---|
| Placement across workers | `placement.rs` | Three unpinned sessions land one per worker (least loaded), and each is started by the worker it landed on |
| Git relay and cross-worker share | `relay.rs` | A commit made on `worker-1` is stamped on the sender's result, staged to the server's mirror through the mTLS door, absent from the origin, and present in the receiver's checkout on `worker-2` after a `--git-ref` send. A container with no identity is refused at the same door |
| Kill and restart inside the lease | `liveness.rs` | SIGKILL, restart: the worker redials with its stored identity and the next message revives the agent, nothing re-run |
| Kill past the lease | `liveness.rs` | The started agent goes `lost` and `failed` in the log, messages to it are refused, and the returning worker redials and takes new work |
| Network partition | `partition.rs` | `docker network disconnect` (sockets open, packets dropped), a message sent into the partition, `connect`: delivered once after the heal, never lost |
| A second client machine | `attach.rs` | `client-2`, enrolled through `horde enroll client`, follows a session (`sync --follow`, the scriptable attach) and sees a reply produced on a third container |

Assertions read the server's event log through the CLI's JSON output (`horde --json sync`, `agent status`, `worker list`) inside a client container, plus the git state of the containers' disks where a share is the claim.

The relay scenario has been checked both ways: built with the worker's identity presentation disabled, it fails with git's `certificate signer not trusted` refusal surfacing in the log.

### Adding a scenario

1. Add a module under `crates/horde-multinode/tests/multinode_suite/` and register it in `main.rs`.
2. Open with `Cluster::up("<short-name>")`. The name becomes part of the compose project name, so keep it short and unique.
3. Drive the system only through `cluster.horde_json(...)` (the CLI in `client`), `cluster.exec(service, ...)` for what an operator would do on that machine, and `compose`, `partition` and `heal` for faults. Assert on CLI JSON and on-disk git state. Never assert on log text.
4. Mark it `#[ignore = "needs Docker; run with --run-ignored only (docs/TESTING.md)"]` and keep every wait bounded. The harness's waits all are.

### Real harnesses

`worker-real` (compose profile `real`) bind-mounts the host's `claude` binary and the directory holding codex's native binaries, plus signed-in profile directories that become the worker-owned `CLAUDE_CONFIG_DIR` and `CODEX_HOME` (`/state/harness/<provider>/default`). Credentials are never in the image. Use dedicated profile directories rather than your everyday `~/.claude`. The container writes to them, for example to refresh tokens.

```sh
export HORDE_CLAUDE_BIN="$(readlink -f "$(command -v claude)")"
export HORDE_CODEX_BIN_DIR=<npm prefix>/lib/node_modules/@openai/codex/node_modules/@openai/codex-linux-x64/vendor/x86_64-unknown-linux-musl/bin
export HORDE_CLAUDE_PROFILE=$HOME/horde-docker/claude   # CLAUDE_CONFIG_DIR=... claude, then /login
export HORDE_CODEX_PROFILE=$HOME/horde-docker/codex     # CODEX_HOME=... codex login
export HORDE_UID=$(id -u) HORDE_GID=$(id -g)
cargo nextest run -p horde-multinode --test real_harness --run-ignored only
```

The smoke spends real quota: one Claude (haiku) and one Codex turn. With any variable unset it fails, naming the missing ones.

### Not covered

- Clock skew between machines, and asymmetric or lossy links. Partitions here are total, and the in-repo fault proxy (§2) still owns delay and flapping.
- A server restart or server-host failure across machines. The fault suite covers server kill on one host.
- Port forwards and interactive login across containers. Both stay in the single-host e2e tier.
- Different OSes and CPU architectures. Every container is the same Linux x86_64 image; the macOS and Windows workers are exercised by hand (W12g).
- Performance and scale. Three workers is a topology, not a load test.
- Credential-less workers (DESIGN §8 end state). Workers still clone the origin directly, with ambient access.

