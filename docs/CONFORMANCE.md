# DESIGN.md Conformance Audit

This file has two parts. **Part 1** is the wave-11 refresh, checked against the
code at `22f2599` (2026-11-02): current verdicts, the surfaces waves 8–11 added,
the status of every W7d gap and contract disagreement, and the deferred list.
Every claim in Part 1 was checked against code, and its file:line citations are
for `22f2599`. **Part 2** is the original W7d audit at `dddec01`, kept as the
record the fix lists below refer to; its verdicts and line numbers are historical.

The summary rows for §4, §5 and §9, the a8 row, and contract disagreements 2 and
3 were re-checked at `9218ac2` after the three wave-11 resolutions below; their
citations are for `9218ac2`.

## Part 1 — Wave-11 refresh (`22f2599`)

### Summary

| Section | Verdict | One line |
|---|---|---|
| §1 Goals and constraints | CONFORMS (index) | Each goal resolves below; single-user still holds everywhere. |
| §2 Three roles, three binaries | CONFORMS | All three binaries exist (`crates/horde-worker/src/bin/horde_worker.rs:13`); `horde up` stands in for the unbuilt `--with-local-worker`; the version gate is enforced (`service/shared.rs:268`, called at Hello `control_plane.rs:412` and registration `service/worker.rs:85`). |
| §2 Transport and resilience | CONFORMS | Worker redials on 0.5 s→10 s backoff (`horde-worker/src/startup.rs:54-59,814-852`); lease, boot reconcile, journaled inbox (ADR-0011, ADR-0012). |
| §3 Identity and security | CONFORMS | Role table per RPC, worker identity from its certificate, mint/revoke Client-only, revocation severs streams, journal reset, `--remint` (ADR-0009). Rotation is still manual re-enrollment. |
| §4 Providers and adapters | CONFORMS | Both adapters run under worker-owned profiles (ADR-0010); interactive-auth relay built with `--account`. Held provider accounts are advertised on every dial and re-advertised after a sign-in (`horde-worker/src/login.rs:100,248`; `startup.rs:691`) — checked at `9218ac2`. |
| §5 Agent model | PARTIAL | The endpoint is wired and agents spawn agents; an MCP-spawned child inherits its parent's spec — workspace, agent type, model, permission mode — and may not exceed it (`horde-server/src/lineage.rs:28,54`, applied at `service/control_plane.rs:318,321`) — checked at `9218ac2`. Reaper and native-tool suppression not built. |
| §6 Sessions, placement, scheduling | CONFORMS | Placement skips workers lacking the harness; messages to unplaced agents buffer; idempotent retries. `MaterializationProgress` still has no producer. |
| §7 Workspaces | PARTIAL | `horde workspace create/list/get/add-repo/update/delete`; `unmanaged_paths` stored but read by no worker code; no cwd auto-registration. |
| §8 Git backbone, relay, blob store | CONFORMS (as built) | Relay mounted by `--relay-bind`; dirty trees refused on the stamp path (`horde-worker/src/share.rs:139-144`); blob GC absent by design. |
| §9 MCP hub (ADR-0004) | CONFORMS | Hub warmed at serve; canonical `horde/<tool>` names. MCP-spawned children carry their parent's agent type, so allowlists resolve for them (`horde-server/src/lineage.rs:28`; `hub.rs:152,194`) — checked at `9218ac2`. |
| §10 Permissions | PARTIAL | Chain driver runs in `serve()` with `PermissionModes::shipped()` (`horde-server/src/startup.rs:443-451`); the judge is still a seam with no production implementation. |
| §11 Clients | CONFORMS (largely) | Front door, `doctor`, `agent status`, stale-stream badge, subtree attach, actor attribution on every event that carries an `Actor`. Archive and workspace/MCP edits stay unattributed. |
| §12 Usage and observability | PARTIAL | Usage folds conform; threshold alerts absent; OTLP plaintext only; hard budgets shape-only. |
| §13 Configuration layering | ABSENT | No config file is loaded anywhere; tiers are `TierCatalog::shipped_defaults()` (`service/commands.rs:646`). |
| §14–§15 | CONFORMS | Unchanged. |
| §16 Development quality | CONFORMS | Ten blocking gates; coverage has a 95.0 floor (`horde-gauntlet/src/gates/coverage.rs:20`). |

### Surfaces added in waves 8–11

| Surface | Implementation | Proving test |
|---|---|---|
| `horde up` — takes `up.lock`, finds or starts a server, enrolls the client from the bootstrap-token file only for a server it started (fingerprint-matched), proves the pair with a Hello, mints a worker token and passes it as `HORDE_JOIN_TOKEN`, prints `ready`; every wait is bounded (startup 60 s, Hello 20 s, teardown 10 s) | `crates/horde-cli/src/up.rs:52-74,316,448-505,530-767,806-838` | `crates/horde-cli/src/up/tests.rs:242,565`; `crates/horde-e2e/tests/e2e_suite/joining.rs:275` |
| `horde doctor` — identity, certificate windows (warn under 7 days), endpoint, Hello within 5 s, harness binaries on PATH, state-dir modes, disk (warn under 2 GiB, FAIL under 256 MiB); any FAIL exits 1 | `crates/horde-cli/src/doctor.rs:19-28,172`; `bin/horde.rs:60` | `crates/horde-cli/src/doctor/tests.rs:319,353` |
| `horde agent status` — folds lifecycle and placement client-side; names a LOST placement | `crates/horde-cli/src/agent.rs:76-219` | `agent.rs:270`; `crates/horde-cli/tests/cli_suite/cli.rs:496` |
| `horde worker reset` — records the journal floor as `WorkerJournalReset` and declares the worker's agents lost; Client-only | `crates/horde-server/src/liveness.rs:315`; `trust.rs:169` | `liveness.rs:650`; `cli_suite/verbs.rs:301` |
| `horde workspace update` — rename, re-lay a repo's branch or path, set or clear unmanaged paths, always at the caller's etag; the repo set cannot change | `crates/horde-cli/src/workspace_update.rs`; `crates/horde-server/src/workspace.rs:216,249` | `workspace_update.rs:128,145`; `workspace.rs:465`; `server_suite/contracts.rs:504` |
| `horde-server --remint` — finds the running server through its state lock and SIGHUPs it; the server re-mints while no client is enrolled; signals are guarded | `crates/horde-server/src/remint.rs:34`; `startup.rs:299,323,379-390` | `startup.rs:1303,1460` |
| `--san` validated as an IP literal or well-formed host name at parse | `crates/horde-server/src/startup.rs:151,165` | `startup.rs:896` |
| Worker inbox (ADR-0012) — commands accepted into a journaled `inbox` in the acking transaction, run on per-agent lanes, replayed at start | `crates/horde-worker/src/runtime/mod.rs:304`; `journal.rs:252`; `startup.rs:824` | `inbox.rs:149`; `runtime/tests.rs:920,2003,332` |
| Per-account login — `horde worker login --account` signs in `harness/<provider>/accounts/<name>` | `crates/horde-cli/src/cli.rs:376`; `crates/horde-server/src/login/relay.rs:149`; `crates/horde-worker/src/login.rs:120-141` | `login/relay.rs:477`; `horde-worker/src/harness.rs:280` |
| Worker lease and LOST agents (ADR-0011) — 120 s default, `--worker-lease`; HTTP/2 keepalive 20 s/10 s both ends; boot reconcile | `crates/horde-server/src/liveness.rs:56,211,229,279`; `transport.rs:52-55` | `liveness.rs:426,583,596` |
| Session projection and read pool (ADR-0013) — in-memory projection, four `query_only` WAL readers, one-transaction batch ingest, ListSessions name filter | `crates/horde-server/src/projection.rs:52`; `log/mod.rs:83,264`; `log/store.rs:111`; `service/ingest.rs:374`; `control_plane.rs:390` | `projection.rs:396`; `log/mod.rs:792`; `store.rs:710,751`; `ingest.rs:931`; `control_plane.rs:2594` |
| Trust boundary (ADR-0009) — per-method role table, unknown methods refused; worker from its certificate; dead letters to `horde-dead-letters`; worker sync scoped to its sessions | `crates/horde-server/src/trust.rs:129-205,270`; `service/ingest.rs:52,536,600`; `control_plane.rs:634-671,1840` | `trust.rs:344,496`; `ingest.rs:807,1300`; `server_suite/trust.rs:390` |
| Harness isolation (ADR-0010) — `CLAUDE_CONFIG_DIR`/`CODEX_HOME` under `<state>/harness/<provider>`; MCP tokens in env or 0600 files; one process group per harness, leftovers swept at start | `crates/horde-worker/src/harness.rs:42`; `claude.rs:19`; `codex.rs:45,324`; `process.rs:223,519` | `claude.rs:542,637`; `worker_suite/supervision.rs:897,940,1037` |
| Wire limits — 16 MiB messages and 1 MiB push batches on the worker spine; 64 KiB output chunks | `crates/horde-proto/src/limits.rs:14,19`; `horde-server/src/transport.rs:133`; `horde-worker/src/client.rs:54`; `runtime/shape.rs:25` | `limits.rs:26`; `shape.rs:319` |

### Status of the W7d fix list (Part 2, (a))

| # | Status | Evidence at `22f2599` |
|---|---|---|
| a1 worker binary and endpoint | CLOSED | `horde-worker/src/bin/horde_worker.rs:13`; journal, adapters, redial in `startup.rs:519,571,814-852`; endpoint `endpoint.rs:145,338,363`. `--with-local-worker` superseded by `horde up`. |
| a2 `spawn_agent` routing | CLOSED | Task becomes the initial assignment (`horde-mcp/src/verbs.rs:147-153`); keys minted unique (`endpoint.rs:420-424`). |
| a3 mint/revoke authz | CLOSED | `horde-server/src/trust.rs:167-176`; `control_plane.rs:1376,1416`. |
| a4 revocation reaches streams | CLOSED | `service/shared.rs:182-219` wraps every long-lived stream. Test `server_suite/enrollment.rs:458`. |
| a5 chain driver in `serve()` | CLOSED | `startup.rs:443-451`. |
| a6 version gate | CLOSED | `service/shared.rs:268`. |
| a7 archive producer | CLOSED | Session archive folds into `ArchiveAgentCommand` (`service/commands.rs:381-406`). |
| a8 children from parent HEAD | CLOSED | At `9218ac2`: an MCP-spawned child inherits its parent's workspace (`horde-server/src/lineage.rs:28`) and its own checkout starts at the parent's committed HEAD (`horde-worker/src/runtime/shared.rs:204-243`), as the endpoint tells agents (`horde-mcp/src/endpoint.rs:947`). Test `horde-e2e/tests/e2e_suite/lineage.rs:11`. |
| a9 TUI staleness | CLOSED | `horde-tui/src/app.rs:68,403`; badge `view.rs:224-235`. |
| a10 relay and hub in `--serve` | CLOSED | `startup.rs:456-457,594-628`. |
| a11 `require_committed` on stamp | CLOSED | `horde-worker/src/share.rs:139-144`. |
| a12 real `AgentSpec` from clients | CLOSED | `SpecArgs` (`horde-cli/src/cli.rs:683-794`); workspace, prewarm, worker list verbs exist. |
| a13 `Actor` attribution | PARTIAL | `actor_of` (`control_plane.rs:1740`) fills every event with an `Actor` field; archive-session and workspace/MCP-config events have none. |
| a14 TUI subtree scope | CLOSED | `horde-cli/src/tui.rs:160-165,252-267`; `horde-tui/src/app.rs:144-197`. |
| a15 advertisement fed | CLOSED | Warm instances, tags, remotes on every registration (`horde-worker/src/startup.rs:534-536,678-691`). The worker never calls `ReportCapabilities`/`ReportWorkspaceInstances`, so warmth refreshes only at re-registration. |
| a16 coverage threshold | CLOSED | `FLOOR_PERCENT = 95.0` (`gates/coverage.rs:20`). |

**Contract disagreements (Part 2):** 1 CLOSED (unique keys; a reused key on a different event kind is refused loudly, `service/shared.rs:370,422-442`). 2 CLOSED (as a8). 3 CLOSED — clients send full specs, and MCP-spawned children inherit the parent's `agent_type` unless they name another (`horde-server/src/lineage.rs:28`; `horde-mcp/src/verbs.rs:171`), so hub allowlists resolve for them (checked at `9218ac2`). 4, 5, 6, 9, 11 CLOSED (a6, a4, a9, a11, a14). 7 OPEN — `RevivalIdentity::is_resumable_under` (`horde-adapter/src/identity.rs:24,61`) is still called only by its tests. 8 OPEN — journal and session sequences are both raw `uint64` on the wire. 10 PARTIAL (as a13). 12 PARTIAL — `workspace_definition_id` is read and `horde new` exists; `MaterializationProgress` has no producer (its comment now says so) and `unmanaged_paths` is read by no worker code.

**DESIGN amendments (Part 2, (b)):** landed in W7f and kept current since; spot-checked at `22f2599`: §4's Codex bullet (DESIGN.md §4), ADR-0001's worker-table scope ruling, TESTING.md §5, and the `horde new` proto comment.

### Open findings at `22f2599`

1. **Agent-spawned children get a default spec** (`horde-mcp/src/verbs.rs:147-153`): no workspace, no agent type, no inheritance from the parent. Consequences: a8, c2, c3 above.
   **RESOLVED (wave 11, after `22f2599`).** The Server fills every field a spawn naming a parent leaves unset (workspace, agent type, model selection, permission mode) from that parent's spec as the log recorded it (`horde-server/src/lineage.rs:28`, applied in `service/control_plane.rs:305,981`). The checkout policy is not inherited, so a child gets its own worktree, which the worker starts at the parent's committed HEAD (`horde-worker/src/runtime/shared.rs:196-243`). A spawn made by an agent through its worker is refused when it would hold more than its parent: another workspace, another permission mode, or a type that sees an MCP server the parent's type cannot (`lineage.rs:54`, ADR-0009). The tool takes optional `agent_type` and `tier` overrides (`horde-mcp/src/endpoint.rs:71,74`; `verbs.rs:120,171`). Tests: `horde-server/src/lineage.rs` units; `server_suite/trust.rs:428`; `horde-mcp/tests/verbs_round_trip.rs:296`; end to end through a real worker, `horde-e2e/tests/e2e_suite/lineage.rs:11` (the child's checkout HEAD equals the parent's commit). This closes a8 and contract disagreements 2 and 3.
2. **The control plane keeps tonic's 4 MiB message default.** Only `WorkerService` sets 16 MiB (`horde-server/src/transport.rs:131-135`, `horde-worker/src/client.rs:54-55`); the `ControlPlane` server and the CLI's client do not. An event between 4 and 16 MiB can be ingested from a worker but not read back over `SyncEvents`. Not exercised by any test.
   **RESOLVED (wave 11, after `22f2599`).** `horde_proto::limited!` (`horde-proto/src/limits.rs:29`) applies `MAX_MESSAGE_BYTES` to both encode and decode. Every tonic server uses it: control plane, blobs, worker spine and enrollment (`horde-server/src/transport.rs:131,140,146,152`; the test harness serves through the same builders). Every tonic client uses it too: the CLI's control-plane and blob clients, which the TUI and the MCP endpoint share (`horde-cli/src/client.rs:527,534`), the worker's spine and blob clients (`horde-worker/src/client.rs:53`, `blob.rs:57`), and enrollment (`horde-identity/src/enroll.rs:99`). Tests: a 5 MiB output chunk journaled over the worker spine and a 5 MiB note sent over the control plane both come back whole over `SyncEvents` (`server_suite/services.rs:1614`, `cli_suite/round_trip.rs:496`).
3. **Provider accounts are advertised empty** (`horde-worker/src/capabilities.rs:62`) while the scheduler matches `required_provider_accounts` against them (`horde-server/src/scheduler.rs:361-376`): a spawn requiring an account looks unplaceable. Not traced end to end.
   **RESOLVED (wave 11, after `22f2599`).** The worker advertises every `<profile>/accounts/<name>` directory that a sign-in has written into (`horde-worker/src/harness.rs:134`), for each provider whose login recipe signs in per account (`login.rs:100`). Every dial carries them (`startup.rs:691`). A login that exits 0 makes the worker register again, mid-connection, with the accounts and instances now on disk (`login.rs:240,378`), and that registration drains the queue. Tests: `harness.rs:325`; `server_suite/placement.rs:339`, where a queued account-requiring spawn places once its worker advertises the account; `worker_suite/logins.rs:179`, end to end from `horde worker login --account work` through the re-advertisement to the queued spawn's placement.
4. **ADR text behind the code:** ADR-0007 still lists a re-mint verb as not done; ADR-0010 says the login wire does not yet carry the account; ADR-0013 says the CLI consumes the name filter "in a later change". All three are built.
   **RESOLVED.** Each ADR now has a dated amendment note (2026-11-02).

### ADR verdicts at `22f2599`

| ADR | Verdict | Notes |
|---|---|---|
| 0001 event log | CONFORMS | Worker-table scope ruled (amendment); batch ingest is still appends only. |
| 0002 git backbone | CONFORMS | Relay mounted by `--relay-bind`; clones go through git's transport (`--no-local`). |
| 0003 claude stream-json | CONFORMS | Hardening now reachable; `--setting-sources ''`/`--strict-mcp-config` per ADR-0010. |
| 0004 central hub | CONFORMS | Hub calls are worker-forwarded (W9b, `horde-worker/src/hub.rs`); canonical `horde/<tool>` names (amendment). |
| 0005 codex app-server | CONFORMS | The exec re-probe is done (2026-08-19 amendment); per-call usage differencing (2026-10-30 amendment). |
| 0006 port forwards | BUILT (wave 8) | Not re-audited in this refresh. |
| 0007 joining UX | CONFORMS | Join token, stored endpoint, `horde up`, state locks; amendment text lags on re-mint (finding 4). |
| 0008 session front door | CONFORMS | Front door, names through the server's name filter, JSON refusals. |
| 0009 trust boundary | CONFORMS | See the surfaces table. |
| 0010 harness isolation | CONFORMS | See the surfaces table; account-wire text lags (finding 4). |
| 0011 delivery and liveness | CONFORMS | Lease, boot reconcile, reset, position resume. |
| 0012 worker inbox | CONFORMS | Inbox, lanes, replay. |
| 0013 read path and ingest | CONFORMS | Projection, read pool, batch ingest, name filter. |

### Deferred by design (supersedes Part 2 (c))

| Item | Where deferred | Status at `22f2599` |
|---|---|---|
| ssh worker bootstrap | §3/§14 | Built in wave 12 (W12i): `horde worker add` (`crates/horde-cli/src/worker_add.rs`, `worker_add/plan.rs`; ADR-0007 2026-11-13 amendment); tests `worker_add/tests.rs`, `worker_add/plan/tests.rs`, `crates/horde-e2e/tests/e2e_suite/worker_add.rs`. |
| Harness install recipes | §3/§14 | Absent: `worker add` reports missing harnesses and installs none. |
| Raw/self-hosted model harness | §4/§14 | Absent: `--adapter` takes only `fake`, `claude`, `codex` (`horde-worker/src/flags.rs:43-47`). |
| Hard budgets | §12/§14 | Proto shape only (`agent.proto:155`); the CLI sends `budget: None` and nothing reads it. Budget unit (§15) undecided. |
| Threshold usage alerts | §12 | Absent. |
| OTLP over TLS | §12 | Absent: the exporter has no TLS (`horde-observe/src/pipeline.rs:131`). |
| Blob GC and deletion | §8/§15 | Absent by design (`horde-server/src/blob.rs:24-37`). |
| Orchestrator-level sandbox profiles | §10/§14 | Absent; the harnesses' own sandboxes carry it. |
| Credential-less workers (relay-only git) | §8/§14 | Absent: workers clone remotes with ambient credentials (`horde-worker/src/workspace.rs:458`). |
| Configuration layering | §13 | Absent. |
| Multi-user server | §1/§15 | Deferred; single-user assumptions remain load-bearing. |
| Policy judge (production implementation) | §10 | A seam; a missing judge escalates to the human (`startup.rs:446`). |

Delivered since W7d and removed from this list: port forwards (wave 8), the interactive-auth relay (W9a), the hub wire (W9b, worker-forwarded calls), the codex re-probe (ADR-0005 amendment), and Gherkin, fault injection and e2e hardening (wave 7).

---

## Part 2 — W7d audit at `dddec01` (historical)

Audited: `main` @ `dddec01`, 2026-08-17. Method: every numbered DESIGN.md section,
every ADR, and every behavioral claim in CONTEXT.md was traced to implementing
code by reading function bodies — module names and doc comments were not trusted,
and the load-bearing claims were spot-checked against the functions themselves
(e.g. §10's chain order against `chain::evaluate`'s actual order). Verdicts:

- **CONFORMS** — implemented as designed, with a proving test.
- **PARTIAL** — some of the section exists; the entry says what and where.
- **ABSENT** — nothing implements it.
- **DIVERGES** — the code deliberately does something different.
- **DEFERRED-BY-DESIGN** — explicitly sequenced later (DESIGN §14, ADR-0006).

Line numbers cite this tree at `dddec01`.

## Summary

| Section | Verdict | One line |
|---|---|---|
| §1 Goals and constraints | PARTIAL | Index of everything below; the constraint "single-user, multi-user deferred" holds everywhere. |
| §2 Three roles, three binaries | PARTIAL | `horde-server` and `horde` are real; **there is no `horde-worker` binary**; `--with-local-worker` does not exist; version-skew handshake is wire-only, enforced nowhere. |
| §2 Transport and resilience (ADR-0001) | CONFORMS (server) / PARTIAL (worker) | Event log, sync-from-offset, and the command stream are the best-built things in the tree; the worker's durable journal is real but no production code ever reconnects. |
| §3 Identity and security | PARTIAL | mTLS, enrollment, and revocation conform; **any enrolled principal may mint/revoke**; revocation does not sever live streams; no rotation mechanism. |
| §4 Providers and adapters | PARTIAL | Both adapters conform to their ADRs and are strongly tested; account *advertisement* is deliberately empty; interactive-auth relay absent; DESIGN's "ACP" text is stale. |
| §5 Agent model | PARTIAL | Verbs, results, and payload stamping are built and tested — **and unreachable**: no worker stands up the per-agent MCP endpoint. Reaper, attenuation, tier wiring, native-tool suppression: absent. |
| §6 Sessions, placement, scheduling | CONFORMS (core) | Agent-unit placement, queued-with-reason, 1-hour queue timeout with expiry-as-event, warmth-first colocation preference; prewarm RPC real, its progress stream unpopulated; capability advertisement is 2 of 6 kinds. |
| §7 Workspaces | PARTIAL | Definition/instance split, checkout policies, state-file truth all conform; no `horde workspace` CLI, no `horde new`-from-cwd; repo additions raise no permission event; `unmanaged` paths are a dead field. |
| §8 Git backbone, relay, blob store | PARTIAL | Relay is real `git http-backend` behind an mTLS door; three share routes proven — but the relay is not mounted by the server binary, and **the dirty-tree refusal exists yet the production stamp path never calls it**. Blob store matches its as-built §8 text, gap note included. |
| §9 MCP hub (ADR-0004) | CONFORMS (as built) | Dual-era endpoint on pinned rmcp 3.1.2, catalog-as-fold, allowlists, permission gating — all as the ADR's implementation note says, with its open items still open. |
| §10 Permissions | PARTIAL | Chain order is exactly §10's and proven; judge is a seam with no production implementation; the chain driver is never spawned by the production binary; modes are code-configured; `expires_at` exists on the wire and is never set. |
| §11 Clients | PARTIAL | TUI zoom levels, prompt scoping, badge, inbox, steering all proven end to end; CLI parity broken for eight RPCs; both client spawn paths send an empty `AgentSpec`; actor attribution never populated; heartbeats absorbed with no stale-stream announcement. |
| §12 Usage and observability | PARTIAL | Per-agent usage events and tree/account grouping conform (CLI only — the TUI has no usage view); threshold alerts absent; hard budgets deferred with the shape present; OTLP is plaintext-only RPC-handler spans — no agent-tree-to-span mapping exists. |
| §13 Configuration layering | ABSENT | No layer of the four exists; the wire seams (`prompt_overlay`, `permission_mode`, tiers) are built and permanently fed empty values. |
| §14 Build sequencing | CONFORMS | The wave order was followed; what §14 calls "activation" is now precisely the set of unwired seams this audit lists. |
| §15 Open questions | CONFORMS (still open) | All six remain open; the §8 blob-deletion gap is a seventh in practice. |
| §16 Development quality | CONFORMS (largely) | Ten gates, all blocking (coverage blocks in name only); property tests, insta snapshots, golden transcripts real; TESTING.md §5's CI section describes workflows that do not exist (local-only per IMPLEMENTATION.md); Gherkin/fault-injection/e2e hardening are the in-flight W7a–c items. |
| ADR-0001 | CONFORMS | One INSERT in the whole server; everything else is a fold. Worker-side mutable tables raise a scope question the ADR should answer. |
| ADR-0002 | CONFORMS (library) | Mirrors, smart-HTTP, routes, commit-then-share all as ruled; no bundles anywhere. |
| ADR-0003 | CONFORMS | stream-json argv, `--resume`, settings-ownership hardening all verbatim — the hardening is gated on an endpoint wire that never happens in production. |
| ADR-0004 | CONFORMS | Matches its own implementation note, including every open item, unchanged. |
| ADR-0005 | CONFORMS | Adapter matches the amended ADR; the 0.147.0 exec/shell re-probe the ADR says is owed is still owed. |
| ADR-0006 | DEFERRED-BY-DESIGN | Wave 8; no forward code or proto exists, correctly. |

**The one-sentence conclusion:** the subsystems are built to design and proven in
isolation, but the production binaries do not assemble them — there is no worker
binary, and the server binary wires neither the permission chain driver, nor the
hub, nor the relay; the tree's biggest conformance gap is a missing wiring layer,
not missing mechanisms.

---

## §1 Goals and constraints — PARTIAL (index)

Each goal resolves to a section below: multi-provider subscription accounts (§4:
both adapters real, account isolation real, advertisement empty), agents
orchestrating agents (§5: verbs built, endpoint unwired), run anywhere (§2/§3:
mTLS and enrollment real, worker binary missing), attachable (§11: conforms),
usage tracked per tree (§12: conforms, CLI only), resilient (§2: log conforms,
reconnect loop absent), single-user (holds everywhere — and is load-bearing for
§3's and §9's accepted risks).

## §2 Architecture: three roles, three binaries — PARTIAL

- **Three binaries: two exist.** `horde` (`crates/horde-cli/src/bin/horde.rs:13`)
  and `horde-server` (`crates/horde-server/src/bin/horde_server.rs:14`). The
  worker crate declares only two test-fixture binaries, `stub-harness` and
  `codex-stub-harness` (`crates/horde-worker/Cargo.toml:49,55`); **no
  `horde-worker` role binary exists** — the worker is a library whose
  `Connection`/`Runtime` are constructed in-process by its test suite
  (`crates/horde-worker/tests/worker_suite/connection.rs:213-231`). Both live
  adapters (`ClaudeAdapter`, `CodexAdapter`) are constructed only in tests.
- **`--with-local-worker`: ABSENT.** The string appears exactly once in the repo
  — DESIGN.md:29. The serve flag parser enumerates every accepted flag
  (`crates/horde-server/src/startup.rs:128-154`) and refuses unknown ones
  (`startup.rs:158-161`). The *discipline* half of the sentence does hold: no
  in-process shortcut exists anywhere; worker tests dial a real socket.
- **Version skew: wire-only, enforced nowhere — DIVERGES from its own doc
  comment.** The handshake schema is complete
  (`proto/horde/v1/handshake.proto:20-48`), but the client's Hello is discarded
  unread (`crates/horde-server/src/service/control_plane.rs:311-320`, binds
  `_request`), the worker's Hello is never read (`service/worker.rs:127-165`
  reads only `worker_id` and `capabilities`), `MIN_SUPPORTED_SEMVER`
  (`service/shared.rs:94`) is only echoed outward and never compared, and every
  producer sets `capability_flags: Vec::new()`. The doc comment at
  `shared.rs:94` — "Below this, a peer's role is refused outright (DESIGN.md
  §2)" — asserts enforcement that does not exist; sharpest docs-vs-code
  divergence found. (The emptiness of the flags is itself deliberate and pinned:
  `crates/horde-worker/src/capabilities.rs:17-20` — "an empty list is honest".)
  A test-helper assertion message ("a Hello without a version is refused",
  `crates/horde-server-testkit/tests/secure.rs:155`) describes server behavior
  while testing a helper; it should be reworded.

### Transport and resilience (with ADR-0001) — CONFORMS (server) / PARTIAL (worker)

- **Append-only, sequence-numbered, per-session:** literal. One `events` table,
  STRICT, dense 1-based `sequence`, `PRIMARY KEY (session_id, sequence)`
  (`crates/horde-server/src/log/schema.rs:32-73`); the only mutating SQL in the
  whole server is one INSERT (`log/store.rs:129`) — no UPDATE, no DELETE.
  Partial unique indexes make worker-journal replays structurally unable to
  double-append and client retries dedupe (`schema.rs:61,66`).
- **Sync-from-offset:** `read_from` (`store.rs:221`), streamed with history →
  `CaughtUp` cursors → live tail with heartbeats
  (`service/sync.rs:196-222`). Property-proven: resuming from any offset yields
  the same suffix as a full replay
  (`crates/horde-server/tests/server_suite/sync_equivalence.rs:89`), and a
  replayed worker journal never duplicates (`sync_equivalence.rs:223`).
- **Partition behavior: PARTIAL.** The worker's durable local journal is real
  SQLite on disk with an explicit head counter (never `MAX(...)`) and
  apply-command-plus-journal in one transaction
  (`crates/horde-worker/src/journal.rs:203,9-18,390`). Reconcile-on-reconnect is
  implemented (`crates/horde-worker/src/client.rs:200-226`; the command feed
  resumes from the worker's own applied watermark, outranking the server's view,
  `client.rs:157-162`) and proven by a severed-TCP-proxy test
  (`worker_suite/connection.rs:462`) and a real-restart test
  (`connection.rs:345`). Two precise gaps: (a) **no production caller** —
  "Reconnection is the caller's policy" (`client.rs:192-194`), and no
  reconnect/backoff loop exists anywhere in the worker crate; `Journal::open`
  (the durable form) is called only by tests, everything else uses
  `in_memory()`; (b) **"keeps its agents running" is untested** — the partition
  test journals marker entries with no agent running, and the harness rebuilds
  `Runtime` on redial. What is actually proven is *revival* (journaled
  `provider_session_id`, `runtime.rs:507`), which is a different mechanism than
  surviving a partition. DESIGN §2 should say which it means.

### CONTEXT.md "Command Stream" — CONFORMS

Folded, not stored: the module header records it used to be a VecDeque and why
that was wrong (`crates/horde-server/src/service/commands.rs:1-50`); there is no
commands table (see schema evidence above); `CommandStream::open(log, worker,
resume_from)` rebuilds from the log (`commands.rs:479`). Densely numbered per
worker (`commands.rs:117,394-395`; proven by
`each_worker_is_numbered_independently_and_densely`, `commands.rs:1288`).
Resumes from the worker's own ack (`commands.rs:447`,
`service/worker.rs:180-190`; proven at `commands.rs:1466`). Re-folding the same
log reproduces every sequence and id (`commands.rs:1311`).

### ADR-0001 "everything is an event; side channels are bugs" — CONFORMS, with a scope question

Server state outside the log is all legitimately non-durable or non-session:
`WorkerRegistry` (liveness, `scheduler.rs:100`), `InstanceCache` (documented
cache, `workspace.rs:265-272`), `placement_lock` (guards no data,
`service/shared.rs:39-45`), blob bytes (log carries references, ADR-0001
contemplates it), CA key material (issuance/revocation *records* are events).
Even enrollment is a fold over a reserved stream
(`crates/horde-server/src/enrollment.rs:1-10`). **The scope question:** the
worker's journal database holds three *mutable* tables beside its append-only
journal — `applied_command`, `agent` (INSERT/UPDATE/DELETE at
`journal.rs:699,706,728,734`), `meta` — load-bearing for revival and not a fold
of anything. ADR-0001 is written about the server's session log; nothing says
whether its rule covers the worker. It should say.

## §3 Identity and security — PARTIAL

- **mTLS for every principal: CONFORMS.** CA server-side, depth-0 constraints
  (`crates/horde-identity/src/ca.rs:111,118,143`); main listener requires a
  client cert unconditionally (`tls.rs:86`), all three services wrapped in
  `require_enrolled` (`crates/horde-server/src/transport.rs:64-75`); the
  enrollment listener is the one server-auth-only door (`transport.rs:104`).
  Handshake truth table proven over real sockets (`tls.rs:469-540`). Plaintext
  *capable* paths exist but are unreachable in a deployed server: the relay's
  bootstrap listener with ambient principal `"horde"`
  (`crates/horde-relay/src/http.rs:457,527`) and the MCP endpoint's loopback
  config (`crates/horde-mcp/src/server.rs:76`) — neither is wired into
  `horde-server --serve` at all.
- **Enrollment: CONFORMS on every sub-claim.** Code is `secret.fingerprint`
  (`service/enrollment.rs:53,130`); join pins the fingerprint and verifies the
  server against it, deliberately skipping name/validity
  (`crates/horde-identity/src/enroll.rs:86`, `tls.rs:194`); keypair local, CSR
  submitted, no key field on the wire (`enroll.rs:26`,
  `proto/horde/v1/enrollment.proto:22-33`); the CSR subject is overwritten
  server-side so a joiner cannot name itself (`ca.rs:258`, test `ca.rs:372`).
  Codes expire (default 10 min, cap 60 — `code.rs:17,21,57`) and are one-time
  (spent on redemption, race-serialized — `enrollment.rs:167,201-217`,
  `service/enrollment.rs:181`). Refusals proven over the real listener
  (`server_suite/enrollment.rs:145`).
- **CLI surface: PARTIAL.** `horde worker enroll` and `horde worker revoke`
  exist (`crates/horde-cli/src/cli.rs:155,163`). **`horde-worker join` cannot
  exist** — there is no worker binary; the join capability ships as
  `horde enroll join <endpoint> <code>` on the client binary (`cli.rs:175`),
  plus `enroll client` / `enroll revoke-client` extensions (`cli.rs:182,188`).
- **Revocation: PARTIAL — the seed flag holds.** Storage and checking conform:
  journaled `EnrollmentRevoked` before the deny-list update, seeded from the
  log at startup, all serials a principal holds, enforced by both the tonic
  interceptor and the TLS verifier with session resumption disabled so a ticket
  cannot ride past it (`service/enrollment.rs:251-273,88-92`,
  `revocation.rs:14-34`, `interceptor.rs:56`, `tls.rs:244,315-316`). **But
  revocation does not sever already-established streams**: the interceptor runs
  per HTTP/2 request, so unary calls fail on the next call (tested,
  `server_suite/enrollment.rs:308`) while `StreamCommands`/`PushEvents`
  (`proto/horde/v1/worker_service.proto:31,36`) run the worker's whole lifetime
  and are never re-checked — the pump loop (`service/worker.rs:199-210`) never
  consults the deny-list, and the revoke path touches no registry and aborts no
  stream. A revoked worker with open streams keeps receiving commands and
  pushing events until it reconnects. Same handshake-only property for the
  relay's keep-alive connections and the MCP endpoint's SSE stream
  (`server.rs:243`). No test covers revoking a principal with a live stream.
  The `transport.rs:6-11` comment ("stops working at its next call") is accurate
  for unary, misleading for streaming.
- **Who may mint/revoke: the seed flag holds — any enrolled principal.**
  `mint_enrollment_code` (`control_plane.rs:1156`) and `revoke_enrollment`
  (`control_plane.rs:1192`) perform no authorization beyond enrollment itself;
  the interceptor's `PeerIdentity` is inserted into request extensions and read
  by no horde-server handler. A worker certificate can mint codes for any role
  and revoke every client. Known and pinned:
  `server_suite/enrollment.rs:445` asserts a worker identity *successfully*
  mints, commented "if this starts failing, someone added the role check the
  wave report flags as open".
- **Rotation: ABSENT (flagged gap, not divergence).** Lifetimes CA 20y /
  listener 2y / principal 1y (`ca.rs:34-44`); no renew RPC, no expiry warning;
  the documented story is manual re-enrollment (`ca.rs:17-18`), and the roster
  accumulates serials per name precisely to allow it (`enrollment.rs:220-237`).
  DESIGN §3 is silent; it should stop being.
- **No non-localhost binding before auth: CONFORMS.** Loopback defaults
  (`startup.rs:192-197`), wider binding requires explicit flags, both listeners
  authenticate. Bootstrap trust root decided in-code and flagged: a first
  client code is printed to stdout exactly when no client holds an active cert
  (`startup.rs:12-16,315`; tests `startup.rs:524`,
  `server_suite/enrollment.rs:385`).
- **ssh bootstrap + harness install recipes: DEFERRED-BY-DESIGN.** Zero hits for
  ssh/launchd/systemd/install recipes across crates and proto; `WorkerCommand`
  has exactly Enroll and Revoke (`cli.rs:151-169`).

## §4 Providers and adapters — PARTIAL

- **Adapter trait: CONFORMS with documented divergences.** Split into `Adapter`
  (factory: `provider`/`capabilities`/`spawn`/`resume`,
  `crates/horde-adapter/src/adapter.rs:18-52`) and `AgentSession`
  (`send`/`next_event`/`interrupt`/`answer_permission`/`shutdown`,
  `adapter.rs:56-99`) — all five DESIGN verbs present, `stream` as pull-one
  `next_event`, split justified in `docs/ADAPTERS.md`. Capabilities are a closed
  11-value enum including `Resumable` (`capability.rs:23-118`) plus the
  ADR-0005 vocabulary `ToolCallKind`/`PermissionEnforcement` with the kind-blind
  answer defined as the floor (`capability.rs:236-285`; tests
  `capability.rs:300-466`).
- **Claude adapter (ADR-0003): CONFORMS**, with the wiring caveat. stream-json
  child process (`crates/horde-worker/src/claude.rs:64-72`; real-child tests
  `worker_suite/supervision.rs:122,230`), `--resume` with turn-number
  continuation so revived turns cannot re-mint dedupe keys
  (`claude.rs:148-159`; tests `supervision.rs:402-475`), settings-ownership
  hardening verbatim — `--settings {"permissions":{"allow":[],"deny":[]}}`
  `--permission-mode manual` (`claude.rs:87-96`, both directions tested
  `claude.rs:404-426`), cumulative-usage diffing with restart detection
  (`crates/horde-adapter/src/usage.rs:112-160`,
  `horde-adapter-claude/src/turn.rs:188-207`), interrupt as a control message
  with steerability-after proven (`codec.rs:118-125`, `supervision.rs:552`).
  Session-id/account pairing is satisfied in substance via the worker journal's
  `RevivableAgent` (`journal.rs:129-140`) — though the purpose-built
  `RevivalIdentity` type and its `is_resumable_under()` guard
  (`horde-adapter/src/identity.rs:24-63`) are never called in production: two
  mechanisms for one contract. **Caveats:** the `--permission-prompt-tool` flag
  is emitted only when an endpoint is supplied, and none ever is (see §5);
  the hardening is therefore currently unreachable code, and the manual live
  smoke exercises a different flag set (`live_smoke.rs:33-47`).
- **Codex adapter (ADR-0005 as amended): CONFORMS.** app-server JSON-RPC with
  `experimentalApi` (`crates/horde-worker/src/codex.rs:113-195`), granular
  `mcp_elicitations: true` always-on with the transcript-19 rationale
  (`horde-adapter-codex/src/codec.rs:58-108`), `action` vs `decision` reply
  shapes held apart (`codec.rs:162-180`, test `codec.rs:411`, process-level
  `worker_suite/codex_supervision.rs:302,386`), sandbox tier fail-closed with
  `danger-full-access` unrepresentable (`sandbox.rs:22-37,85-109`), tier+reason
  journaled on first turn (`codex.rs:127-144`, test
  `codex_supervision.rs:268-296`), scoped `PreAuthorizeMcpToolCalls` with the
  blanket wire flag deliberately refused
  (`horde-adapter-codex/src/lib.rs:100,127-131`,
  `horde-wire/src/capability.rs:57-68`), interrupt-without-terminal-frame
  handled (`codex.rs:461-467`, test `codex_supervision.rs:556-578`).
- **"ACP where supported": DESIGN text is stale — DIVERGES.** DESIGN.md §4
  still names ACP for Codex; ADR-0005 rejects ACP and the adapter implements
  app-server. Amend the bullet (fix list b).
- **Provider accounts: isolation real, advertisement deliberately absent,
  rotation logic unfed.** `CLAUDE_CONFIG_DIR`/`CODEX_HOME` per account
  (`claude.rs:109-115`, `codex.rs:104`; real-child test `supervision.rs:201`);
  but the worker advertises `provider_accounts: Vec::new()`
  (`capabilities.rs:48`, rationale at :36-39, pinned by
  `nothing_unmaintained_is_advertised` :189-198). The consuming side — account
  matching, exhausted-account refusal with reset phrasing, all three tier
  strategies, bind-once — is fully built and tested
  (`scheduler.rs:344-364,413-450`, `tiers.rs:235-295`) and currently has no
  live input.
- **Interactive-auth relay (PTY + port-forward): ABSENT.** The only PTY in the
  workspace is the TUI's own test harness; ADR-0006 forwards are W8.
- **Raw/self-hosted harness: DEFERRED-BY-DESIGN.** Zero hits for ollama.
- **FakeAdapter: CONFORMS to the §16 rule.** A real `Adapter`/`AgentSession`
  backend driven by scripts, pushing usage through the same `UsageAccumulator`
  as Claude, blocking on scripted permission asks
  (`horde-adapter-fake/src/lib.rs:8-27`), held to the same conformance checks
  as the recorded-transcript suites (`horde-adapter-fake/tests/conformance.rs`),
  narrowable to a non-resumable provider for the honestly-terminal path.

## §5 Agent model — PARTIAL, dominated by one missing wire

**The one missing wire:** no crate in the workspace depends on `horde-mcp` —
grep of every Cargo.toml finds only its own declaration. The worker hardcodes
`horde_endpoint: None` (`crates/horde-worker/src/runtime.rs:1035`, comment "the
sibling increment; wired here when it lands"). So the per-agent endpoint, the
`approve` permission tool, the four orchestrator verbs, the hub proxy, and
payload stamping are all built and individually tested — and a real Claude or
Codex child launched by this tree today sees none of them. ADR-0004's W6a note
described exactly this state (per-agent token minting, worker `JournalSink`,
control-plane channel still owed); nothing has moved.

- **Lifecycle states: PARTIAL.** The enum has nine members — the designed four
  plus QUEUED/STARTING/FAILED (`proto/horde/v1/agent.proto:218-233`). The live
  spine is queued→starting→running→{failed,archived}
  (producers: `state.rs:192`, `runtime.rs:349-354`, `translate.rs:138-145`,
  `translate.rs:219-230`, `runtime.rs:669-676,917-925,1270-1277,743-750`).
  **IDLE, WAITING_ON_CHILD, WAITING_ON_HUMAN are never emitted by any
  producer** — vocabulary, not behavior (downstream of the missing endpoint:
  nothing can report a result, so nothing goes idle). "Context dies at archive,
  not at result" *is* enforced and well tested
  (`runtime.rs:2345-2377`, `journal.rs:1295-1319`, `state.rs:466-484`).
- **Reaper: ABSENT.** No idle timer, no 60-minute policy, no idle-since field.
  `ArchiveAgentCommand` exists and the worker honors it
  (`worker_service.proto:139-143`, `runtime.rs:715-756`) **but nothing on the
  server ever issues it** — the command fold has no arm producing one
  (`commands.rs:150-232`); archive is unreachable end to end. No parent-cleanup
  verb (`control_plane.proto:22-94` has ArchiveSession only). No keep-warm flag
  (as designed). The only "60m idle" in the tree is an illustrative proto
  comment (`event.proto:121`).
- **Capability attenuation: ABSENT.** The spawn handler copies the spec into
  the event comparing nothing to the parent (`control_plane.rs:727-799`); no
  widening-request approval event exists. The *precondition* is real and right:
  MCP `spawn_agent` takes the parent from the authenticated caller, never an
  agent-supplied value (`verbs.rs:136-147`), so ancestry cannot be forged.
  Ancestry is used for permission routing only (`driver.rs:387-411`). The
  planned attenuation proptest (docs/TESTING.md:26) does not exist.
- **Messaging: PARTIAL — one-result-per-assignment is not enforced.** Two
  kinds/one bit, result = status+summary+payload with statuses validated not
  rounded, server-minted AssignmentId (notes get none — "owes nothing" made
  structural), delivery mode carried whole, spawn's implicit first assignment
  becomes the first turn input: all conform
  (`agent.proto:17-33,61-71,181`, `verbs.rs:91-96`, `endpoint.rs:122-129`
  with truth tables `endpoint.rs:740-788`, `control_plane.rs:577-596`,
  `commands.rs:187-198` → `runtime.rs:339-345,466-473`). But `report_result`
  writes `assignment_id: None` (`verbs.rs:238`) — the minted id never returns —
  and `wait_for_agent` matches on which *agent* reported, not which assignment
  (`verbs.rs:287-300`, admitted in its doc comment). A second result is an
  ordinary second event; a parent waiting twice is woken by whichever result
  lands first. Sibling isolation is what the test proves (`verbs.rs:324-334`).
- **Payload stamping (seed flag): observed-over-declared HOLDS for results —
  and the surface is partly misrouted.**
  - `report_result`: the agent *cannot* declare commits — params are
    {status, summary} (`endpoint.rs:89-94`), payload hardcoded None as a named
    seam (`verbs.rs:24-31,242-243`); the worker's `ShareBroker::stamp` observes
    HEAD per repo (`share.rs:131-164`) via `StampingSink` (`share.rs:304-312`),
    re-stamping refused (`share.rs:249-260`). Proven end to end with a real
    relay and second worker (`worker_suite/payload_share.rs:261-300`). Caveat:
    `Runtime::stamping_sink()` (`runtime.rs:196-198`) has no production caller.
  - `send_message`: `Assignment.payload`/`Note.payload` exist on the wire
    (`agent.proto:24,32`) and every caller hardcodes None
    (`client.rs:369,388`); the MCP `SendParams` has no payload field
    (`endpoint.rs:77-87`). Inbound obtaining *works* — commits materialized,
    attachments landed under `.horde/attachments`, unobtainable payloads fail
    the agent loudly (`runtime.rs:624-690`; test `payload_share.rs:370-427`).
    Outbound attachments are dead: `stamp` hardcodes `attachments: vec![]`
    (`share.rs:162`) and `AttachmentBroker::stash` has no production caller.
  - **`spawn_agent` (MCP) is misrouted:** the tool's `task` string
    (`endpoint.rs:61-66`) is passed as the *idempotency key*
    (`endpoint.rs:290` → `verbs.rs:131-135`), and the client sends
    `spec: Some(AgentSpec::default())` (`client.rs:532`). An agent-initiated
    spawn carries no task, no tier, no workspace, no checkout policy, no
    permission mode — and two spawns with identical task text collide on the
    key, silently swallowing the second (see the dedupe footgun below).
  - **"Children start from the parent's committed HEAD" — DIVERGES:**
    `worktree_repo` runs `git worktree add -b` with no commit-ish, branching
    from the workspace *instance's* HEAD (`workspace.rs:325-337`, its own doc
    comment says so). A parent's worktree commits do not reach its child at
    checkout time.
- **Model tiers: selection built and proven, everything around it missing.**
  Catalog with shipped `fast`/`standard`/`deep`, three strategies, candidates ×
  accounts, exhausted-never-selected, deterministic ties, `bind()`
  (`tiers.rs:60-295`; unit + property tests `tiers.rs:466-745`). **Zero of the
  four precedence layers exist** (no `.horde/` reading, no session default
  field, no workspace/user-global mapping — `tiers.rs:12-15` says so);
  `AgentSpec.model` has no consumer anywhere; nothing calls
  `options_for`/`select`/`bind`; the fold emits `binding: None`
  (`commands.rs:340`) and the worker falls back to its default provider and the
  harness's default model (`runtime.rs:1016-1062`). "Held for life" is only
  vacuously true — no migration path, but no binding either.
- **One MCP endpoint: the five tools are `approve`, `spawn_agent`,
  `send_message`, `report_result`, `wait_for_agent`**
  (`endpoint.rs:250,278,301,330,379`; names pinned by test
  `endpoint.rs:797-813`), plus hub/local proxied tools with the permission
  chain gating every proxied call (`endpoint.rs:208-215,416-462`; e2e
  `horde-mcp/tests/hub_e2e.rs`). DESIGN's "list/query" verbs are
  designed-but-unregistered — acknowledged honestly via `VerbError::Unbuilt`
  whose own test names `list_agents` (`verbs.rs:66-75,376-384`). There is no
  agent-facing attach verb, and DESIGN does not ask for one (attach is a client
  concern: `SyncEvents.subtree_root`, `control_plane.proto:193-197`,
  `sync.rs:72`).
- **Native subagent suppression: ABSENT.** No `--disallowedTools`, no Task deny,
  no `--strict-mcp-config` in production argv (`claude.rs:64-99`; those flags
  appear only in live-smoke tests). Native Task remains available to any
  spawned Claude agent — and with the endpoint unwired it is currently the
  *only* subagent mechanism such an agent has.

## §6 Sessions, placement, scheduling — CONFORMS (core)

- Session = one tree, no project entity: CONFORMS (`state.rs:89-117,26-52`;
  root minted with `parent_agent_id: None`, `control_plane.rs:326-385`; no
  project type anywhere). One wrinkle: "sessions reference a workspace" is
  carried by the *root agent's spec*, not the session —
  `CreateSessionRequest.workspace_definition_id` (`control_plane.proto:119`,
  commented "this is set either way") is **never read**, and `SessionState` has
  no workspace field. A dead wire field with a live-sounding comment.
- Agent is the unit of placement: CONFORMS — one `PlacementChanged` per agent
  (`placement.rs:287-356`), `PlacementRequest` takes no session id
  (`scheduler.rs:51-67`), decisions serialized by a lock that guards no data
  (`service/shared.rs:39-45`). Parent's worker preferred when nothing forces
  otherwise (tests `scheduler.rs:1024`, `placement.rs:734`).
- Workspace-locality colocation: CONFORMS **as a preference, not a
  constraint** — the candidate sort ranks warmth first,
  `(!warm, !on_parent, load, id)` (`scheduler.rs:289-311`), and warmth counts
  MATERIALIZING as warm so placement colocates onto an in-flight build
  (`workspace.rs:359-362`); but warmth never overrides a failed constraint
  (`scheduler.rs:243-260`). DESIGN's "agents sharing a mutable instance
  colocate" reads harder than what shipped. Tests `scheduler.rs:1003`,
  `server_suite/placement.rs:498`.
- Queued-with-reason, timeout, expiry-as-event: CONFORMS.
  `Placement::Queued { reason }` with actionable reasons naming the exhausted
  account and its reset (`scheduler.rs:44-46,214-269,417-432`); spawn never
  blocks (`control_plane.rs:783-818`); **default queue timeout is 1 hour**
  (`placement.rs:50`); expiry is a real event the fold refuses to resurrect
  (`placement.rs:336-350,60-62`; tests `placement.rs:581`,
  `server_suite/placement.rs:452`). Two caveats the code itself documents:
  expiry is evaluated only when a placement pass runs, not at the deadline
  (`placement.rs:26-33`), and the spawner's "wait / relax / cancel / escalate"
  verbs do not exist — there is no client-facing cancel-placement RPC.
- `prewarm`: the RPC and command path exist
  (`control_plane.rs:1017-1071` → `placement.rs:225-241` →
  `commands.rs:261-283`), catalog-journaled when session-less. **No CLI verb**
  (§11), and **progress does not stream**: `MaterializationProgress`
  (`workspace.proto:88-98`, "streamed while materializing") is produced by
  nothing — the worker emits one terminal report with `progress: None`
  (`runtime.rs:304-307`).
- Capability advertisement: **two of the six designed kinds are real** —
  os/arch facts (`capabilities.rs:75-101`) and workspace support. Tags,
  provider accounts, reachable remotes are hardcoded empty and headroom
  unlimited (`capabilities.rs:41-55`), deliberately and pinned by
  `nothing_unmaintained_is_advertised` (`capabilities.rs:190`); warm-instance
  reporting is plumbed but unfed (`WorkspaceManager::warm_instances`,
  `workspace.rs:221-260`, has no caller; the `ReportWorkspaceInstances` RPC has
  no worker-side caller). The server matches on all six
  (`scheduler.rs:332-388`) — so today a spawn requiring a remote refuses every
  worker.

## §7 Workspaces — PARTIAL

- Definition/instance split: CONFORMS. Definitions fold from
  `WorkspaceDefinitionChanged` in the reserved catalog stream with
  etag-as-sequence (`horde-server/src/workspace.rs:32,59-127` — the module doc
  argues this *is* the ADR-0001 reading of "server SQLite"); instances never
  self-initiated (`runtime.rs:286-312`, a worker without a manager errors
  rather than improvising); the worker state file is real and written *after*
  the clones so a crash cannot leave a state file describing an absent instance
  (`.horde-instance.pb`, `horde-worker/src/workspace.rs:38,116-120`; checkout
  reads it rather than trusting the server, `:148,421-431`); the server's
  `InstanceCache` explicitly refuses to persist (`workspace.rs:264-270`);
  materialization failure is a DEGRADED report, not a crash
  (`workspace.rs:18-25`).
- **Reconciliation: server half built, worker half absent.** Registration
  replaces the worker's cached instances and a `ReportWorkspaceInstances` RPC
  exists with MISSING evicting (`service/worker.rs:329-352,383`,
  `workspace.rs:291-307`) — but the worker never calls `warm_instances()` and
  never reports, so nothing produces the report the server is ready for.
- Checkout policies shared/worktree/fresh-clone: CONFORMS
  (`agent.proto:120-132`; `workspace.rs:136-189` — Unspecified shares the
  Worktree arm, `git worktree add -b horde/{agent_id}`, compose skips existing
  destinations so at-least-once redelivery cannot stack worktrees; tests
  `worker_suite/workspaces.rs:210-402`). (But see §5: worktrees branch from
  the instance HEAD.)
- Repo set fixed at definition: CONFORMS — `same_repo_set` refuses
  add/remove/repoint through Update, allowing only layout
  (`workspace.rs:216-241`, `control_plane.rs:937-943`); widening only via
  `AddWorkspaceRepo`'s etag-checked path (`workspace.rs:194-207`). **But the
  addition raises no permission event** — `add_workspace_repo`
  (`control_plane.rs:977-997`) appends an ordinary catalog change; the shipped
  ceremony is a separate RPC + etag, not human approval. The §15 open question
  ("more ceremony than the permission-event?") is currently answered with
  *less* ceremony than designed.
- **CLI authoring: ABSENT.** All five workspace RPCs plus `PrewarmWorkspace`
  and `ListWorkers` have neither a CLI verb nor a client-library method
  (`cli.rs:47-81` is the whole verb list; `client.rs` has no workspace
  methods); they are exercised only by `server_suite/contracts.rs`.
  **`horde new` cwd auto-registration: ABSENT** — `session new <title>` reads
  no cwd (`cli.rs:196-203`); `control_plane.proto:117-118`'s comment
  ("auto-registered from cwd by `horde new`") describes a verb that does not
  exist.
- `unmanaged` paths: **vacuously true — a dead field.** Declared and
  round-tripped (`workspace.proto:32-34`, `control_plane.rs:850,963`) but the
  worker never reads it (`materialize` iterates `definition.repos` only,
  `workspace.rs:73-128`); "never synced" holds only because the stamp walks
  repos exclusively (`share.rs:131-164`). No behavior test exists.

## §8 Git backbone, relay, blob store — PARTIAL

- **Relay: CONFORMS as a library; not mounted by the server binary.** Real
  `git http-backend` spawned as a CGI child with concurrent body/stdout piping
  to avoid receive-pack deadlock (`horde-relay/src/http.rs:380-434`, full CGI
  environment `:246-302`) — pack negotiation stays git's problem. Mirrors are
  bare, lazily cloned on first request via staging-then-rename so a crash
  leaves debris, never a half-mirror (`mirror.rs:145-174`; `refresh` omits
  `--prune` to protect `refs/horde/share/*`, `:185-189`). The mTLS door is a
  *separate listener with the same identity material* — same CA and revocation
  list as the main listener, peer principal landed in `REMOTE_USER`
  (`http.rs:475-489,562-568`) — not literally "behind the existing mTLS
  channel"; the bootstrap plaintext listener is loopback-only with ambient
  principal `"horde"` (`http.rs:457,527`). **`horde-server` does not depend on
  `horde-relay`**, and `Runtime::with_relay` has no production caller — so no
  deployed process serves or dials the relay, and a cross-worker share today
  returns `ShareError::NoRelay` (`transport.rs:159-163`).
- Transport resolution: CONFORMS — the three topologies resolve to
  `AlreadyShared` / `PeerFetch` / `ViaRelay`, with the object store *observed*
  via `git rev-parse --git-common-dir` rather than guessed
  (`resolve.rs:39-50,92-108`; tests `resolve.rs:127-196`, e2e
  `share_topologies.rs`, cross-worker through a live relay
  `worker_suite/payload_share.rs:262`). All three routes end in
  `verify_present` at the receiver (`transport.rs:145-156`); shares travel as
  content-addressed `refs/horde/share/<sha>` (`transport.rs:50-52`).
- **Commit-then-share: the error exists and is never raised — DIVERGES.**
  `require_committed` produces exactly the designed message ("commit, then
  share — dirty working trees never travel", `transport.rs:61-67,127-138`)
  **but has no production caller**: the live path, `ShareBroker::stamp`
  (`share.rs:131-164`), observes HEAD without checking the working tree, so an
  agent reporting with a dirty tree gets its HEAD stamped silently and the
  uncommitted work is omitted rather than refused. The designed helpful error
  is test-only (`share_topologies.rs:268,282`).
- Credential chokepoint: bootstrap phase *half* as designed — clones use
  ambient machine credentials by construction (inherited env,
  `workspace.rs:289-323,359-385`), but reachable remotes are **not** advertised
  (§6), so the other half of the bootstrap sentence is unwired. Relay-only end
  state DEFERRED-BY-DESIGN. No bundles anywhere (ADR-0002 holds).
- **Blob store: CONFORMS to §8's as-built paragraph, gap note included, every
  sub-claim verified including the absences.** SHA-256-addressed fan-out under
  the state dir (`blob.rs:145-173`; startup proves `blobs/sha256` exists before
  first upload, `startup.rs:285-289,556`); served behind the enrolled mTLS door
  on the main listener (`transport.rs:71-74`); upload verified against both
  declared hash and size with temp cleanup on failure (`blob.rs:381-424`; test
  `an_upload_lying_about_its_digest_is_refused_and_stores_nothing`,
  `server_suite/blobs.rs:116`); **every read re-hashed**, with a mismatch
  surfacing as a stream *failure* (DataLoss), never a clean end
  (`blob.rs:447-479`, `service/blob.rs:43,176-181`); the 64 KiB inline
  threshold is a named constant enforced at both append chokepoints
  (`horde-proto/src/blob.rs:43,135-137`, `service/shared.rs:159-168`; the
  65,536-exactly boundary is pinned by test `horde-proto/src/blob.rs:328`);
  per-blob cap `--blob-max-bytes` default 256 MiB, checked against the
  declaration *and* the running total so an under-declaring upload cannot
  stream past (`blob.rs:51,190-195,356-362`, `startup.rs:143-150`). **No
  delete** (the proto declares exactly Upload + Download,
  `blob.proto:18-33`; the only `remove_file` is temp cleanup, `blob.rs:422`),
  **no ownership records** (`StoredBlob` is `{digest, size}`,
  `blob.rs:125-129`), **no store-wide cap** — exactly as the §8 gap note says.
  The note's premises still hold: no DeleteSession exists anywhere in the
  proto (`control_plane.proto:31` is ArchiveSession), and a repeated digest is
  one blob (`blob.rs:404-409`, test `blob.rs:507`).

## §9 MCP hub (ADR-0004) — CONFORMS as built

Matches the ADR's W6a implementation note in every checked particular:

- Dual-era serving on pinned `rmcp =3.1.2` (`horde-mcp/Cargo.toml:15-20`), with
  the three measured configuration corrections including `legacy_session_mode`
  scoped per era (`server.rs:1-10,120-125`, pinned by test `server.rs:492`).
- Catalog as a fold over `McpServerConfigChanged` in the reserved stream,
  last-writer-wins (`horde-server/src/mcp.rs:1-12`); authored via
  `horde mcp add/list/remove` (`cli.rs:102-148`) with the name charset making
  `{server}__{tool}` splits unambiguous.
- Per-agent-type allowlists resolved at command-fold time — full `local`
  configs and visible hub names ride the spawn command
  (`commands.rs:343-346`); empty allowlist = every agent, non-empty fails
  closed for untyped agents.
- Permission chain gates every proxied call at the agent endpoint
  (`endpoint.rs:416-462`), whole path e2e with nothing faked
  (`horde-mcp/tests/hub_e2e.rs`).
- **Open items: all still open, unchanged** — worker-runtime wiring
  (`runtime.rs:1035`), secrets riding the journaled stdio `env` (warned in the
  CLI's own help text, `cli.rs:130-133`), no client-side mTLS dial for the hub
  link, multiplexing under concurrency (one shared `Upstream` per config,
  `hub/upstream.rs:1-15`; every e2e one-agent-at-a-time; `local` remains the
  pressure valve), `tools/list_changed` not emitted. The hub endpoint itself is
  stood up only by tests — `HubEndpoint` has no caller in `horde-server`.

The seed question "how is the multiplexing risk actually handled?" has a
precise answer: it is *contained* (per-call identity on the endpoint, `local`
valve, permission gate per call) but not *mitigated* — no per-agent upstream
session, no serialization, no concurrency test.

**Addendum, 2026-08-19 (W9b).** Three of the open items above are closed by
the ADR's 2026-08-19 amendment: hub identity travels as worker-forwarded
calls (`WorkerService.ListHubTools`/`CallHubTool`) over the worker's existing
mTLS channel — no client-side hub dial exists because no hub dial exists;
worker-runtime wiring is done (the spawn command's `local` configs are
spawned per agent and the hub link fetched, both registered per-agent in
`AgentProxies`, `horde-worker/src/endpoint.rs`); and `--serve` wires the hub
from the journaled catalog (`startup.rs` warms `crate::hub`), with
`HubEndpoint`'s unmounted state recorded as a decision. Attribution and
visibility are re-checked server-side per forwarded call
(`horde-server/src/hub.rs`), revocation severs the forwarded path with the
channel, and the whole wire is proven against real processes in
`horde-e2e`'s hub scenario. Secrets in journaled `env`, multiplexing under
concurrency, and `tools/list_changed` remain open.

## §10 Permissions — PARTIAL

- **Chain order: CONFORMS, verified in the function.** `chain::evaluate` runs
  deny → remembered-allow → allow → judge-consult → escalate
  (`permission/chain.rs:46-81`), with deny-beats-remembered explicitly
  reasoned and tested (`chain.rs:10-14,196-207`), tree-path routing through
  ancestry (`chain.rs:257-300`), first-matching-deny names the reason
  (`chain.rs:303`). Escalations carry tree path and context as designed
  (`driver.rs:387-411,469`); decisions can narrow (`updated_input` proven
  end-to-end necessary by W1c, `permission.proto:79-93`); answerable from any
  client (`horde approve`, `cli.rs:251-271`; decided-set race handling in the
  driver, `driver.rs:194`).
- **Judge: seam only.** `PolicyJudge` trait with abstention-escalates semantics
  and deliberately no default (`chain.rs:100-123`); a missing judge warns and
  escalates (`driver.rs:316-321`). **No production implementation exists** —
  the only impls are the scripted test judge (`driver.rs:832-850`) and testkit.
  No cheap-model judge anywhere.
- **The chain driver is not spawned by the production binary.**
  `permission::spawn` (`driver.rs:102-107`) is called only from
  `horde-server-testkit/src/lib.rs:110`; `startup::serve()`
  (`startup.rs:270-301`) never constructs modes, judge, or driver. In a
  deployed server every raised request waits for a human (the inbox and
  `approve` still work, since they are folds/RPCs) — layers 1–3 exist only in
  tests.
- **Modes: code-configured — the seed flag holds.** The registry is built by
  whoever builds the server (`modes.rs:10-12` says exactly this, naming §13 as
  the missing authoring layer); default is escalate-everything; unknown names
  escalate rather than guess (`modes.rs:104-117`).
- **`expires_at`: on the wire, honored by the wait, never set.**
  `PermissionRequest.expires_at` (`permission.proto:58`) resolves as an event
  not a stopwatch (`horde-mcp/src/permission.rs:17-19`), but the only producer
  writes `expires_at: None` (`translate.rs:179`) — requests never expire.
- **"Deterministic layers worker-local, fast" — DIVERGES.** The whole chain
  evaluates server-side over the log (`driver.rs`); the worker raises and
  waits. Nothing deterministic runs worker-locally. As-built this is a
  reasonable single-authority choice (the ADR-0003 settings hardening exists
  precisely to keep the harness from pre-empting it), but DESIGN's text says
  otherwise and no as-built note exists — flag for amendment.
- **Per-provider gating capability: CONFORMS** (see §4: per-call-kind
  enforcement vocabulary, codex refusing the blanket flag so placement cannot
  land high-trust exec on it).
- **Sandboxing: bootstrap phase as designed.** Harness sandboxes relied on;
  codex tier derived from the permission mode, fail-closed
  (`sandbox.rs:85-109`); orchestrator-level profiles DEFERRED-BY-DESIGN.

## §11 Clients — PARTIAL

- **CLI parity: PARTIAL — the hard rule has real holes.** The full verb list
  (`cli.rs:47-81`): hello, session new/list/get/archive, sync (--follow =
  headless attach), send (--note/--interrupt), interrupt, approve
  (--deny/--remember/--update-input), spawn, attach (--agent subtree scoping),
  blob stash/fetch, mcp add/list/remove, usage, worker enroll/revoke, enroll
  join/client/revoke-client; bare `horde` is the overview TUI. Present from
  DESIGN's named list: sessions (as `session list`), spawn, approve, attach.
  **Eight server RPCs have neither a CLI verb nor a client-library method**:
  the five workspace RPCs, `AddWorkspaceRepo`, `PrewarmWorkspace`,
  `ListWorkers` (`control_plane.proto:55-82`; no workspace method in
  `client.rs`). **Both spawn-shaped verbs send an empty spec** —
  `AgentSpec::default()` at `client.rs:295` (session create) and
  `client.rs:533` (spawn) — so no client can spawn a typed agent, pin a
  tier/model, choose a checkout policy, set a permission mode, or carry an
  initial assignment. The half that holds: every verb that exists is a server
  RPC, and the TUI genuinely drives the same client library
  (`tui.rs:169-193`).
- **TUI zoom levels: CONFORMS.** Overview ↔ attach ↔ back without restart
  (`horde-tui/src/app.rs:52-56,96,112,318,387-394`; tests `app.rs:676,789`;
  real-terminal `cli_suite/tui_pty.rs:127,155`). One gap: re-attach from the
  overview hardcodes `agent_id: None` (`app.rs:331-339`) — detaching from a
  `--agent` subagent view silently returns at session-root scope.
- **Subtree scoping + badge: prompt scoping CONFORMS, feed scoping does not.**
  Pending prompts scope by parent-chain walk with a counted ceiling
  (`app.rs:220-233,466-475`, `state.rs:210-228`; tests `app.rs:952,973`,
  `state.rs:901,932`), the "N waiting elsewhere" badge renders
  (`app.rs:237-239`, `view.rs:175-178`), tree paths stamp prompts
  (`view.rs:216-227`), all snapshot-pinned. But the *feed* stays unscoped —
  the same snapshot shows an out-of-subtree "needs you" line, because the TUI
  hardcodes `subtree_root: None` on its one stream (`client.rs:604`) even
  though the server honors subtree scoping (`control_plane.rs:511`) and the
  client library exposes it (`client.rs:963`). Scoping is a display filter on
  one panel, not a scope.
- **Global inbox: CONFORMS.** Escalations across every streamed session,
  answerable in place (`app.rs:190-204`, `view.rs:48-73`; e2e against a real
  server `cli_suite/tui_drive.rs:202,228`). Archived sessions are outside it
  unless attached (`tui.rs:202`).
- **Multi-attach free-for-all: CONFORMS, with an auditability gap.** No attach
  RPC, no client registry, no driver locks; every write is a log append and a
  lost race is routine (`tui.rs:104-106`, test `tui_drive.rs:318`). **But
  `Actor` attribution is never populated**: the proto's own rationale says
  "the user did it" is not specific enough under multi-attach
  (`id.proto:47-55`), and every producer passes `None`
  (`control_plane.rs:321,361,583,592,707,792`, `commands.rs:638,1147,1164,1217`,
  `driver.rs:445`) — every event in a multi-client session is anonymous, mTLS
  principals notwithstanding.
- **Steering vs interrupt: CONFORMS — proven end to end, the strongest-built
  §11 claim.** Turn-boundary messages ride a real queue drained exactly when
  `TurnEnded` brings the pump to Idle
  (`cli.rs:662-668` → `runtime.rs:447-472,1290-1294,1142-1143,1217-1221`);
  interrupt exists both as its own RPC/verb and as `--interrupt` delivery
  (`runtime.rs:686-710,1168-1178,468`); an interrupt delivery to an
  uninterruptible provider is *refused, not downgraded*
  (test `runtime.rs:1772`; truth table `runtime.rs:2023`; real-terminal
  `tui_drive.rs:287`). Codex parks the replacement until the old turn closes
  (`codex.rs:494-498`).
- **Stale-stream announcement (seed flag): the gap holds.** The server emits
  `SyncHeartbeat` (`control_plane.proto:215-223`); the client absorbs them
  explicitly — "they carry nothing to render" (`client.rs:656-658,734`) — and
  the TUI pump ends silently on stream end, its comment deferring to "the
  heartbeat gap will show soon enough" (`tui.rs:232-239`). No code measures the
  gap; a dead stream is indistinguishable from a quiet one in the UI.

## §12 Usage and observability — PARTIAL

- **Per-agent usage events, tree/account aggregation: CONFORMS — in the CLI.**
  Adapters emit per-turn deltas through one accumulator (§4); the server folds
  `QueryUsage` as a pure fold ("no usage table exists to disagree with the
  events", `control_plane.rs:1236-1239`) with groupings
  agent/session/tree-path/account/model and subtree scoping
  (`usage.rs:49-55,343-356`); rate windows are modeled losslessly across all
  three provider shapes with measured-vs-derived never blended
  (`horde-wire/src/usage.rs:1-14`, `usage.rs:10-19`). Surfaced via `horde
  usage --by agent|session|tree|account|model` with flag→wire→render mappings
  pinned (`cli.rs:283-332`; test `cli.rs:1007`). **The TUI has no usage
  view** — individual `UsageRecorded` feed lines only, aggregation
  deliberately deferred to the CLI (`state.rs:341-363`); "clients surface
  per-agent/per-tree usage" is half-met.
- **Threshold soft alerts: ABSENT.** No alert event kind, threshold config, or
  emitter anywhere in proto or crates; §12 says "this lands first" — it has
  not landed.
- **Hard budgets: DEFERRED-BY-DESIGN, shape present as promised.**
  `Budget.total_tokens` rides `AgentSpec` (`agent.proto:154-161`) unread, and
  `DeclaredQuota { window_ms, budget_tokens }` is the per-account quota model
  honestly documented as awaiting the config layer (`usage.rs:120-127`). The
  §15 budget-unit question is visible in the shape: `budget_tokens` is raw
  tokens, which is exactly the incomparability §15 says must be decided before
  activation.
- **OTLP tracing: PARTIAL — the export pipeline is real; the designed mapping
  is not.** What is instrumented is *RPC handler spans* (~20 ControlPlane, 5
  WorkerFacing, 2 blob, 2 log methods carry `#[instrument]`). **No span per
  agent, no parent-child linkage following spawn ancestry, no tool call as a
  span event** — §12's "agents = spans, tool calls = events, flame-graph for
  free" is not obtainable from what is exported. Export is initialized by the
  server and CLI binaries only (`bin/horde_server.rs:36`, `bin/horde.rs:25`);
  the worker — where agents run — exports nothing, having no binary. The
  pipeline itself is well built: loss counted not swallowed, self-watching
  suppressed, sampling real, proven against a real OTLP collector
  (`export.rs:18-90`, `otlp_smoke.rs:177-375`). **Plaintext gRPC only**:
  `opentelemetry-otlp` is pinned with `default-features = false, features =
  ["grpc-tonic", "trace"]` — no TLS feature, no `with_tls_config`
  (`horde-observe/Cargo.toml:19`, `pipeline.rs:131-139`) — so the seed flag
  holds, and spans stamped with the enrolled principal
  (`pipeline.rs:26`) travel cleartext, a live inconsistency with §3's
  mTLS-for-every-principal posture.

## §13 Configuration layering — ABSENT (the seed flag holds in full)

No layer of the four exists:

- **User-global (server DB):** no config table/stream, no authoring RPC or verb.
- **Workspace:** the definition carries repos/layout/unmanaged only — no default
  mode, no default agent type, no MCP allowlist field, no collision pins
  (`workspace.proto`).
- **Repo `.horde/`:** nothing reads a `.horde/` directory anywhere; the only
  same-named paths are the worker's `.horde-instance.pb` state file and
  `.horde-agents` checkout root (`workspace.rs:38-41`) — different things.
- **Prompt overlays:** the wire seam exists and is permanently empty —
  `SpawnAgentCommand.prompt_overlay` is set to `String::new()` at the only
  production construction site (`commands.rs:342`), and the worker would apply
  it if it ever arrived (`runtime.rs:1025-1026`).

The dependents that name §13 as their missing piece, in their own words:
permission modes (`modes.rs:10-12`), tier precedence (`tiers.rs:12-15`). This
is the single most-referenced unbuilt section in the tree.

## §14 Build sequencing — CONFORMS

The wave order of IMPLEMENTATION.md was followed through wave 6, and wave 7
(this audit among W7a–c) is in flight. The sequenced-later list is honestly
absent (raw harness, hard budgets, orchestrator sandbox, GUI/multi-user,
credential-less workers, ssh bootstrap — all verified ABSENT above). The
caveat: §14 calls this "ordering of activation, not scope" — as of `dddec01`
the un-activated set is not later-wave features but the *wiring* of built
wave-3–6 mechanisms (worker binary, endpoint, chain driver, hub, relay
mounting). DESIGN.md:7's "only activation is sequenced" reads differently when
activation is the missing layer.

## §15 Open questions — CONFORMS (all still open)

1. Worker tag taxonomy — still freeform (`PlacementConstraints.required_tags`),
   unrevisited. Open.
2. Hub multiplexing for stateful upstreams — open; see §9 (contained, not
   mitigated).
3. Multi-user — deferred; nothing contradicts it.
4. Workspace repo-set addition ceremony — open, and currently *below* the
   designed floor: no permission event is raised at all (§7).
5. Raw-endpoint harness mechanism — open; nothing built.
6. Budget unit for hard budgets — open; `Budget.total_tokens` is raw tokens,
   which is exactly the shape §15 says needs deciding before activation.

Plus the §8 blob-deletion gap, which the design flags in place and which this
audit confirms is still faithfully reflected by the code (no delete, no
ownership, no store-wide cap).

## §16 Development quality — CONFORMS (largely), with four TESTING.md drifts

- **Gauntlet:** ten gates, all `Blocking`, list and enforcement pinned by
  registry self-tests as TESTING.md §4 requires — fmt, clippy, test (nextest),
  coverage, crap, duplication (jscpd), machete, module-structure (cycles folded
  in, `baselines/module-cycles.txt`), proto-contract (buf lint + breaking,
  FILE mode, merge-base baseline), mutants
  (`registry.rs:13-105,138-168`). Pinned tool versions enforced with install
  hints.
- **TESTING.md drifts, all in the direction the docs should move:**
  1. Enforcement is *stricter* than TESTING.md's ladder table —
     coverage/crap/duplication/mutants are Blocking, justified in-code
     ("starts from zero", `registry.rs:163-166`); the ladder rungs exist
     unused (`gate.rs:17-24`). QUALITY_GATES.md already says this;
     TESTING.md §4 does not.
  2. **`coverage` blocks in name only**: `gates/coverage.rs:52-68` can return
     error/skipped/pass, never fail — "no threshold yet, recording the
     number". An Observe gate wearing a Blocking label.
  3. fmt/clippy/test *are* gauntlet rows, which TESTING.md §4 says they should
     not be ("CI proper, not gauntlet rows").
  4. **There is no CI at all** — no `.github/` exists. TESTING.md §5 specifies
     two workflows in detail; IMPLEMENTATION.md's ground rules ("local-only
     development: no PRs, no hosted CI — the gauntlet is the only enforcement
     mechanism") superseded it and TESTING.md §5 was never amended.
- **Pyramid:** unit tests extensive; property tests real but only two sites
  (`sync_equivalence.rs:81`, `tiers.rs:670`) of TESTING.md §2's four promised
  invariant families — reconciliation idempotence and capability attenuation
  have none (the latter has no mechanism to test, §5); integration suites are
  real components over loopback; **multi-process e2e is effectively absent** —
  the testkit serves the server in-process, and the PTY tests spawn the real
  `horde` binary against it (`tui_pty.rs:32`), so a real client process exists
  but a real server+worker pair never does (no worker binary); TUI insta
  snapshots over `TestBackend` (9, both zoom levels, inbox, scoping); adapter
  conformance genuinely symmetric — one suite against recorded real-harness
  transcripts *and* the FakeAdapter (`horde-testkit/src/conformance.rs`,
  `docs/spikes/transcripts/`); proto contract gate blocking; doc tests run by
  the test gate (`gates/test.rs:124-127`).
- **Missing at this base, in flight as W7a–c:** fault-injection as a named
  testkit facility (the severed-proxy partition test exists,
  `connection.rs:462-476`), Gherkin (zero `.feature` files, no cucumber
  dependency), e2e hardening.
- **Rules hold:** clippy denies unwrap/expect/panic workspace-wide
  (`clippy.toml`, TESTING.md §4); no mocking frameworks anywhere; fakes are
  real backends (§4).

## ADR verdicts

| ADR | Verdict | Notes |
|---|---|---|
| 0001 event log | CONFORMS | See §2. One INSERT; folds everywhere; worker journal's mutable tables need a scope ruling. |
| 0002 git backbone | CONFORMS (library) | See §8. `git http-backend` real; three routes proven; no bundles; **relay unmounted by any binary**. |
| 0003 claude stream-json | CONFORMS | See §4. Settings-ownership hardening verbatim with both-directions test; currently unreachable pending the endpoint wire. |
| 0004 central hub | CONFORMS | Implementation note accurate in every checked particular; all five open items still open, none regressed. |
| 0005 codex app-server | CONFORMS | Amended-ADR behavior implemented and process-tested; **the owed 0.147.0 exec/shell six-configuration re-probe has not happened** (flags re-checked 2026-08-13 only). |
| 0006 port forwards | DEFERRED-BY-DESIGN | Wave 8. No forward proto, events, or code exist — correctly nothing rather than something half. |

## Cross-subsystem contract disagreements

The semantic-collision class the merge protocol can miss — places where two
subsystems hold different beliefs about one shared contract:

1. **Idempotency-key scope vs. key minting.** The server dedupes on
   `(session_id, idempotency_key)` across *all* event kinds and drops
   duplicates silently (`schema.rs:66`; the cost is documented at
   `verbs.rs:205-216` — "it cost an afternoon here"). The MCP endpoint then
   mints the spawn key *from the task text* (`endpoint.rs:290`): two spawns
   with the same task text in one session silently become one. One subsystem
   treats the key as a retry token; another as a content hash. Fix the minting
   (unique ids), and consider making the dedupe kind-aware or loud.
2. **"Children start from the parent's committed HEAD."** The endpoint's tool
   description and §5's agent mental model promise it; the checkout code
   branches worktrees from the *instance* HEAD (`workspace.rs:325-337`). The
   prompt tells agents their commits ride; the mechanism disagrees.
3. **`AgentSpec` on the wire vs. `AgentSpec::default()` from every caller.**
   The proto promises tier, workspace, checkout policy, permission mode,
   budget, initial assignment (`agent.proto:160-182`); both real callers send
   the default (`client.rs:532`). Live consequence: hub allowlists key on
   `agent_type`, and "non-empty fails closed for untyped agents" — so **any
   hub server with an allowlist is invisible to every agent spawnable today**,
   because no path sets a type.
4. **Version gate: comment vs. code.** `shared.rs:94` claims refusal below
   `MIN_SUPPORTED_SEMVER`; no comparison exists (§2). An auditor reading docs
   would pass a claim the wire cannot enforce.
5. **Revocation reach: unary vs. streams.** `transport.rs:6-11` says a revoked
   certificate stops working at its next call; true per-request, false for the
   long-lived streams that are a worker's actual life (§3).
6. **Heartbeats: emitted, absorbed, presumed rendered.** The sync stream emits
   them so quiet ≠ dead (`sync.rs`); the client swallows them
   (`client.rs:658`); the TUI comments that "the heartbeat gap will show soon
   enough" (`tui.rs:235`) — no component owns showing it.
7. **Two revival-identity mechanisms.** `RevivalIdentity` +
   `is_resumable_under()` built for the session-id/account pairing
   (`identity.rs:24-63`, never called in production); the worker journal's
   `RevivableAgent` actually carries the pairing (`journal.rs:129-140`). Two
   types for one contract; the guard the type exists for is never enforced.
8. **Number spaces.** Worker `journal_sequence` vs. session `sequence` diverge
   the moment a worker serves two sessions; feeding one to the other yields a
   plausible wrong offset. Handled today by identity-matching instead of
   offsets (`horde-mcp/src/permission.rs:31-46`) — a trap held at bay by
   discipline, worth a wire-type distinction eventually.
9. **"Commit, then share" vs. the stamp path.** ADR-0002 and §8 promise a
   helpful refusal on dirty trees; `require_committed` implements the exact
   message and `ShareBroker::stamp` never calls it — the sharing layer
   believes dirty trees are refused, the stamping layer silently omits them
   (`transport.rs:127-138` vs. `share.rs:131-164`).
10. **Actor attribution: demanded by the proto, supplied by nobody.**
    `Actor`'s own doc comment says multi-attach makes "the user did it" too
    vague to audit (`id.proto:47-55`); every event producer passes `None`
    (§11). The identity layer (§3) mints exactly the principals the event
    layer never records.
11. **Subtree scoping: server offers, TUI declines.** `SyncEvents` honors
    `subtree_root` (`control_plane.rs:511`) and the client library exposes it
    (`client.rs:963`); the TUI hardcodes `None` and filters one panel
    client-side (`client.rs:604`) — two components implementing the same
    feature at different layers, only one of which covers the feed.
12. **Dead wire fields with live comments.**
    `CreateSessionRequest.workspace_definition_id` ("this is set either way")
    is never read (`control_plane.proto:119`); `MaterializationProgress`
    ("streamed while materializing") is produced by nothing
    (`workspace.proto:88-98`); `WorkspaceDefinition.unmanaged_paths`
    round-trips and is never consulted (`workspace.proto:32-34`);
    `control_plane.proto:117-118` documents a `horde new` verb that does not
    exist. Each is a proto contract one side honors and the other ignores.

## Prioritized fix list

### (a) Implementation gaps worth closing this wave

1. **Stand up the worker and its endpoint — the one wire that unlocks §5.**
   A `horde-worker` role binary (join/dial/reconnect loop over
   `Connection::run`, durable `Journal::open`, adapters registered,
   `stamping_sink()` installed), per-agent MCP endpoint wiring
   (`runtime.rs:1035`: token minting, worker `JournalSink`, control-plane
   channel — ADR-0004's own list), and `--with-local-worker` on the server.
   Nearly every §5 verdict above is downstream of this.
2. **Fix `spawn_agent`'s parameter routing** (`endpoint.rs:290`): task text →
   initial assignment on a real `AgentSpec`; idempotency key minted from a
   unique id. Closes collision #1's sharp edge and makes agent-initiated
   spawns carry a task at all.
3. **Authorize mint/revoke** (`control_plane.rs:1156,1192`): read the
   `PeerIdentity` the interceptor already injects; require role Client (the
   test at `server_suite/enrollment.rs:445` is written to flip).
4. **Make revocation reach live streams**: on revoke, drop the principal's
   registry entry and abort its streams (or re-check the deny-list in the
   pump loops). Add the missing revoke-with-live-stream test.
5. **Spawn the permission chain driver in `startup::serve()`** with an
   escalate-everything default registry, so layers 1–2 exist in a deployed
   server and mode registration has a production seam. (The judge can stay a
   seam; escalation-on-absent-judge is already correct.)
6. **Enforce the version gate or delete its claim**: compare Hello semver
   against `MIN_SUPPORTED_SEMVER` at register/hello, and fix the two
   misleading assertions (`shared.rs:94` comment,
   `testkit/tests/secure.rs:155` message).
7. **Give archive a producer**: at minimum, session-archive folds into
   `ArchiveAgentCommand` for its agents; the reaper proper can follow.
8. **Branch child worktrees from the parent's committed HEAD** (or stamp the
   spawn payload with the parent's HEAD and check it out) —
   `workspace.rs:325-337`.
9. **Surface stream staleness in the TUI**: track last-heartbeat and render a
   stale badge; the server half already exists (15s period, `sync.rs:34`).
10. **Mount the relay and hub endpoint in `--serve`** (flags for state dir /
    bind), so §8/§9 stop being test-only capabilities.
11. **Call `require_committed` on the stamp path** (or stamp dirty state as an
    explicit fact) — today an agent's uncommitted work is silently omitted
    from its result payload (`share.rs:131-164`).
12. **Let clients send a real `AgentSpec`**: expose agent type, tier,
    workspace, checkout policy, permission mode, and the initial assignment
    through `client.rs`/CLI spawn and session-new (`client.rs:295,533`), and
    add the missing workspace/prewarm/list-workers verbs (the §11 parity
    rule). Without a settable `agent_type`, hub allowlists fail closed for
    every agent (collision 3).
13. **Populate `Actor` from the interceptor's `PeerIdentity`** on every
    client-caused append — the audit trail multi-attach requires (§11).
14. **Scope the TUI's stream** with `subtree_root` when attached with
    `--agent`, and preserve agent scope across detach/re-attach
    (`client.rs:604`, `app.rs:331-339`).
15. **Feed the advertisement**: wire `warm_instances()` into the worker's
    register/report path and give tags/remotes a config source — or matching
    on them (`scheduler.rs:332-388`) stays a filter nothing can pass.
16. **Give `coverage` a threshold or relabel it Observe**
    (`gates/coverage.rs:52-68`) — a Blocking gate that cannot fail miscounts
    the gauntlet's promises.

### (b) DESIGN.md text to amend to match justified as-built reality

1. **§4 Codex bullet**: replace "ACP where supported (Codex, etc.)" with
   "Codex via app-server JSON-RPC (ADR-0005); ACP rejected there."
2. **§10 layer siting**: "worker-local, fast" → the deterministic layers
   evaluate on the server in the chain driver; the worker owns only the
   harness-side settings hardening (ADR-0003). Add the as-built note.
3. **§5 lifecycle**: add QUEUED, STARTING, FAILED to the state list (they are
   real and produced); note that result↔assignment correlation is by agent,
   not assignment id, if that as-built choice is kept.
4. **§2 partition sentence**: say whether "keeps its agents running" means
   process-survival or journaled revival; the code implements the latter.
5. **§3 additions**: record the bootstrap-code-on-stdout decision (already
   flagged in code), the any-principal-may-mint status (until fix a3 lands),
   the re-enrollment rotation story and cert lifetimes, and that revocation
   is next-call for unary / reconnect for streams (until fix a4 lands).
6. **§5 native-tool suppression**: mark "suppressed and substituted" as not
   yet built, or specify the mechanism (disallowed-tools vs. strict MCP
   config) so the builder has a target.
7. **§8 relay**: note the bootstrap plaintext listener is loopback-only with
   ambient principal `horde`, and that the mTLS door is a separate listener
   sharing the CA/revocation material rather than literally the main channel —
   per the as-built blob paragraph's precedent.
8. **§12**: mark threshold alerts as not yet landed (the text says "lands
   first"), note OTLP export is currently plaintext gRPC, and either commit to
   the agents-as-spans mapping as future work or soften "flame-graph views for
   free".
9. **ADR-0001**: state whether the worker journal's mutable
   `agent`/`applied_command`/`meta` tables are inside or outside the
   "side channels are bugs" rule.
10. **§6**: "agents sharing a mutable workspace instance colocate" →
    "placement prefers warm workers; constraints always win"; document the
    1-hour default queue timeout and that expiry is evaluated on placement
    passes, not at the deadline.
11. **TESTING.md**: delete or rewrite §5 (CI workflows) to match
    IMPLEMENTATION.md's local-only ground rule; update §4's ladder table to
    the all-blocking reality QUALITY_GATES.md already records; either build
    the two missing §2 property families or stop listing them.
12. **Proto comments**: fix the dead-field comments collision 12 lists
    (`workspace_definition_id` "set either way", `MaterializationProgress`
    "streamed", the `horde new` reference) — comments describing behavior are
    contracts, and these three describe behavior that does not exist.

### (c) Deferred items, with their wave

| Item | Where deferred | Status |
|---|---|---|
| Port forwards (expose/inject) | ADR-0006, wave 8 | Nothing built, correctly. |
| ssh worker bootstrap + harness install recipes | §3/§14 "sequenced later" | Absent, correctly. |
| Interactive-auth relay (PTY + port-forward) | §4, needs ADR-0006 machinery | Absent. |
| Raw/self-hosted model harness | §4/§14 | Absent; §15 question open. |
| Hard budgets | §12/§14 | Proto shape present, unread — as designed. Budget unit (§15) undecided. |
| Orchestrator-level sandbox profiles | §10/§14 | Absent; harness sandboxes carry it (bootstrap phase as designed). |
| Credential-less workers (relay-only git) | §8/§14 | Bootstrap phase (ambient credentials) is what exists. |
| Multi-user server | §1/§15 | Deferred; single-user assumptions noted where load-bearing (§3 mint authz, §9 journaled env secrets). |
| Gherkin, fault-injection suite, e2e hardening | §16, wave 7 | In flight as W7a–c alongside this audit. |
| Codex 0.147.0 exec/shell re-probe | ADR-0005 amendment | Still owed; flags re-checked 2026-08-13, six-configuration probe not re-run. |
