# Horde — Design

A multi-provider agent orchestrator. One control plane through which *all* agents flow — the ones you drive directly and the ones agents spawn themselves — with session trees, cross-machine placement, subscription-based provider accounts, and attach-from-anywhere clients.

Status: design settled 2026-08-09 after initial brainstorm + grill session; built in waves since (IMPLEMENTATION.md), measured against this document in docs/CONFORMANCE.md.

**This document defines the end state.** Nothing in it is a "v1 feature" versus a "someday feature" — the shape of every mechanism is designed in from the start, and only *activation* is sequenced (§14). When a section says "bootstrap phase," that is a stepping stone inside the designed shape, not a scope cut to be revisited.

---

## 1. Goals and constraints

- **Multiple providers, via subscription, not API keys.** Claude is a first-class citizen and also the most constrained; the design must work within what Claude Code supports. Multiple accounts per provider (e.g. two Claude Code accounts) must coexist.
- **Agents orchestrating agents is the product.** Every agent — top-level or spawned — flows through the same orchestrator. Spawn ancestry is tracked; agents communicate with their parent and children. Agents decide what agents they need and create them.
- **Run anywhere.** Server, clients, and workers can live on different machines. Multiple clients attach to one server (tmux-pane workflow, later mobile). Sessions can span workers.
- **Attachable.** Any session/agent can be attached to at any time, though normally you shouldn't need to.
- **Usage tracked** per agent and attributable across a tree.
- **Resilient to spotty networks; performant.**
- Single-user per server for now. Multi-user sharing is deferred, not precluded.

## 2. Architecture: three roles, three binaries

| Role | Binary | Responsibilities |
|---|---|---|
| **Server** | `horde-server` | Control plane. Session/agent state, event log, scheduler, git relay, blob store, MCP hub, permission routing, usage accounting, auth CA. SQLite storage. |
| **Worker** | `horde-worker` | Agent process supervisor. Runs provider harnesses as child processes, materializes workspace instances, applies deterministic permission layers, journals events. Dials the server outbound (NAT-friendly). |
| **Client** | `horde` | Ratatui TUI + CLI. Top-level session views, per-session attach, enrollment, config authoring. Later: GUI/mobile. |

All Rust. Shipped as three separate binaries — combining them invites designs that break once they're apart. Convenience affordance: `horde up` (ADR-0007) starts a real `horde-server` and a real `horde-worker` as separate processes that connect over gRPC on localhost (the `horde-server --with-local-worker` flag this replaced was never built). Same wire path as remote; no in-process shortcut exists.

### Transport and resilience

gRPC (tonic) everywhere. Resilience comes from state, not transport: every session has an **append-only event log** on the server (agent output chunks, spawns, messages, permission requests, usage records — all events with sequence numbers). Client and worker streams are "sync me from offset N," so reconnect, attach, replay/scrollback, crash recovery, and usage attribution are one mechanism. See ADR-0001.

**Partition behavior:** a worker that loses the server keeps its agents running and journals events locally, syncing on reconnect. The worker journal is SQLite in WAL mode with `synchronous=NORMAL`, and agent output is coalesced (deltas the harness has already produced merge per turn and channel) and chunked at 64 KiB before it is journaled. "Keeps running" means the live process rides out the partition; continuity across a worker *restart* is **journaled revival**, not process survival — the journal holds each agent's spawn command and provider session id, and the worker rebuilds the agent by resuming that provider session. Anything that *requires* the server (permission escalation, cross-worker spawn, relay push) blocks only the agent that needs it.

**Liveness is a lease (ADR-0011).** HTTP/2 keepalive runs on both ends, and the server notices a dead command stream through the transport closing it, not the next failed send. A worker that leaves gets one **worker-lost lease** (default 120 s, `--worker-lease`) to come back: past it, the agents it never started are re-placed and the ones it had started are marked `LOST` and `FAILED` in the log — their context lived on that worker, so they are never silently re-spawned elsewhere. A restarted server treats every worker the log places agents on as away until it dials in (boot reconcile), so nothing stays `Placed` forever on a machine that never returns. A late journal from an agent's former placement lands as history (output, usage) or as a visible `StaleJournalEvent` in the agent's session — never dropped, never overwriting what the current placement says. Command streams resume by the causing event's log position when the worker knows one, because a newer server's fold may number the same history differently. A lost worker redials on jittered exponential backoff (0.5 s doubling to 10 s, reset by a registration), and both ends of the worker spine set a deliberate 16 MiB message limit; the worker's push batches are bounded by bytes well inside it.

**A worker accepts commands into a journaled inbox (ADR-0012).** The ack means *accepted*: each command is written to the worker's inbox in the transaction that acks it, then run on a per-agent lane — in order for that agent, without blocking any other — and completed in a second transaction. A crash in between replays the inbox at the next start. A command that cannot be applied is journaled against its agent as a diagnostic and never stops the worker; only journal failures are fatal. A message stays in the inbox until the harness has actually taken it, so a harness that died holding steering, or died idle between turns (found by a broken pipe), is revived and handed it — never double-delivered alongside ADR-0011's server-side buffer, because forgetting an incarnation drops what it was handed.

**The server reads a live projection (ADR-0013).** Session state stays a pure function of the log, but it is folded once rather than per request. An in-memory projection, rebuilt from the log at boot, catches up on newly appended rows before each read, using the same fold a cold replay uses. Reads go through a pool of query-only WAL connections, so they never queue behind the writer. A worker's journal batch is appended in one transaction, with the same ordering and dedupe as one append per event. `ListSessions` can filter by exact session name.

### Version skew

Hello handshake on every connection: semver + capability flags. Server enforces a minimum supported version per role; above the minimum, features gate on advertised flags rather than version comparisons. Protobuf evolution discipline (no field reuse, additive changes) carries the rest.

## 3. Identity and security

The server is remote-code-execution-as-a-service by design; the trust boundary is real.

- **mTLS for every principal** (workers *and* clients), certs issued by the server's internal CA.
- **Enrollment (ADR-0007):** `horde worker enroll <name>` on a trusted client prints one short-lived join token, `horde://join/<host:port>/<secret>.<fingerprint>`. The token carries the enrollment listener, the one-time secret, and the server cert fingerprint. On the new machine, `horde-worker --join <token>` (or `--join-file`, or `HORDE_JOIN_TOKEN`) verifies the server against the fingerprint, generates a keypair locally, submits a CSR, and the server signs it. Private keys never travel. Clients enroll identically with `horde enroll join <token>`, which refuses to replace an existing identity without `--force` (and then backs the old one up). The join stores the server's endpoint beside the credential, so later invocations need no flag. There is no default server: with no flag, env var, or stored endpoint, both refuse rather than guess: the CLI points at `enroll join` or `horde up`, the worker at `--server`, `HORDE_SERVER`, or `--join <token>`.
- **Quick start, one machine:** `horde up --adapter claude` starts a server (state under `~/.horde`), enrolls the local client through the server's user-only bootstrap-token file, mints and joins a local worker, and prints one ready line. A second machine joins with a token minted through a name it can dial: `horde up --san box.lan --bind 0.0.0.0:7777 --enrollment-bind 0.0.0.0:7778` on the server, then `horde worker enroll gpu-box --endpoint https://box.lan:7777` there and `horde-worker --join-file <token-file>` on the new machine.
- **State directories:** each daemon keeps its state under `~/.horde` (`server/`, `worker/`, `id/`), owner-only, and holds an exclusive lock on it while running. `horde-server` needs one mode — `--serve`, `--check` (the health check), or `--remint` (below) — and never writes to the working directory.
- **Revocation:** per-cert denylist by serial (`horde worker revoke <name>`), journaled as events and seeded from the log at startup. Reach (wave-7 fix): unary calls are checked per request, and revoking a principal also severs its live streams — a revoked worker does not keep pumping commands until it happens to reconnect.
- **Minting and revoking require a Client-role principal** (wave-7 fix): enrollment alone does not confer them, so a worker cert can neither mint codes nor revoke peers.
- **Every RPC has a role, and a worker is its certificate** (ADR-0009): one table decides, per gRPC method, whether clients, workers, or either may call it — the worker spine is workers only, the control plane clients only except the agent conduit (spawn, send, sync, hello) a worker drives for its own placed agents — and a method without a row is refused. Worker-spine handlers take the worker from the certificate and refuse a body naming another. Ingest is origin-aware: a worker may journal only observations about agents placed on it, and a refused entry is dead-lettered in its place so the journal never wedges. A worker's `SyncEvents` reads only sessions it holds agents in, never all sessions (ADR-0011).
- **Journal reset** (ADR-0011): a worker whose journal regressed (a fresh state directory after a disk loss) is refused at registration, and `horde worker reset <name>` is the explicit recovery — its fresh journal is numbered above the old one's floor, its command stream starts over, and the agents it held are declared lost.
- **Bootstrap trust root:** at `--serve` startup, exactly when no client holds an active cert, the server mints a first client join token, prints it to stdout (`bootstrap-client-token horde://join/…`), and writes it to a user-read-only `bootstrap-token` file in its state directory, removed once a client has enrolled. Whoever reads either already owns the disk the CA key lives on — the same trust, no new surface. An expired bootstrap token is re-minted on the running server by `horde-server --remint` on the same state directory (SIGHUP to the server does the same).
- **Rotation:** lifetimes are CA 20y, listener 2y, principal 1y. No renewal RPC and no expiry warning yet; rotation is manual re-enrollment under the same name, which the roster supports by keeping every serial a name has held.
- No non-localhost binding before auth exists — auth ships with the first networked build, not as a follow-up.

### Worker bootstrap over ssh (sequenced later)

`horde worker up --ssh myles@host` composes existing primitives — it is client-side orchestration, not new protocol:

1. Client mints a one-time enrollment code from the server.
2. Client shells out to **system ssh** (so `~/.ssh/config`, keys, agent, and password prompts all behave normally), copies or downloads the right `horde-worker` binary for the target's os/arch, and runs `horde-worker --join-file <token-file>` with the minted join token (ADR-0007) copied there rather than put on the remote argv.
3. Installs a persistence unit (launchd/systemd user unit) so the worker survives reboots; `--foreground` for throwaway use.

**Harness installation:** adapters carry an install recipe (brew/npm/curl per platform). A worker missing a harness advertises that as an installable capability; `horde worker install <worker> claude` (or `--install claude,codex` at bootstrap) runs the recipe worker-side. Note install ≠ auth: after install, provider login still runs through the interactive-auth relay (§4).

## 4. Providers and adapters

A per-provider **adapter** trait: `spawn / send / stream / interrupt / resume`, plus capability flags (notably `resumable`).

- **Claude (first-class):** the worker drives `claude --input-format stream-json --output-format stream-json` directly as a child process — no Node sidecar. Session revival via `--resume`. Permission decisions route through `--permission-prompt-tool` pointed at the horde MCP endpoint. If stream-json proves too lossy, fallback is a thin Agent SDK shim per agent (contained, not preferred). See ADR-0003. **This is the de-risking spike to run first.**
- **Codex:** driven via its app-server JSON-RPC interface (ADR-0005); ACP was evaluated there and rejected. Other harnesses land behind the same trait.
- **Raw/self-hosted models (Ollama etc.):** part of the end state, sequenced late. A bare endpoint brings no tool loop, so raw endpoints require a harness Horde supplies (built-in minimal loop vs. wrapped OSS harness is an open question); the adapter trait is the seam either slots into. Until then, harness-backed providers only.

### Provider accounts

- Multiple accounts per provider = isolated config/credential dirs per account (e.g. `CLAUDE_CONFIG_DIR`), a **worker-level resource** advertised as a capability.
- Scheduler can rotate across accounts on rate-limit headroom.
- **Getting accounts onto workers:** generic interactive-auth relay built from two primitives — a **remote PTY relay** (client terminal attached to e.g. `claude login` running on the worker, over the mTLS channel) and an optional **port-forward relay** (tunnel the worker's localhost OAuth-callback port to the client during auth). Together these cover nearly any CLI login flow with zero per-provider code. Manual ssh + login on the machine remains the escape hatch. As built: `horde worker login <worker> <provider> [--account <name>]` (the account selects the worker-owned profile directory `harness/<provider>/accounts/<name>` agents bound to that account run under, ADR-0010; omitted, the account-less default); adapters carry a **login recipe** (argv + browser-facing callback port, plus an optional split listener port — separable because the relay itself is a port-forward); a login is **ephemeral intent** — journaled as `WorkerLoginStarted`/`WorkerLoginEnded` in a reserved stream for audit, but its liveness is the relay's in-memory pairing and nothing re-realizes after a partition (a PTY cannot be replayed into existence; the operator reruns the verb). The callback rides the wave-8 expose machinery as an unjournaled intent merged into the worker's pushed set, bound port-exact client-side (a redirect URI admits no fallback). One login per worker+provider at a time; only Client principals may open one; the shipped claude/codex recipe argv/ports are provisional pending live verification.

## 5. Agent model

**One agent primitive.** No bounded/unbounded split.

- An agent is spawned with a task, works, and emits a **result**; then it goes **idle with context retained** — revivable by just sending it another message (requires adapter `resumable`; non-resumable providers get honestly-terminal agents).
- **Lifecycle states:** queued → starting → running → waiting-on-child / waiting-on-human → idle → archived; failed is reachable from any active state. Context dies at archive, not at result. **Archive is terminal** (ADR-0011): archiving a session journals each agent's own `ARCHIVED` with the state it left, placement never places an archived agent, and messages to — or children of — an archived agent are refused with a named `FAILED_PRECONDITION`. An agent lost with its worker is refused the same way.
- **Cleanup:** reaper policy (default: archive after 60 min idle with no pending assignment; archived metadata retained until session deletion) plus explicit parent cleanup ("done with these children"). No keep-warm flag.
- **Capabilities attenuate down the tree only.** A child asking for more than its parent has is a human-approval event, never silent.

### Messaging: assignments and notes

Two message kinds, sender declares intent (one bit, no taxonomy):

- **`assign`** — expects exactly one **result**: `status (success | partial | blocked) + summary + payload`. Spawning carries an implicit first assignment. A follow-up assignment ("you missed X") yields a new result.
- **`note`** — chat/context/status. No result owed. Ongoing agents (monitors) live on notes.

`wait_for_agent` waits on an assignment's result. As built, correlation is by **agent**, not assignment id (results carry no assignment id yet): the wait answers with the agent's result reported *after the latest assignment sent to it* — a result already in history answers at once unless a newer assignment has superseded it, in which case the wait holds for the new one. The wait reads the log from the endpoint's shared cursor and redials it from that cursor on a transport failure, so a wait (or a pending approval) survives a worker↔server partition. (CC precedent: Task subagents are strict result-per-call; teammates are freeform + shared task list. This is the synthesis.)

### Sharing is implicit in the verbs (payloads)

Agents never think about transport. A **payload** = committed git ref and/or blob **attachments**, riding on the three verbs:

- **`spawn_agent`** — same-workspace children start from the parent's committed HEAD by default.
- **result** — the worker **auto-detects** the agent's branch/commits and stamps them into the result. Optional attachments (screenshots, builds, logs).
- **`send_message`** (assign or note) — may carry a ref and/or attachments.

Design principle: **prefer the orchestrator observing what happened over agents declaring what happened.** Observed state doesn't drift.

The entire agent-facing mental model fits in a paragraph of system prompt: *you have your own worktree; commit as you go; agents you spawn start from your latest commit; your commits ride your result automatically; attach files when the content isn't code.*

### Model selection for spawned agents

Agents request a **tier**, not a brand. Tiers are user-defined config keys (shipped defaults: `fast` / `standard` / `deep`; define as many as you like), each mapping to a set of candidate (provider, model) pairs — multiplied by available accounts into concrete options. The tier itself is resolved through the standard precedence chain:

1. **Spawn args** — explicit tier, or a concrete provider/model (concrete requests are subject to permission policy, so "always use my provider" bias is containable)
2. **Agent-type definition** (repo `.horde/`: "the reviewer runs `deep`")
3. **Session defaults** — set at `horde new` ("everything in this session runs on codex")
4. **Workspace / user-global tier mappings**

Unspecified = inherit the spawner's tier. Within a tier, a per-tier **selection strategy** picks the concrete option: `headroom` (default — most quota headroom), `ordered` (first listed candidate with headroom above a floor), or `weighted` (preference weights combined with headroom). Headroom itself is adapter-reported rate-limit state where the provider exposes it (Claude Code does), otherwise Horde's own metering against a per-account declared quota model (window length + budget) — every token flows through Horde, so it is never blind. The binding is made once, at spawn, and held for the agent's life (revival resumes the same provider session); rebalancing happens across new spawns, never by migrating live agents. Rationale: letting spawners name concrete models invites provider self-bias and couples prompts to today's model names; tiers keep agent definitions portable while explicit override + session pinning preserve full control.

### Agent-facing surface: one MCP endpoint

Every agent gets exactly one horde-controlled MCP endpoint carrying:

1. Orchestrator verbs: `spawn_agent`, `send_message` (assign/note), `report_result`, `wait_for_agent`, `approve`, and the port-forward verbs `expose`/`inject` (ADR-0006). A list/query verb is not built yet. Permission rules name these as `horde/<tool>` (ADR-0004 amendment).
2. Proxied hub MCP tools (§9).
3. Worker-local MCP tools (config-flagged `local`).

Native subagent tools (CC Task, etc.) are suppressed and substituted — not yet built; the target mechanism is the harness's own tool-disable flag (CC: `--disallowedTools Task`), applied by the worker at spawn. Until it lands, native Task remains reachable. Permission prompts also ride this endpoint (Claude: `--permission-prompt-tool`).

## 6. Sessions, placement, scheduling

- **Session = one conversation tree rooted at one top-level agent.** No "project" entity. Sessions reference a workspace; grouping by workspace is a TUI query, not data model.
- **The agent, not the session, is the unit of placement.** Sessions span workers freely.
- **Workspace locality is a placement preference:** the scheduler ranks workers with a warm instance (present or already materializing) first, then the parent's worker, then load — and constraints always win; warmth never overrides a failed constraint. Independent agents go anywhere.
- Workers advertise **capabilities**: freeform tags (`gpu`, `macos`) + auto-detected facts (os/arch) + provider accounts + reachable git remotes + warm workspace instances + headroom. Spawn requests carry constraints; scheduler matches, preferring the parent's worker when nothing forces otherwise.
- **No matching worker:** spawn never blocks the caller — returns a handle with status `queued` and the reason ("no online worker with tag `gpu`; `gpu-box` matches but is offline"). Spawner may wait, relax, cancel, or escalate. Default queue timeout: 1 hour. Expiry is an event the spawner hears — evaluated when a placement pass runs, not at the deadline itself.
- **Messages to unplaced agents wait** (ADR-0011): a message sent before its agent is placed, or while a not-yet-started agent waits to be re-placed, is buffered by the command fold — durable because it is a fold over the log — and delivered right behind the spawn that places it, again on every re-placement, until the agent is seen running.
- **Retries are idempotent:** a spawn or send retried under the same idempotency key answers with the ids the first attempt stored, and creates nothing.
- Explicit `prewarm` command materializes a workspace instance on a chosen worker ahead of need; instantiation progress streams as session events.

## 7. Workspaces

Split: **definition** (portable) vs **instance** (per-worker materialization). See also ADR-0002. Freshness at spawn, lazy checkout, shared branch names, two-tier cleanup and the finished `unmanaged`/progress/report pieces: ADR-0014.

- **Definition:** name + 1..N repos (remote URL, default branch) + layout + optional `unmanaged` paths. Lives in server SQLite, managed by CLI (`horde workspace create/add-repo/update/...`). Repo set is fixed at definition; additions are explicit (a permission-style event). Definitions are deliberate: nothing auto-registers one from cwd; bare `horde` may offer to create one on a TTY (ADR-0014).
- **Instance:** checkouts on a worker's disk. Worker-local state file is the truth; reported to the server on connect (server treats it as cache; reconciliation handles wiped disks / out-of-band deletion). Created by server command at placement time; never self-initiated.
- **Per-agent checkout policy:** `shared` (parent's instance — rare but real), `worktree` (default for mutating agents; one worktree per repo composed under an agent-private root), `fresh-clone` (full isolation).
- **`unmanaged` paths** (datasets, weights, build caches): per-instance, never synced, not covered by payloads. Cross-worker artifact passing is the blob store's job, not git's.
- Single vs multi repo is the degenerate case, not a different model.

## 8. Git backbone and relay

Committed to git deliberately (ADR-0002). The unit of sharing is the commit; syncing dirty working trees across workers is permanently out of scope ("commit, then share" — the tool errors helpfully on uncommitted changes).

**Server git relay:** bare mirrors per registered repo (lazy — mirrored on first fetch), served over git smart-HTTP (`git http-backend`) behind mTLS. As built: the mTLS door is a separate listener sharing the main listener's CA and revocation material — the same trust, not literally the same channel — and a plaintext bootstrap listener exists for loopback only, serving with the ambient principal `horde`. The registry tracks the workspace catalog exactly: a deleted definition's repos stop being served, while their mirrors stay on disk for commits the log may still name (ADR-0011).

- Transport resolution per sharing pair, invisible to agents: same clone/worktrees → ref checkout only; same worker → fetch from peer path; cross-worker → push/fetch through the relay.
- Relay doubles as clone cache and **credential chokepoint**. End state: only the server holds external git credentials; workers' only remote is the relay. Bootstrap phase: workers use ambient machine credentials (ssh-agent, `gh auth`) and advertise reachable remotes as capabilities.
- Git bundles were considered and dropped entirely (ADR-0002).

**Blob store (server):** content-addressed, for non-code payload attachments (screenshots, binaries, logs, checkpoints). Worker uploads on send, receiver downloads lazily. Lifetime tied to session; GC'd at session deletion; server-configurable size cap. As built (wave 6): SHA-256-addressed files under the state dir, transfer over the mTLS listener behind the enrolled door, integrity verified on upload and on every read; attachments at or under a 64 KiB inline threshold ride in the event itself and the log refuses anything larger inline — events carry blob references and hashes, never bulk (ADR-0001). The size cap shipped as a **per-blob** cap (`--blob-max-bytes`, default 256 MiB).

> **Gap (flagged, not decided): blob deletion.** "GC'd at session deletion" presumes a session-deletion operation and per-session blob ownership records; neither exists — sessions archive (retaining the log), the store records no ownership a collector could act on, and content addressing means one blob may serve many sessions. Until this section decides what may be deleted and when (ownership records, refcounts, or lifetime tied to log retention), the store exposes **no delete at all**: an unreferenced blob wastes disk, but a deleted blob a logged event still references would break replay. The store-wide (as opposed to per-blob) size cap is part of the same gap, since enforcing one requires deciding what to evict.

## 9. MCP hub

**Transport/spec policy.** Horde-as-MCP-server (the per-agent endpoint, permission prompting, hub-proxied tools) serves **dual-revision**: an `initialize` request selects the handshake-style legacy flow, a `_meta`-versioned request the stateless **2026-07-28** one. That discriminator is the spec's own rule for a dual-era server, and 2026-07-28 — no initialize handshake, no protocol sessions, cross-call state as explicit server-minted handles — is exactly our per-agent-token shape.

**The legacy era is built first, and it carries all current traffic.** The audit (`docs/research/mcp-2026-audit.md`, 2026-08-11) found that no harness client we ship an adapter for can speak 2026-07-28 yet: Claude Code opens with `initialize` at `2025-11-25` over both HTTP and stdio, Codex pins `2025-06-18`, and neither falls forward — against a modern-only endpoint Claude Code takes the `UnsupportedProtocolVersionError` and stops, so **a 2026-07-28-only endpoint would ship zero tools to every agent, silently and with no error event**. The legacy half must therefore reach back to `2025-06-18`. The modern era comes free from rmcp's dual-era serving on one endpoint (pin the version exactly — 3.1.2 at audit time; the crate is moving fast), so this is an emphasis and a config default, not a second implementation. A per-agent stdio shim forwarding to the same endpoint remains the universal fallback for any harness client that can't speak HTTP.

Horde-as-MCP-client (the hub consuming user servers) is deliberately transport-omnivorous — stdio, streamable HTTP, and legacy HTTP+SSE — because the integration long tail lives there and new-spec-only would exclude deployed servers. The 2026 Tasks extension fits the blocking verbs (`wait_for_agent`) but not agent lifetime, since a task's TTL and terminal states can't model an idle-but-revivable agent — evaluate at hub build time. Re-verify the concrete harness-client and SDK support matrix whenever the endpoint/hub are touched, **by wire trace against the installed binaries, never from memory and never from binary strings** — both harness binaries contain `2026-07-28` and `server/discover` while emitting neither, so grepping them yields the opposite of the truth.

Single-auth for user MCP servers (Linear, Grafana, …): the **server** runs/owns them and proxies their tools onto every agent's horde MCP endpoint. Auth happens once, at the hub. Config can mark a server **`local`** (machine-bound, e.g. browser automation), which runs worker-side instead. Per-agent-type allowlists scope who sees what. The multiplexing (many agents, one upstream) is the main implementation risk; `local` is the pressure valve for stateful/misbehaving servers. See ADR-0004.

## 10. Permissions

Layered, evaluated in order:

1. **Deterministic deny**
2. **Deterministic allow**
3. **LLM policy judge** — plain-text mode definitions ("allow X, never Y") evaluated by a cheap model against the proposed tool call
4. **Human approval** — an ordinary event in the tree, answerable from *any* connected client (this is the phone-approval story)

As built, the whole chain evaluates **server-side**, in one driver over the event log (deny → remembered allow → allow → judge → escalate) — one authority, not the worker-local fast path originally sketched here. The worker's share of enforcement is the harness-side settings hardening that stops a harness pre-empting the chain (ADR-0003).

Modes are named, user-definable (the plain-text-instructions-as-a-mode idea), and layered under manual-approve / allowlist styles. Escalation requests carry tree path + context ("session ci-fix → reviewer → test-runner wants `docker rm`"). As built (ADR-0009), the production server's unnamed mode allows an agent's own orchestrator verbs within its own subtree and escalates everything else. Rules name Horde's own tools canonically as `horde/<tool>`, and the chain maps Claude's `mcp__horde__<tool>` onto that name, so one rule covers both harnesses (ADR-0004, 2026-10-31 amendment).

**Per-provider gating capability varies.** Adapters advertise whether tool calls can be pre-authorized externally: Claude can (permission interception incl. input rewrite, ADR-0003); Codex cannot in current releases — its sandbox tier + project-trust chosen at thread start are the real controls (ADR-0005). Permission modes and placement must consult this flag: a high-trust assignment can refuse placement on an agent whose provider can't be gated. As built (ADR-0009), placement stamps the placed agent's adapter capabilities into the log, and a narrowing (`updated_input`) is refused — or, replayed from memory, escalated — for an agent whose adapter cannot rewrite tool input.

**Sandboxing:** the end state is orchestrator-level per-process sandbox profiles (writes confined to workspace, network allowlist), applied by the worker around the whole agent process — that's what protects agents from *each other* on a shared worker. Always optional per spawn/policy, never required. Bootstrap phase: rely on the harnesses' own sandboxes (CC/Codex: Seatbelt on macOS, Landlock/seccomp or bubblewrap-style on Linux).

## 11. Clients (CLI and TUI)

**CLI parity is a hard rule: the TUI is optional.** Every operation is a server RPC; the TUI and plain CLI subcommands are two frontends over the same client library, so anything the TUI can do has a scriptable `horde <verb>` equivalent (`horde session list`, `horde spawn`, `horde approve <id>`, `horde workspace create`, `horde agent status`, …). This is also what makes "Claude Code as a client" work — a CC wrapper or MCP shim drives the same verbs. New capability = new RPC + CLI verb first; TUI surface second.

- **Two zoom levels, one binary, same rendering machinery:** top-level multi-session view and per-session attach (`horde attach <session>`; `horde new` creates and attaches — the CC-per-tmux-pane flow, nearly verbatim). A bare `horde` is the front door (ADR-0008): with no live session it creates one, with one it attaches, with several it opens a picker. Detach to overview and back without restarting.
- **Attached to a session:** you see the top-level agent; prompts from anywhere in the tree bubble up CC-style, stamped with tree path.
- **Attached to a subagent:** you see prompts from that agent's subtree only, plus a passive badge ("2 agents elsewhere waiting"). Session-root attachment sees everything.
- **Global inbox view:** all agents blocked on human input across all sessions — the primary "what needs me" surface.
- **Multi-attach: free-for-all.** All attached clients can send; the server serializes into the event log; everyone sees everything. No driver locks — it's one user.
- **Steering:** messages to a busy agent (from user or parent) queue and deliver at the next turn boundary; a distinct **interrupt** action (Esc-like) cancels the current turn and delivers immediately. Two verbs, no ambiguity.

## 12. Usage and observability

- Usage events per agent (tokens, rate-window state where providers expose it), aggregated by tree path and by provider account.
- **Reporting + soft alerts:** clients surface per-agent/per-tree usage; threshold alerts fire as ordinary events (phone-visible). Not yet landed — reporting shipped first (`horde usage`, CLI only), with no alert event kind or threshold config yet.
- **Hard budgets (end state, sequenced later):** a spawn may carry a budget; at the cap the subtree pauses pending human approval (an ordinary permission-style event). The accounting schema is designed for this from day one.
- **OTLP tracing:** the agent tree maps onto the span tree (agents = spans, tool calls = events), giving flame-graph views of an orchestration. As built: the export pipeline is real but carries RPC-handler spans only — the agent-tree-to-span mapping is still to build, so the flame graph is not yet obtainable — and export is plaintext gRPC (no TLS), an open inconsistency with §3's mTLS-everywhere posture.

## 13. Configuration layering

Precedence: **spawn args > repo > workspace > user-global.**

| Layer | Lives | Holds |
|---|---|---|
| User-global | Server DB (CLI/TUI-authored) | Default provider, personal permission modes, top-level-vs-subagent prompt overlays (about *you*) |
| Workspace | Workspace definition (server) | Default mode, default agent type, layout, MCP allowlists, collision pins |
| Repo | `.horde/` in each repo | Repo-scoped agent types & prompt fragments (about *that codebase*), versioned with the code, rides PRs |

Multi-repo workspaces: each repo's `.horde/` contributes namespaced definitions; cross-repo name collisions are surfaced warnings, pinnable at the workspace layer. Top-level agents and subagents get different prompt overlays (a stated requirement) — user-global config, applied by the worker at spawn.

## 14. Build sequencing

Ordering of activation, not scope. Everything in this document is end state; this is the order it comes alive in. The quality gauntlet (§16 / docs/TESTING.md) lands alongside the first crate, not after.

1. Proto contracts + event log (server + storage)
2. **Claude adapter via stream-json — the de-risking spike** (full turn + permission prompt through `--permission-prompt-tool`)
3. MCP verb surface (spawn / assign / note / result, ancestry, payload auto-detect)
4. Workspaces + git relay
5. TUI: attach + session tree + `horde new`
6. Permission layers (deterministic → judge → human)
7. Enrollment/mTLS, remote workers, scheduler with capabilities
8. MCP hub; blob store; usage reporting; OTLP

Sequenced later (still end state): raw-model harness, hard budgets, orchestrator-level sandbox, GUI/mobile clients, multi-user server, credential-less workers (relay-only git), ssh worker bootstrap + harness install recipes.

## 15. Open questions

- Worker tag taxonomy conventions (freeform now; revisit when it hurts).
- MCP hub multiplexing details for stateful upstreams.
- Multi-user / shared-server model (explicitly deferred).
- Whether workspace repo-set additions need more ceremony than the permission-event.
- Raw-endpoint harness mechanism: built-in minimal loop vs. wrapped OSS harness (§4).
- Budget unit for hard budgets (§12): raw tokens aren't comparable across models — per-tier budgets or a normalized cost unit, decide before activation.

## 16. Development quality

Horde's own development runs under a full test and metrics suite — the point is that agent-written code is trusted through the gates it passes, not by line-by-line reading. The complete specification lives in **[docs/TESTING.md](./docs/TESTING.md)**: test pyramid (unit, integration, multi-process e2e, property, mutation, Gherkin behavioral, TUI snapshot, proto contract), the FakeAdapter harness, the gauntlet-style gate runner, metrics (coverage, CRAP, duplication, mutants, unused deps, module structure), and the observe → ratchet → hard-gate rollout ladder. It lands with the first crate.
