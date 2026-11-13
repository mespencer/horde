# Horde

Multi-provider agent orchestrator: a control plane through which all agents flow — user-driven and agent-spawned — with session trees, cross-machine placement, and attach-from-anywhere clients.

## Language

### Roles

**Server**:
The single control plane holding session state, the event log, the scheduler, and shared services (relay, blob store, hub).
_Avoid_: control server, hub (that's the MCP hub), master

**Worker**:
A machine-resident supervisor that runs agent processes and materializes workspace instances. Dials the server; never dialed.
_Avoid_: runner, node, host

**Client**:
A user-facing program (the `horde` CLI and TUI today) attached to the server to view, steer, and approve.
_Avoid_: frontend, terminal

**Command Stream**:
One worker's sequence of commands, folded out of the event log rather than stored. Densely numbered per worker, so a reconnecting worker resumes from its own ack — or, when it knows one, from the log position of the event that caused its last command, which takes precedence (ADR-0011).
_Avoid_: command queue, outbox, mailbox

### Agents and sessions

**Agent**:
One conversation with one model via one provider harness, supervised by a worker. The single primitive — there is no bounded/unbounded distinction.
_Avoid_: subagent-as-a-type, teammate, task

**Top-level Agent**:
The root agent of a session; the one the user converses with directly.

**Session**:
One conversation tree rooted at one top-level agent. The unit of viewing and attachment, never of placement.
_Avoid_: project, conversation, tree (alone)

**Assignment**:
A message that expects exactly one Result. Spawning carries an implicit first assignment.
_Avoid_: task, request

**Note**:
A message owing no Result — chat, context, status.
_Avoid_: update, ping

**Result**:
The single response an Assignment is owed: status (success | partial | blocked), summary, and Payload.
_Avoid_: report, output, deliverable

**Payload**:
What rides a spawn, message, or Result: a committed git ref and/or Attachments. Transport is never the agent's concern.

**Attachment**:
A non-code file (screenshot, binary, log) carried by a Payload via the Blob Store.
_Avoid_: artifact, asset

**Steering**:
Sending input to a busy agent. Queued to the next turn boundary by default; an Interrupt cancels the current turn and delivers immediately.

**Attach**:
A client joining a session or agent's live view. Attaching to an agent scopes prompts to that agent's subtree.

**Reaper**:
The policy that archives idle agents; archiving (not the Result) is what discards context.

### Placement and workspaces

**Placement**:
The scheduler assigning one agent to one worker at spawn time.
_Avoid_: scheduling a session

**Capability**:
A fact a worker advertises that placement can match on: tags, provider accounts, reachable remotes, warm instances.
_Avoid_: feature, label (alone)

**Worker Lease**:
How long a worker may stay away before it is declared lost (ADR-0011): its never-started agents are re-placed, its started agents are marked lost — their context lived on that worker.
_Avoid_: timeout (alone), heartbeat

**Lost**:
The settled placement of an agent whose worker outlived its lease, or whose journal was reset. Never re-placed automatically; it takes no messages or children.

**Journal Reset**:
The operator's recovery for a worker whose journal regressed (`horde worker reset`): its fresh journal is numbered above the old one's floor and its agents are declared lost.

**Workspace**:
A named set of 1..N repos an agent tree works in. Exists as a portable Definition and per-worker Instances.

**Workspace Definition**:
The portable description: repos, layout, unmanaged paths, checkout mode, per-repo reclaim commands and cleanup overrides. Server-owned; always created deliberately, never from cwd on its own.

**Workspace Instance**:
A Definition materialized on one worker's disk. Worker-owned; server's view is cache.
_Avoid_: checkout (that's per-agent)

**Checkout Policy**:
How an agent occupies an instance: shared, worktree (default), or fresh-clone.

**Unmanaged Path**:
A directory in a workspace that git never covers and payloads never sync (datasets, weights, caches). Lives once in the instance and is linked into every checkout.

**Checkout**:
One agent's private worktrees (or clones) of an instance's repos, under its own root, on its own branch. Recorded in a ledger beside it so it outlives a journal reset.
_Avoid_: instance (that's per-worker)

**Base**:
The commit a checkout's repo starts from: the freshly fetched `origin/<default branch>` by default, a parent's committed HEAD for a child, or a commit, branch or tag a spawn names. Never the instance's own HEAD unless nothing else exists.

**Checkout Mode**:
Whether an agent checks out every repo of its workspace at spawn (all) or only the ones it names, the rest on demand through `workspace_checkout` (lazy).

**Soft Reclaim**:
Running a repo's reclaim command (`cargo clean` for a Rust repo) in a checkout; the worktree, branch and commits stay.

**Hard Removal**:
Deleting a checkout's worktree and branch after pinning its HEAD under `refs/horde/archived/<agent>`. Automatic only when provably lossless; otherwise only `gc --hard --force`.

**Archive Pin**:
The ref (`refs/horde/archived/<agent>`) that keeps a removed checkout's last commit reachable; dropped only when origin or the relay holds the commit.

**Orphan**:
A checkout on a worker's disk whose agent the worker no longer knows — what a journal reset leaves. Reported, soft-reclaimed at most, removed only by an explicit hard gc.

### Services

**Relay**:
The server's bare git mirrors — the transport for cross-worker commit sharing, the clone cache, and (eventually) the sole holder of external git credentials.
_Avoid_: mirror (alone), git server

**Blob Store**:
The server's content-addressed store for Attachments. Lifetime tied to the session.

**MCP Hub**:
Server-owned MCP servers proxied onto every agent's endpoint; the single place user MCP auth happens. A server marked `local` runs worker-side instead.
_Avoid_: proxy (alone)

**Adapter**:
The per-provider implementation of spawn/send/stream/interrupt/resume, with capability flags (e.g. resumable).
_Avoid_: driver, harness (that's the provider's own CLI)

**Provider Account**:
One authenticated subscription login for one provider, isolated per config dir; a worker-level capability.

**Enrollment**:
The flow by which a new worker or client obtains its mTLS identity from the server's CA, by redeeming a one-time join token (`horde://join/<host:port>/<secret>.<fingerprint>`) that carries the enrollment listener, the secret, and the server fingerprint to pin (ADR-0007).

**Policy Judge**:
The cheap-model evaluator that applies a plain-text permission mode to a proposed tool call, between the deterministic layers and human approval.

**Prompt Overlay**:
The user-global prompt variant applied at spawn, differing for top-level agents vs spawned agents.

### Process

**Sweep**:
The whole-tree `gauntlet run --full` mutation run, owed at every wave boundary — as opposed to the diff-scoped run every merge gets.
_Avoid_: full run (alone), audit
