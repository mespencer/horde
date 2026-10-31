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
A user-facing program (TUI today) attached to the server to view, steer, and approve.
_Avoid_: frontend, terminal

**Command Stream**:
One worker's sequence of commands, folded out of the event log rather than stored. Densely numbered per worker, so a reconnecting worker resumes from its own ack.
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
The portable description: repos, layout, unmanaged paths. Server-owned.

**Workspace Instance**:
A Definition materialized on one worker's disk. Worker-owned; server's view is cache.
_Avoid_: checkout (that's per-agent)

**Checkout Policy**:
How an agent occupies an instance: shared, worktree (default), or fresh-clone.

**Unmanaged Path**:
A directory in a workspace that git never covers and payloads never sync (datasets, weights, caches).

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
The one-time-code flow by which a new worker or client obtains its mTLS identity from the server's CA.

**Policy Judge**:
The cheap-model evaluator that applies a plain-text permission mode to a proposed tool call, between the deterministic layers and human approval.

**Prompt Overlay**:
The user-global prompt variant applied at spawn, differing for top-level agents vs spawned agents.

### Process

**Sweep**:
The whole-tree `gauntlet run --full` mutation run, owed at every wave boundary — as opposed to the diff-scoped run every merge gets.
_Avoid_: full run (alone), audit
