# Enrollment is not authorization: roles per RPC, identity from the certificate, origin-aware ingest

A review of `main` at `2a50877` found the trust boundary DESIGN §3 calls
"real" stopped one question short. Every RPC on both spines passed the mTLS
door, and the door answered exactly one thing: *is this an enrolled,
unrevoked principal?* Nothing asked *which kind*, or *which one*. A worker
certificate could answer permission prompts, rewrite the MCP catalog, and
archive sessions; any certificate could register as any worker by naming it
in a request body, drain that worker's command stream, and journal events
into any session — including the server's own reserved streams, as kinds
only the server should write. The same review found the interrupt verb's
acknowledgement folded back into another request (an unbounded log storm),
a narrowing decision deliverable to an adapter that cannot apply one, and a
production default that escalated an agent's own `report_result` to a
human. This ADR draws the boundary in five places.

**Roles per RPC, decided in one table.** The door now answers the second
question too. Every guarded method path — `/horde.v1.ControlPlaneService/…`,
`/horde.v1.WorkerService/…`, `/horde.v1.BlobService/…` — has a row in one
table (`horde_server::trust::admits`) naming who may call it: clients only,
workers only, or either. The worker spine is workers only, every method.
The control plane is clients only except four rows, which are the *agent
conduit*: an agent's MCP endpoint lives on its worker, so its spawns,
messages, and waits ride the worker's certificate (`SpawnAgent`,
`SendMessage`, `SyncEvents`, `Hello`). Those handlers scope a worker caller
to its own placed agents — a spawn's parent, and a message's target
session, must hold an agent placed on the calling worker. Blob transfer is
either role. The table is keyed by the gRPC method path, which a router
layer stamps into each request so the door's interceptor can read it; a
path the table does not name is refused, so the failure mode of a forgotten
row is closed, not open. A test walks every method the compiled proto
declares on the guarded services and requires a row for each — adding an
RPC without deciding who may call it fails the suite rather than shipping.

**A worker's identity is its certificate.** On the worker spine the
`worker_id` a request body carries is no longer believed: every handler —
registration, the command stream, the journal push, capability and instance
reports, the forward and login streams, the hub calls — derives the worker
from the certificate through one helper (`trust::caller_worker_id`). The
body field stays on the wire (compatibility, and the plaintext loopback test
shape, which has no certificate to read), and a body that names a different
worker than its certificate is refused rather than silently corrected: a
mismatch is either a bug or an impersonation attempt, and neither should
succeed quietly. Instance reports naming another worker are refused the
same way, since the instance cache is keyed by the worker a report names.

**Ingest is origin-aware, and a refusal does not wedge the journal.** The
server now judges each journaled event against what a worker may say
before appending it. A worker may journal only the kinds a worker
*observes* — agent output, lifecycle, permission requests, results,
interrupt acknowledgements, usage, workspace instance changes — and only
about agents the log has placed on it; an instance change with no agent
must answer a prewarm that named this worker in that stream. Everything
else is refused: server-only kinds (placement, permission decisions, the
workspace and MCP catalogs, enrollment and revocation, forwards, logins,
spawns and messages), any reserved stream other than the prewarm case, and
agents placed elsewhere. A refused event is not dropped and does not fail
the batch. Failing the batch was a wedge: the worker reconnects, resumes at
the same journal position, and offers the same poison event forever. So a
refusal is journaled as a `JournalEventRefused` dead letter in the reserved
`horde-dead-letters` stream, appended under the worker's origin and the
refused event's journal sequence. That one row does three jobs: the
refusal is auditable as an event, the worker's journal watermark (derived
from stored rows) advances past it, and a replay of the same poison event
deduplicates onto the dead letter instead of appending another. The worker
is acked through it and truncates normally. Malformed entries (no session,
no event, inline bulk, an idempotency key colliding with a different kind)
dead-letter the same way — every per-event refusal is non-wedging; only a
storage fault fails the batch.

**Interrupt requests and acknowledgements are told apart by origin.** The
command fold turned every `TurnInterrupted` into an `InterruptAgentCommand`,
and the worker carries one out by journaling a `TurnInterrupted` of its
own — which the fold turned into another command. The global replay row now
carries the origin worker the log already stored, and only a
Server-originated `TurnInterrupted` is a request. Origin rather than a
field on the event, because origin is the one fact about a row no journal
can forge: the server stamps it at append. No proto change was needed.

**A narrowing needs an adapter that can apply it.** `PermissionDecision`
may carry `updated_input` — replacement arguments — and permission.proto
already says a worker whose adapter cannot rewrite must reject one rather
than drop it. The server sent them anyway: a human's narrowing via
`AnswerPermission`, or a remembered narrowing the chain replays to another
agent in the session, reached Codex agents, whose adapter has no
`RewritesToolInput`. Placement now stamps the placed agent's adapter
capabilities, as its worker advertised them, onto `PlacementChanged`
(`adapter_capabilities`, additive), so the capability is a log fact the
chain can fold rather than a live-registry lookup that a restart or a
disconnect would lose. `AnswerPermission` refuses a narrowing for an agent
without input rewriting (`FailedPrecondition`, naming the agent and the
missing capability), and the chain's remembered-narrowing replay degrades to
an escalation for such an agent, so a human decides rather than a clamp
being silently widened or crashing the worker. An agent placed before this
field existed reads as unable to rewrite — fail closed.

**The shipped default mode lets an agent orchestrate its own subtree.**
`PermissionModes::default()` escalates everything, which was the right
do-no-harm configuration for the chain's tests and the wrong one for
production: every agent's own `report_result`, `spawn_agent`,
`send_message`, and `wait_for_agent` escalated to a human. Production now
starts the chain with `PermissionModes::shipped()`, whose unnamed mode
allows an agent's own orchestrator verbs — as either harness names them
(`horde/<verb>`, `mcp__horde__<verb>`) — and escalates everything else.
"Own" is scoped by the ancestry the log records: spawning and reporting are
always the caller's own; a message or a wait must target one of the
caller's descendants, or (for a message) its parent. The forward verbs
(`expose`, `inject`) are not orchestrator verbs and keep their own chain
treatment. The predicate is pure and tested over the session projection;
deny rules still run first.

## Consequences

- A worker certificate can no longer call operator verbs, and a client
  certificate can no longer speak on the worker spine. Mint and revoke keep
  their in-handler Client check as defense in depth; the table is what makes
  the rule structural.
- Workers that present a body `worker_id` different from their certificate
  are refused. The shipped worker already sends its enrolled principal's
  name, so this changes nothing for an honest peer.
- Two additive proto changes: `PlacementChanged.adapter_capabilities` and
  the `JournalEventRefused` event kind. `buf breaking` passes; old logs read
  as "no capabilities" and "no dead letters".
- The reserved-stream set grows by `horde-dead-letters`; it is filtered
  from session listings and placement like the others.
- What is deliberately not done here: worker *reads* of the control plane
  (`SyncEvents`) are not yet scoped to sessions the worker holds agents in;
  a general reset verb for a worker whose journal regressed is a separate
  item; and a worker-journaled event for an agent re-placed elsewhere after
  a partition dead-letters rather than landing — the log records it, but no
  reconciliation acts on it yet. (All three are closed by ADR-0011.)
