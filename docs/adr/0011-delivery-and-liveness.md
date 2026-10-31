# Delivery and liveness: a worker is alive on a lease, a message waits for its agent, and archive is terminal

A review of `main` at `15a3171` found the delivery spine sound in its steady
state and wrong at every edge where time passes. A worker that vanished
without a FIN was never noticed: no keepalive ran on either end, and the
server only learned a command stream was dead when its next send failed —
for a quiet worker, never. A server restart left every agent the log placed
on a worker `Placed` forever, because a fresh process holds no registrations
and nothing reconciled the log against that. A message sent to an agent
before placement, or to a not-yet-started agent waiting to be re-placed,
issued no command and was never delivered. Archiving a session left its
agents' own lifecycles untouched, so a queued agent of an archived session
was placed and spawned after the archive, and a message to an archived agent
was accepted. A retried spawn minted a fresh agent id and appended a
placement for an agent the log never spawned; a retried send answered with
an assignment id the log never stored. The permission chain advanced its
cursor past a whole page before processing it, so one failed row skipped
every request behind it. The `SyncEvents` heartbeat wrapped the tail's
`next_batch` in a timeout, which cancelled it mid-read. The relay registry
only ever added repos. And ADR-0009 left three gaps open: worker reads of
the log were not scoped, a worker that lost its journal had no way back, and
a late journal from an agent's former placement dead-lettered. This ADR
closes all of them in seven decisions.

**A worker is alive on a lease.** Liveness has three layers. HTTP/2
keepalive runs on both ends (`horde_server::transport::keepalive`,
`horde_worker::client::keepalive`: ping every 20 s, dead after 10 s
unanswered, the worker pinging even while idle), so a half-open connection
becomes a transport error instead of an eternal wait. The server's command
stream pump selects on `sender.closed()` beside the next batch, so the moment
the transport drops a response stream the worker leaves the registry —
not at the next command anyone happens to send it. And leaving the registry
starts the worker's **lease** (`horde_server::liveness`, default 120 s,
`--worker-lease <secs>`): a worker that re-registers inside it bumps its
registration epoch and nothing happens; one that does not is **declared
lost**. Agents it never started (`Queued`/`Starting`) are re-queued and
re-placed — a fresh spawn is exactly what they were waiting for, and that
part still happens at disconnect, as before. Agents it had started get a
`PlacementChanged` with the new, additive `PLACEMENT_STATE_LOST` and an
`AgentLifecycleChanged` to `FAILED` (unless already failed) recording the
state it was lost from. They are not re-placed: their context — provider
session, uncommitted worktree — lived on that worker, and a fresh spawn
elsewhere would silently re-run half-finished work, commits and side effects
included. A human or the parent decides what replaces them. `LOST` is
settled like `EXPIRED`: placement never revisits it, and the command fold's
existing "placement moved away" rule cancels the agent on the lost worker if
it ever does return. The lease is the one timer in placement; everything it
decides is an appended event.

**A fresh server reconciles at boot.** `serve` runs
`liveness::reconcile_at_boot` before it listens: every worker the log places
a live agent on is, to a process with no registrations, away. Each is
treated exactly as a disconnect — its never-started agents re-queued at
once, and a lease started for the rest — so an agent on a worker that never
comes back is declared lost one lease after the restart instead of hanging
forever.

**A message waits for its agent, in the fold.** The command fold
(`service::commands`) now keeps, per agent, every message sent to it while it
may still be (re-)placed — spawned and not yet seen running. The placement
that spawns the agent delivers that buffer right behind the spawn command,
in send order; a re-placement delivers it again to the new worker, because
the new incarnation never saw any of it. Each buffered delivery's command id
is the message's own position suffixed with the placement's
(`s1:7:placed:12`), so a re-placement back onto a worker that applied an
earlier incarnation's copy is not deduplicated away, and its log position is
the placement's, where the fold issued it. The buffer is let go once the
agent is seen running: from then on the agent is never re-placed (the lease
declares it lost instead), so nobody would ever replay it. "Durably" costs
nothing: the buffer is a fold over the log, recomputed identically after any
restart — no table, no queue, ADR-0001 intact.

**Archive is terminal, and each agent records it.** `ArchiveSession` now
appends an `AgentLifecycleChanged` to `ARCHIVED` for every agent of the
session that is not already archived, with `previous_state` set to the state
it was archived from — the server knows it; the worker that carries the
archive out does not track idle-versus-running and still journals its own
confirmation with `UNSPECIFIED`. An agent no worker holds (queued, lost)
finally has its end recorded at all. Everything downstream reads the agent's
own lifecycle: placement skips archived agents and agents of archived
sessions (`placement::placeable`), the fold drops their buffers, and
`SendMessage`/`SpawnAgent` refuse an archived target, a child of an archived
or lost parent, or any spawn into an archived session with
`FAILED_PRECONDITION` naming the agent and why (`ServerError::AgentEnded`).
A lost agent is refused the same way: the context a message would continue
died with its worker.

**Retries return what the first attempt made.** A `SpawnAgent` whose
idempotency key the session already holds answers with the stored
`AgentSpawned`'s agent and its current placement, checked under the
placement lock so racing twins cannot both miss it; nothing new is
appended. A `SendMessage` whose append deduplicates answers with the stored
message's assignment id and sequence rather than the id this attempt made
up. A key that collides with a *different* kind is still refused by name,
as before.

**Command streams resume by position, not only by sequence.** The command
fold numbers each worker's commands densely, and "the same log yields the
same numbering" holds only for one build of the fold. ADR-0009's
interrupt-storm fix removed commands from existing logs, and buffered
delivery adds them, so a worker that acked sequence *N* under one build
would, under the next, skip or repeat commands. Every `StreamCommandsResponse`
now carries `log_position` — the causing event's append position, which no
fold can renumber — the worker journals the highest position it applied in
the same transaction as the command (`applied_command_position`, an additive
meta key), and `StreamCommandsRequest.resume_after_log_position` outranks the
sequence when set: the server redelivers every command caused at or after
that position, and the worker dedupes the overlap on `command_id` as it
always has. A worker that never saw a position resumes by sequence as
before. The same SyncEvents defect class was fixed beside it: the heartbeat
now *races* the pending batch (`sync::next_or_heartbeat`) instead of
cancelling it under `timeout`, because a tail cancelled after consuming its
wake-up — or a lag recovery cancelled between two sessions' reads — loses
events.

**A worker's journal can be reset, deliberately.** A worker whose state
directory was lost reports a journal head below what the log holds and is
refused; renumbering its events under old positions would forge history.
It is now refused earlier — `RegisterWorkerRequest` carries its journal head
(additive, optional) — so its command stream never opens and replays a
history its fresh state never applied, and the refusal names the recovery:
`horde worker reset <worker>` (`ResetWorkerJournal`, client-only). The reset
journals a `WorkerJournalReset` in the reserved `horde-worker-journal`
stream recording the *floor* — the highest journal position the log held.
From then on the worker's journal positions are stored above the floor (so
its fresh journal neither deduplicates onto nor is mistaken for the lost
one), registration and `PushEventsStart` compare against its current
journal only, its command numbering restarts at one and a command stream
owes it nothing folded before the reset, and the agents it held are declared
lost exactly as a lapsed lease declares them.

**Late journals from a former placement are handled, not dead-lettered.**
Ingest's origin view now remembers which worker each agent used to be placed
on. An entry from such a worker — it kept journaling through a partition
while the agent was re-placed or declared lost — is judged by kind. Pure
history lands as itself: output and usage happened, are attributable, and
overwrite nothing. Anything that would speak for the agent *now* — a
lifecycle change, a permission request the current worker cannot answer, a
result from a superseded incarnation, an interrupt acknowledgement, an
instance change — is recorded whole as a `StaleJournalEvent` (additive) in
the agent's own session, under the worker's origin and the entry's journal
position: visible to every attached client, still advancing the worker's
watermark, never silent. Strangers' entries still dead-letter as ADR-0009
decided.

Two smaller closures ride along. Worker reads of the control plane are
scoped: a worker's `SyncEvents` must name its sessions, each one it holds an
agent in, and may not use `all_sessions`. The relay registry now tracks the
catalog exactly — a deleted definition's repos are unregistered and stop
being served; their mirrors stay on disk, since a result in the log may name
a commit only the mirror holds. And the permission driver advances its
cursor per row processed: a storage failure retries from the failing row,
while a row that can never settle (it does not decode, its decision collides
with the log) is logged and passed, as a dead letter is, rather than wedging
every request behind it.

## Consequences

- Proto changes are all additive: `PLACEMENT_STATE_LOST`, the
  `StaleJournalEvent` and `WorkerJournalReset` event kinds,
  `RegisterWorkerRequest.last_local_journal_sequence` (optional),
  `StreamCommandsRequest.resume_after_log_position`,
  `StreamCommandsResponse.log_position`, and the `ResetWorkerJournal` RPC.
  `buf breaking` passes against `main`. Old logs read as "no losses, no
  resets"; old workers resume by sequence and are judged at
  `PushEventsStart`.
- A partition longer than the lease costs a started agent its placement:
  when the worker returns it is told to cancel the agent, and the agent's
  late journal lands as history or stale records. The lease is the knob —
  long enough for a laptop lid or a worker restart, short enough that a dead
  machine is noticed while someone still cares.
- A boot with workers that do come back promptly still re-queues their
  never-started agents (and cancels them on the returning worker), exactly
  as any disconnect does. Started agents are untouched by a prompt return.
- A server-written `ARCHIVED` and the worker's own confirmation are both in
  the log for a placed agent; the fold reads the same terminal state from
  either.
- The fold's per-agent message buffer lives in memory for each command
  stream until the agent runs or is archived; its size is the traffic sent
  to agents that never started.
- A buffered message sent before an upgrade, to an agent placed before it,
  is not retro-delivered to a worker whose position already passed that
  placement: the position resume deliberately never reaches back before what
  the worker applied.
- What is deliberately not done here: a lost agent is not revived by a
  message (it is refused); a retried spawn's placement answer carries no
  queued reason (the projection keeps none); and the reset verb does not
  refuse a worker that is currently connected — it is for a worker whose
  state is gone, and the operator is trusted to know which one that is.
