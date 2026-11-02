# The Server reads a live projection, through a read pool, and ingests a journal batch in one transaction

The project review found the Server's read path correct and quadratic.
ADR-0001 makes state a pure function of the log, and the code took that
literally on every request: `ListSessions` listed every stream, read each one
whole and folded it; a placement pass did the same for every session; an MCP
hub call, a forward open, a forward tunnel and a permission answer each
re-folded the sessions they touched. Every one of those reads also queued
behind the single connection that appends, so a worker pushing a large journal
batch stalled every client. Ingest itself paid one `IMMEDIATE` transaction —
one commit, one WAL frame set — per journaled event. The review also flagged
three smaller costs: the worker-watermark query could not use its partial
index, the schema carried a second index identical to the primary key's, and
the relay's catalog follower re-read the catalog on every append to every
session. This ADR fixes all of them without giving up "state is the log".

**A live session projection.** `SessionProjection` (`projection.rs`, held by
`ServerCore` as `sessions`) keeps every stream's `SessionState` in memory. It
holds no semantics of its own: a stream's fold starts from
`SessionState::new` and advances by `SessionState::apply`, the very start and
step `SessionState::project` uses, so a live state and a cold re-fold of the
same events are one computation split at different points. A property test
drives arbitrary logs, including undecodable payloads, through the real log
and compares the projection, caught up in arbitrary increments, with cold
folds stream by stream and in listing order.

The projection does not listen for append notifications. At the start of
every read it **catches up**: it reads the log forward from a cursor
(`rowid`, the log's one total append order) and folds what it finds. That
keeps it exactly as fresh as the log for every reader. An append that has
returned is folded before the next read answers, whichever clone of the log
wrote it and whichever path the write took. A lagged channel cannot leave it
stale. A read with nothing new costs one indexed query that returns nothing.
The boot path (`startup::reconciled_core`) runs the catch-up once from row
zero, so the cold fold of the whole log happens once per process rather than
once per request.

A stream holding an event that does not decode fails every cold fold of it.
The projection keeps that answer instead of a state: the stream's fold stops
there, and every read of it reports the same `Decode` error. A walk over
sessions (`SessionProjection::walk`) filters streams by id before it looks at
their folds, so the reserved streams it turns away cannot fail it. An
admitted stream that cannot fold fails the walk at the same point a loop of
cold folds would have failed.

These now read the projection: `ListSessions`, `GetSession` and every
control-plane verb that projects one session; agent-to-session lookup;
`AnswerPermission`'s request search; every placement pass (and so
`worker_load`); the hub's caller gate; `OpenForward`, `CloseForward` and
`ListForwards`; the worker's forward intents; and tunnel resolution. Folds
whose input is not "a session at its head" stay as they were. That covers the
permission driver's and the forward opener's folds at an earlier log
position, the subtree filter's seed, the usage fold, and the single-stream
catalogs (workspace, MCP, enrollment, logins), each of which reads one small
stream through the indexed replay.

**A read pool.** A file-backed `EventLog` opens four `query_only` connections
beside its writer. Every read (`read_from`, `head`, `read_global_from`,
`session_ids`, `stored_under_key`, `worker_journal_watermark`) takes an idle
one, or waits on its turn's connection when all four are busy. Under WAL each
read is its own snapshot of everything committed before it began. So an
append that has returned is visible to the next read on any connection, and
the rowid cursor never skips a row, because a single writer commits rows in
rowid order. Appends stay on the one writer, which is still what keeps
per-session sequences dense. An in-memory log has no pool: its database is
private to one connection, so its reads share the writer as before.

**Index work.** The watermark query now says `AND journal_sequence IS NOT
NULL`, without which SQLite cannot use the partial `events_worker_journal`
index and scans every row naming the worker. The journal-dedupe probe spells
out the same predicate, and the idempotency-key probes add
`idempotency_key <> ''` for the partial `events_idempotency` index. Every
caller has already refused the empty key, so the term changes no answer. A
test asserts each probe's query plan names its index. The duplicate
`events_session_sequence` index is gone. The primary key `(session_id,
sequence)` already serves replay. The schema's `DROP INDEX IF EXISTS` is the
migration: the schema is applied on every open, so an older database loses
the index the first time this build opens it.

**Batched ingest.** `ingest_batch` judges every entry of a journal batch
first. It judges against the origin view as it stood when the batch arrived,
which is what the one-append-per-entry loop judged against too. Then it
appends every resulting row — accepted events, stale records, dead letters —
through `EventLog::append_batch`, in entry order and inside one `IMMEDIATE`
transaction. Each write gets the same dedupe and the same dense sequences a
lone append would. Writes inside the transaction see the earlier ones, so a
duplicate within one batch dedupes exactly as it did across two appends. A
key collision can only be known at the append. The batch's `follow_up` hook
dead-letters it directly behind the write that collided, ahead of the next
entry's row, which is where the loop put it. New rows are announced after the
commit, in append order. A storage failure now rolls the whole batch back
instead of leaving a prefix. The worker re-sends the batch whole either way,
and its acknowledged watermark is unchanged. Instance reports reach the
placement cache only for entries that actually landed.

**Relay follower.** The relay's registry follower now wakes only for an
append to the workspace catalog stream, or for a lag (a dropped notification
may have been the catalog's). Before, every agent's every output chunk made
it re-read and re-fold the catalog to register nothing new.

**`ListSessions` by name.** `ListSessionsRequest` gains `optional string
name = 4`. When it is set, only sessions whose name (the summary's `title`,
the root spawn's first tree-path element) is exactly that string are listed,
still subject to `include_archived`. Unset means no filter, which is what an
older client's request, never carrying the field, means. The field is
`optional` so that matching the empty name stays expressible. Adding a field
is wire- and source-compatible for every existing caller. The CLI and TUI
consume it in a later change.

**Amendment (2026-11-02):** the CLI now uses the filter. Session-name
resolution asks the Server for the named session
(`Client::list_sessions_named`, called from `crates/horde-cli/src/resolve.rs`)
rather than listing every session and matching on the client.

## Consequences

- The Server holds every session's state in memory: memory now grows with
  the number of sessions and agents, where before it grew with the work per
  request. That is the trade intended. The events themselves are not held.
- A read pays a projection-lock acquisition and a catch-up query. Reads of
  the projection serialize on that lock, but each holds it only for an
  in-memory walk plus a query that is usually empty, and never behind the
  writer.
- The projection remains a cache of a pure function. Deleting it would lose
  nothing but speed, and nothing writes to it except the fold.
- Ingest is all-or-nothing per batch. A batch the log cannot store leaves no
  partial prefix.
