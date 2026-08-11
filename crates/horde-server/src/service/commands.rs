//! The per-worker command stream, folded out of the event log.
//!
//! Commands are at-least-once and carry a dense per-worker sequence, so a
//! worker that reconnects resumes from the last one it durably applied rather
//! than replaying blindly (`worker_service.proto`).
//!
//! # Why this is a projection and not a queue
//!
//! It used to be a queue: an in-memory `VecDeque` per worker, pushed by
//! whichever handler decided a worker should be told something. That lost every
//! undelivered command on restart, which is wrong by ADR-0001's standard —
//! everything that matters should be an event — and the fix was never a
//! `commands` table, because a second table is a second source of truth and the
//! two drift the first time a write path forgets one of them.
//!
//! The observation that makes a table unnecessary: **every command already
//! corresponds to an event that caused it.** A spawn command exists because an
//! agent was placed on a worker, and that placement is a `PlacementChanged`
//! event sitting in the log beside the `AgentSpawned` that describes what to
//! run. So the command stream is not state to be stored, it is a fold over the
//! log — the same shape as session lists, heads and journal watermarks, and
//! recomputed the same way after a restart.
//!
//! # The three sequence spaces
//!
//! `sequence` is per session and `journal_sequence` is per worker's own
//! journal, so neither can order two events from different sessions. Numbering
//! one worker's commands needs a total order over the whole log, which is the
//! append order [`GlobalEvent::row`] carries. The Nth command this fold emits
//! for a worker *is* that worker's command sequence N: dense, 1-based, and
//! monotonic because append order only ever grows.
//!
//! # Why the numbering survives a restart
//!
//! Nothing about the fold depends on when it runs. The same log yields the same
//! commands in the same order with the same sequences, so a worker that acked
//! command 7 before a restart is still talking about the same command 7 after
//! it. That is the property the old queue could not offer at any price, and it
//! is why the worker's ack — not the server's memory — decides where a stream
//! resumes.
//!
//! # What is not here yet
//!
//! Only the spawn command is derived, because it is the only one the server
//! currently issues. Delivery, interrupt and permission commands join the
//! `match` in [`CommandProjection::advance`] as their causing events start
//! being written; the shape does not change when they do. Reconciling against
//! what a *real* worker actually applied — as opposed to what it says it
//! acked — needs the worker crate, which is not on `main` yet; see
//! `tests/services.rs` for the marked seam.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use horde_proto::v1::{
    AgentId, Event, Placement, PlacementState, SessionId, SpawnAgentCommand,
    StreamCommandsResponse, event::Kind, stream_commands_response::Command,
};
use prost::Message;
use tokio::sync::broadcast;

use crate::error::ServerError;
use crate::log::{EventLog, GlobalEvent, Notification, StoredEvent};

/// How many log rows one fold step reads.
const PAGE: u32 = 512;

/// What an `AgentSpawned` said to run, held until a placement names its worker.
///
/// The two facts a spawn command needs are split across two events: the *what*
/// (`AgentSpawned`, carrying the spec and the parent) and the *where*
/// (`PlacementChanged`, carrying the worker). Neither alone is a command.
#[derive(Debug, Clone, PartialEq)]
struct SpawnDetails {
    session_id: String,
    parent_agent_id: Option<String>,
    spec: Option<horde_proto::v1::AgentSpec>,
    tree_path: Vec<String>,
}

/// The fold state: everything needed to turn the next log row into commands.
///
/// Deliberately free of any I/O, so the interesting logic is a pure function of
/// a sequence of events. Its tests drive it with hand-built events and never
/// stand up a database.
#[derive(Debug, Default)]
pub struct CommandProjection {
    /// Agents whose `AgentSpawned` has been folded, keyed by agent id.
    spawned: BTreeMap<String, SpawnDetails>,
    /// How many commands each worker has been issued. This *is* the per-worker
    /// command sequence.
    issued: BTreeMap<String, u64>,
    /// Append position folded up to; the cursor the next read continues from.
    row: u64,
}

impl CommandProjection {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The append position this fold has consumed.
    #[must_use]
    pub fn row(&self) -> u64 {
        self.row
    }

    /// How many commands this worker has been issued so far.
    #[must_use]
    pub fn issued(&self, worker_id: &str) -> u64 {
        self.issued.get(worker_id).copied().unwrap_or(0)
    }

    /// Folds one log row, returning the command it causes and whose it is.
    ///
    /// The worker id comes back alongside the command because a fold advanced
    /// by one worker's stream still has to account for every other worker's
    /// commands: skipping them would leave this worker's numbering right and
    /// the fold's state wrong.
    pub fn advance(&mut self, global: &GlobalEvent) -> Option<(String, StreamCommandsResponse)> {
        self.row = global.row;

        let event = Event::decode(global.event.payload.as_slice()).ok()?;
        let agent_id = event.agent_id.as_ref().map(ToString::to_string)?;

        match event.kind? {
            Kind::AgentSpawned(spawned) => {
                self.spawned.insert(
                    agent_id,
                    SpawnDetails {
                        session_id: global.event.session_id.clone(),
                        parent_agent_id: spawned.parent_agent_id.as_ref().map(ToString::to_string),
                        spec: spawned.spec,
                        tree_path: spawned.tree_path,
                    },
                );
                None
            }
            Kind::PlacementChanged(changed) => {
                let worker_id = placement_worker(changed.placement.as_ref()?)?;
                let details = self.spawned.get(&agent_id)?;
                let command = Command::SpawnAgent(SpawnAgentCommand {
                    session_id: Some(SessionId::new(details.session_id.clone())),
                    agent_id: Some(AgentId::new(agent_id)),
                    parent_agent_id: details.parent_agent_id.clone().map(AgentId::new),
                    spec: details.spec.clone(),
                    binding: None,
                    tree_path: details.tree_path.clone(),
                    prompt_overlay: String::new(),
                });
                let issued = self.issue(&worker_id, &global.event, command);
                Some((worker_id, issued))
            }
            _ => None,
        }
    }

    /// Assigns the next per-worker sequence, and the command's stable id.
    fn issue(
        &mut self,
        worker_id: &str,
        cause: &StoredEvent,
        command: Command,
    ) -> StreamCommandsResponse {
        let next = self.issued.entry(worker_id.to_owned()).or_insert(0);
        *next = next.saturating_add(1);

        StreamCommandsResponse {
            command_sequence: *next,
            // Derived from the event that caused it rather than from the
            // command's own sequence. `(session_id, sequence)` is the log's
            // primary key, so this is unique by construction and identical
            // every time the fold is recomputed — which is what "stable across
            // redelivery" has to mean once a redelivery can follow a restart.
            command_id: command_id(&cause.session_id, cause.sequence),
            command: Some(command),
        }
    }
}

/// The worker a placement names, if it is actually placed on one.
///
/// Extracted rather than written inline as `state == Placed && worker.is_some()`
/// because the two halves are not separately reachable in a live run: the
/// server only ever writes a worker id alongside `Placed`, so an `||` behaves
/// identically in every end-to-end test while being wrong about a queued
/// placement that still carries a stale worker. As a function the combinations
/// are an ordinary truth table.
fn placement_worker(placement: &Placement) -> Option<String> {
    let placed = placement.state == PlacementState::Placed as i32;
    let worker = placement.worker_id.as_ref().map(ToString::to_string);
    match (placed, worker) {
        (true, Some(worker)) => Some(worker),
        _ => None,
    }
}

/// A command's stable identity: the log position of the event that caused it.
fn command_id(session_id: &str, sequence: u64) -> String {
    format!("{session_id}:{sequence}")
}

/// Whether a folded command is one this stream still owes its worker.
///
/// Two independent reasons to drop one, and inline they are not separately
/// reachable: a stream almost always folds its own worker's commands in
/// ascending order from its resume point, so `&&` keeps behaving correctly
/// while silently delivering another worker's commands the moment two workers
/// share a server. Four cases, all assertable.
fn is_owed(worker_id: &str, command_worker: &str, sequence: u64, resume_from: u64) -> bool {
    worker_id == command_worker && sequence >= resume_from
}

/// One worker's live command stream.
///
/// Holds its own fold rather than sharing one: a stream is positioned by *its*
/// worker's resume point, and the fold is cheap to rebuild because the log is
/// the only durable state and it is already being read.
pub struct CommandStream {
    log: EventLog,
    worker_id: String,
    resume_from: u64,
    projection: CommandProjection,
    notifications: broadcast::Receiver<Notification>,
    parks: Arc<AtomicU64>,
}

impl CommandStream {
    /// Opens a stream for one worker, delivering commands from `resume_from`.
    ///
    /// Subscribes before the first read for the same reason [`EventLog::tail`]
    /// does: from that moment every append is announced, so a command written
    /// during the catch-up read is either in the read or in the channel.
    #[must_use]
    pub fn open(log: &EventLog, worker_id: &str, resume_from: u64) -> Self {
        Self {
            notifications: log.subscribe(),
            log: log.clone(),
            worker_id: worker_id.to_owned(),
            resume_from: resume_from.max(1),
            projection: CommandProjection::new(),
            parks: Arc::new(AtomicU64::new(0)),
        }
    }

    /// How many times this stream has waited for the log to move.
    ///
    /// Exposed because "did not spin" is otherwise unobservable. A stream that
    /// busy-loops instead of parking delivers exactly the right commands and
    /// passes every functional test, while burning a core and — the reason this
    /// counter exists — turning a mutation of the waiting path into a suite
    /// that hangs rather than one that fails. A test can assert this moved
    /// exactly once across an idle period: a hot loop drives it to thousands,
    /// and a wait that never waits leaves it at zero.
    #[must_use]
    pub fn parks(&self) -> u64 {
        self.parks.load(Ordering::Relaxed)
    }

    /// The next commands owed to this worker.
    ///
    /// `None` means the log was dropped and no further commands can arrive.
    ///
    /// Every path out of here returns a value rather than merely finishing. That
    /// is deliberate: an `async fn wait(&self)` returning `()` is
    /// indistinguishable from a mutant that returns immediately, so the mutant
    /// does not fail a test, it hangs one — and a timeout is a mutant nobody
    /// evaluated. Waiting is folded into a function that has to produce commands
    /// to be believed.
    pub async fn next_batch(&mut self) -> Option<Result<Vec<StreamCommandsResponse>, ServerError>> {
        loop {
            match self.catch_up().await {
                Ok(batch) if !batch.is_empty() => return Some(Ok(batch)),
                Ok(_) => {}
                Err(error) => return Some(Err(error)),
            }

            self.parks.fetch_add(1, Ordering::Relaxed);
            match self.notifications.recv().await {
                Ok(_) => {}
                // Lagging is survivable for the same reason it is on a tail: the
                // channel is a hint and the log is the truth, so the fold simply
                // carries on from its cursor.
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::warn!(
                        missed,
                        worker_id = %self.worker_id,
                        "command stream fell behind; re-reading from the log"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// Folds every log row appended since the last call, collecting whatever
    /// this worker is owed.
    async fn catch_up(&mut self) -> Result<Vec<StreamCommandsResponse>, ServerError> {
        let mut owed = Vec::new();
        loop {
            let page = self
                .log
                .read_global_from(self.projection.row(), PAGE)
                .await?;
            if page.is_empty() {
                return Ok(owed);
            }
            for global in &page {
                if let Some((worker_id, command)) = self.projection.advance(global)
                    && is_owed(
                        &self.worker_id,
                        &worker_id,
                        command.command_sequence,
                        self.resume_from,
                    )
                {
                    owed.push(command);
                }
            }
        }
    }
}

impl std::fmt::Debug for CommandStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommandStream")
            .field("worker_id", &self.worker_id)
            .field("resume_from", &self.resume_from)
            .field("row", &self.projection.row())
            .finish_non_exhaustive()
    }
}

/// How many commands a worker has been issued across the whole log.
///
/// What `RegisterWorker` answers with, and a fold rather than a remembered
/// number for the same reason as everything else here: after a restart the
/// remembered one would be zero, and the worker would be told to replay from
/// the beginning of a stream it has already applied.
pub async fn issued_to(log: &EventLog, worker_id: &str) -> Result<u64, ServerError> {
    let mut projection = CommandProjection::new();
    loop {
        let page = log.read_global_from(projection.row(), PAGE).await?;
        if page.is_empty() {
            return Ok(projection.issued(worker_id));
        }
        for global in &page {
            projection.advance(global);
        }
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentSpawned, AgentSpec, PlacementChanged, TurnInterrupted, WorkerId,
        stream_commands_response::Command,
    };

    use super::*;
    use crate::log::PendingEvent;

    /// A log row carrying `event`, at `row` in append order.
    fn row(row: u64, session: &str, sequence: u64, event: Event) -> GlobalEvent {
        GlobalEvent {
            row,
            event: StoredEvent {
                session_id: session.to_owned(),
                sequence,
                recorded_at_unix_ms: 0,
                payload: event.encode_to_vec(),
            },
        }
    }

    fn spawn_event(agent: &str, parent: Option<&str>) -> Event {
        Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::AgentSpawned(AgentSpawned {
                parent_agent_id: parent.map(AgentId::new),
                spawned_by: None,
                spec: Some(AgentSpec::default()),
                tree_path: Vec::new(),
            })),
        }
    }

    fn placement_event(agent: &str, worker: Option<&str>, state: PlacementState) -> Event {
        Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::PlacementChanged(PlacementChanged {
                placement: Some(Placement {
                    agent_id: Some(AgentId::new(agent)),
                    state: state as i32,
                    worker_id: worker.map(WorkerId::new),
                    reason: String::new(),
                }),
            })),
        }
    }

    /// The spawn/placement pair the server writes for one placed agent.
    fn placed_agent(start: u64, session: &str, agent: &str, worker: &str) -> Vec<GlobalEvent> {
        vec![
            row(start, session, start, spawn_event(agent, None)),
            row(
                start + 1,
                session,
                start + 1,
                placement_event(agent, Some(worker), PlacementState::Placed),
            ),
        ]
    }

    /// Folds every row, returning the commands and who they are for.
    fn fold(rows: &[GlobalEvent]) -> Vec<(String, StreamCommandsResponse)> {
        let mut projection = CommandProjection::new();
        rows.iter()
            .filter_map(|global| projection.advance(global))
            .collect()
    }

    /// All four corners. Inline as `state == Placed && worker.is_some()` these
    /// are not separately reachable, because the server never writes a worker
    /// id without `Placed` — so an `||` passes every end-to-end test while
    /// commanding a worker that a queued placement merely mentions.
    #[test]
    fn a_placement_names_a_worker_only_when_it_is_placed_on_one() {
        let placement = |state: PlacementState, worker: Option<&str>| Placement {
            agent_id: Some(AgentId::new("a1")),
            state: state as i32,
            worker_id: worker.map(WorkerId::new),
            reason: String::new(),
        };

        assert_eq!(
            placement_worker(&placement(PlacementState::Placed, Some("w1"))),
            Some("w1".to_owned())
        );
        assert_eq!(
            placement_worker(&placement(PlacementState::Placed, None)),
            None,
            "placed nowhere is not placed"
        );
        assert_eq!(
            placement_worker(&placement(PlacementState::Queued, Some("w1"))),
            None,
            "a queued placement that still names a worker must not command it"
        );
        assert_eq!(
            placement_worker(&placement(PlacementState::Queued, None)),
            None
        );
    }

    /// Both halves, separately. With `||` a stream would hand one worker
    /// another worker's commands; with the comparison flipped it would either
    /// redeliver what was acked or skip the first command owed.
    #[test]
    fn a_command_is_owed_only_to_its_own_worker_and_only_from_the_resume_point() {
        assert!(is_owed("w1", "w1", 5, 5), "the resume point is inclusive");
        assert!(is_owed("w1", "w1", 6, 5));
        assert!(
            !is_owed("w1", "w1", 4, 5),
            "already applied, so not owed again"
        );
        assert!(
            !is_owed("w1", "w2", 5, 5),
            "another worker's command must never be delivered here"
        );
        assert!(!is_owed("w1", "w2", 4, 5));
    }

    #[test]
    fn a_command_is_identified_by_the_log_position_that_caused_it() {
        assert_eq!(command_id("s1", 7), "s1:7");
        assert_eq!(
            command_id("s2", 7),
            "s2:7",
            "the same sequence in another session is a different command"
        );
    }

    #[test]
    fn a_placement_on_a_worker_becomes_that_workers_spawn_command() {
        let issued = fold(&placed_agent(1, "s1", "agent-1", "worker-a"));

        assert_eq!(issued.len(), 1, "one placement, one command");
        let (worker, response) = &issued[0];
        assert_eq!(worker, "worker-a");
        assert_eq!(response.command_sequence, 1, "dense and 1-based");
        assert_eq!(
            response.command_id, "s1:2",
            "identified by the placement event that caused it"
        );

        match response.command.as_ref().expect("a command") {
            Command::SpawnAgent(spawn) => {
                assert_eq!(
                    spawn.agent_id.as_ref().map(ToString::to_string),
                    Some("agent-1".to_owned())
                );
                assert_eq!(
                    spawn.session_id.as_ref().map(ToString::to_string),
                    Some("s1".to_owned()),
                    "the session comes from the log row, not from the placement"
                );
            }
            other => panic!("expected a spawn command, got {other:?}"),
        }
    }

    /// The spec and parent live on `AgentSpawned` and the worker lives on
    /// `PlacementChanged`; a command needs both, so losing either half must not
    /// silently produce a command with a default in it.
    #[test]
    fn a_spawn_command_carries_what_the_spawn_event_said_to_run() {
        let rows = vec![
            row(1, "s1", 1, spawn_event("agent-2", Some("agent-1"))),
            row(
                2,
                "s1",
                2,
                placement_event("agent-2", Some("worker-a"), PlacementState::Placed),
            ),
        ];
        let issued = fold(&rows);

        match issued[0].1.command.as_ref().expect("a command") {
            Command::SpawnAgent(spawn) => {
                assert_eq!(
                    spawn.parent_agent_id.as_ref().map(ToString::to_string),
                    Some("agent-1".to_owned()),
                    "the parent comes from the spawn event"
                );
                assert!(spawn.spec.is_some(), "so does the spec");
            }
            other => panic!("expected a spawn command, got {other:?}"),
        }
    }

    #[test]
    fn a_queued_placement_commands_nobody() {
        let rows = vec![
            row(1, "s1", 1, spawn_event("agent-1", None)),
            row(
                2,
                "s1",
                2,
                placement_event("agent-1", None, PlacementState::Queued),
            ),
        ];
        assert!(
            fold(&rows).is_empty(),
            "a spawn nothing can run is not a command"
        );
    }

    /// A placement for an agent this fold never saw spawned cannot be turned
    /// into a command: half of it is missing. Inventing one with a default spec
    /// would tell a worker to run the wrong thing.
    #[test]
    fn a_placement_without_its_spawn_is_not_guessed_at() {
        let orphan = vec![row(
            1,
            "s1",
            1,
            placement_event("agent-1", Some("worker-a"), PlacementState::Placed),
        )];
        assert!(fold(&orphan).is_empty());
    }

    #[test]
    fn events_that_command_nothing_are_folded_without_issuing() {
        let rows = vec![row(
            1,
            "s1",
            1,
            Event {
                agent_id: Some(AgentId::new("agent-1")),
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(Kind::TurnInterrupted(TurnInterrupted::default())),
            },
        )];
        assert!(fold(&rows).is_empty());
    }

    #[test]
    fn each_worker_is_numbered_independently_and_densely() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.extend(placed_agent(3, "s1", "agent-2", "worker-b"));
        rows.extend(placed_agent(5, "s2", "agent-3", "worker-a"));

        let issued = fold(&rows);
        let numbering: Vec<(&str, u64)> = issued
            .iter()
            .map(|(worker, response)| (worker.as_str(), response.command_sequence))
            .collect();

        assert_eq!(
            numbering,
            vec![("worker-a", 1), ("worker-b", 1), ("worker-a", 2)],
            "each worker counts its own commands, across sessions"
        );
    }

    /// The property the whole rewrite exists for. A restarted server rebuilds
    /// the fold from the same log, so a worker that acked command 7 before the
    /// restart must still be talking about the same command 7 afterwards. If
    /// the numbering or the ids depended on anything but the log, this fails.
    #[test]
    fn re_folding_the_same_log_reproduces_every_sequence_and_id() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.extend(placed_agent(3, "s2", "agent-2", "worker-a"));
        rows.extend(placed_agent(5, "s1", "agent-3", "worker-b"));

        let identity = |folded: Vec<(String, StreamCommandsResponse)>| {
            folded
                .into_iter()
                .map(|(worker, response)| (worker, response.command_sequence, response.command_id))
                .collect::<Vec<_>>()
        };

        let before = identity(fold(&rows));
        let after = identity(fold(&rows));

        assert_eq!(before, after, "the fold must not depend on when it runs");
        assert_eq!(before.len(), 3, "a vacuous pass here would prove nothing");
    }

    /// The cursor is what lets a stream fold only what is new. It must track
    /// the last row consumed, including rows that issue no command — a cursor
    /// that only advanced on commands would re-fold the gaps forever.
    #[test]
    fn the_cursor_advances_over_rows_that_issue_nothing() {
        let mut projection = CommandProjection::new();
        assert_eq!(projection.row(), 0);

        projection.advance(&row(4, "s1", 1, spawn_event("agent-1", None)));
        assert_eq!(
            projection.row(),
            4,
            "a spawn issues nothing but is consumed"
        );

        projection.advance(&row(
            9,
            "s1",
            2,
            placement_event("agent-1", Some("worker-a"), PlacementState::Placed),
        ));
        assert_eq!(projection.row(), 9);
    }

    /// A payload that is not an `Event` must not stop the fold: the row is
    /// consumed and skipped, or one bad row would wedge every worker's stream.
    #[test]
    fn an_undecodable_payload_is_skipped_rather_than_fatal() {
        let mut projection = CommandProjection::new();
        let garbage = GlobalEvent {
            row: 1,
            event: StoredEvent {
                session_id: "s1".to_owned(),
                sequence: 1,
                recorded_at_unix_ms: 0,
                payload: vec![0xff, 0xff, 0xff],
            },
        };

        assert!(projection.advance(&garbage).is_none());
        assert_eq!(projection.row(), 1, "and the cursor still moves past it");
    }

    #[test]
    fn a_worker_with_no_commands_has_been_issued_none() {
        let projection = CommandProjection::new();
        assert_eq!(projection.issued("never-placed-anything"), 0);
    }

    // ---------------------------------------------------------------------
    // Against a real log.
    // ---------------------------------------------------------------------

    /// Appends the spawn/placement pair the server writes for a placed agent.
    async fn place(log: &EventLog, session: &str, agent: &str, worker: &str) {
        for event in [
            spawn_event(agent, None),
            placement_event(agent, Some(worker), PlacementState::Placed),
        ] {
            log.append(PendingEvent {
                session_id: session.to_owned(),
                payload: event.encode_to_vec(),
                ..PendingEvent::default()
            })
            .await
            .expect("append");
        }
    }

    /// Waits for the next batch, bounded. An unbounded wait turns a defect into
    /// a hang, and a hung suite reports nothing at all.
    async fn next_within(stream: &mut CommandStream) -> Vec<StreamCommandsResponse> {
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.next_batch())
            .await
            .expect("the stream must produce a batch rather than hang")
            .expect("the log outlives the stream")
            .expect("fold")
    }

    #[tokio::test]
    async fn a_stream_replays_commands_already_in_the_log() {
        let log = EventLog::in_memory().expect("in-memory log");
        place(&log, "s1", "agent-1", "worker-a").await;

        let mut stream = CommandStream::open(&log, "worker-a", 1);
        let batch = next_within(&mut stream).await;

        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].command_sequence, 1);
    }

    #[tokio::test]
    async fn a_stream_delivers_only_its_own_workers_commands() {
        let log = EventLog::in_memory().expect("in-memory log");
        place(&log, "s1", "agent-1", "worker-a").await;
        place(&log, "s1", "agent-2", "worker-b").await;

        let mut stream = CommandStream::open(&log, "worker-b", 1);
        let batch = next_within(&mut stream).await;

        assert_eq!(batch.len(), 1, "worker-a's command is not worker-b's");
        assert_eq!(
            batch[0].command_id, "s1:4",
            "worker-b's command was caused by the second placement"
        );
        assert_eq!(
            batch[0].command_sequence, 1,
            "and it is worker-b's first, not the log's second"
        );
    }

    #[tokio::test]
    async fn a_stream_resumes_after_what_the_worker_already_applied() {
        let log = EventLog::in_memory().expect("in-memory log");
        place(&log, "s1", "agent-1", "worker-a").await;
        place(&log, "s1", "agent-2", "worker-a").await;

        let mut stream = CommandStream::open(&log, "worker-a", 2);
        let batch = next_within(&mut stream).await;

        let sequences: Vec<u64> = batch.iter().map(|c| c.command_sequence).collect();
        assert_eq!(sequences, vec![2], "command 1 was already applied");
    }

    /// The live half: a stream with nothing to do must wait, and a placement
    /// appended afterwards must reach it.
    #[tokio::test]
    async fn a_waiting_stream_is_woken_by_a_placement() {
        let log = EventLog::in_memory().expect("in-memory log");
        let mut stream = CommandStream::open(&log, "worker-a", 1);

        let writer = log.clone();
        tokio::spawn(async move {
            place(&writer, "s1", "agent-1", "worker-a").await;
        });

        let batch = next_within(&mut stream).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].command_sequence, 1);
    }

    /// The liveness assertion. A stream with nothing to deliver must *park*,
    /// not spin: a hot loop delivers the right commands and passes every
    /// functional test above while burning a core, and a mutation of the
    /// waiting path shows up as a suite that hangs rather than one that fails.
    /// Parking is counted so the difference is a number a test can read.
    #[tokio::test]
    async fn an_idle_stream_parks_instead_of_spinning() {
        let log = EventLog::in_memory().expect("in-memory log");
        let mut stream = CommandStream::open(&log, "worker-a", 1);
        assert_eq!(stream.parks(), 0, "nothing has waited yet");

        // Nothing will ever arrive, so this must time out *while parked*.
        let idle =
            tokio::time::timeout(std::time::Duration::from_millis(200), stream.next_batch()).await;
        assert!(idle.is_err(), "there is no command, so nothing may arrive");

        assert_eq!(
            stream.parks(),
            1,
            "an idle stream parks exactly once: 0 means it never waited, and a \
             spin drives this into the thousands"
        );
    }

    /// An append that commands another worker must not wake this one into
    /// delivering something that is not its own.
    #[tokio::test]
    async fn an_unrelated_append_does_not_produce_a_command() {
        let log = EventLog::in_memory().expect("in-memory log");
        let mut stream = CommandStream::open(&log, "worker-a", 1);

        place(&log, "s1", "agent-1", "worker-b").await;
        place(&log, "s1", "agent-2", "worker-a").await;

        let batch = next_within(&mut stream).await;
        assert_eq!(batch.len(), 1, "only worker-a's own command");
        assert_eq!(
            batch[0].command_sequence, 1,
            "worker-a numbers from its own first command"
        );
    }

    #[tokio::test]
    async fn what_a_worker_has_been_issued_is_folded_from_the_log() {
        let log = EventLog::in_memory().expect("in-memory log");
        assert_eq!(
            issued_to(&log, "worker-a").await.expect("fold"),
            0,
            "a worker nothing was placed on has been issued nothing"
        );

        place(&log, "s1", "agent-1", "worker-a").await;
        place(&log, "s1", "agent-2", "worker-a").await;
        place(&log, "s1", "agent-3", "worker-b").await;

        assert_eq!(issued_to(&log, "worker-a").await.expect("fold"), 2);
        assert_eq!(issued_to(&log, "worker-b").await.expect("fold"), 1);
    }

    /// A restart is a new fold over the same log. The commands a fresh stream
    /// produces must be identical to what the previous one produced, or a
    /// worker's ack points at a different command than it did before.
    #[tokio::test]
    async fn a_second_stream_over_the_same_log_produces_identical_commands() {
        let log = EventLog::in_memory().expect("in-memory log");
        place(&log, "s1", "agent-1", "worker-a").await;
        place(&log, "s2", "agent-2", "worker-a").await;

        let mut before = CommandStream::open(&log, "worker-a", 1);
        let first = next_within(&mut before).await;

        // Nothing of the first stream survives; only the log does.
        drop(before);
        let mut after = CommandStream::open(&log, "worker-a", 1);
        let second = next_within(&mut after).await;

        assert_eq!(first, second, "the log is the only state there is");
        assert_eq!(first.len(), 2);
    }
}
