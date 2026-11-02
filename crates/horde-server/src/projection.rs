//! The live session projection (ADR-0013).
//!
//! Every session's [`SessionState`], kept in memory and advanced by the
//! events appended since it was last read — instead of each read re-folding
//! every event of every session it looks at. A `ListSessions` used to read
//! the whole log; a placement pass did it again, and a hub call, a forward
//! open and a permission answer each re-folded the sessions they touched.
//!
//! # Still a pure function of the log
//!
//! The projection holds no semantics of its own. Each new event is handed to
//! [`SessionState::apply`] — the very fold [`SessionState::project`] runs —
//! starting from [`SessionState::new`], the very state it starts from. So
//! "the projection after these events" and "a cold re-fold of these events"
//! are the same computation split at different points, which is what the
//! property test below checks against arbitrary logs.
//!
//! # Catching up, not listening
//!
//! The projection does not subscribe to append notifications; it reads the
//! log forward from a cursor (`rowid`, the log's total append order) at the
//! start of every read. That keeps it exactly as fresh as the log for every
//! reader — an append that has returned is folded before the next read
//! answers, whichever clone of the log wrote it and whatever path the write
//! took (single append, ingest batch) — and makes a lagged channel
//! impossible to get wrong. The cost per read is one indexed query for the
//! rows since the last one, usually none. A cold start is the same loop
//! from row zero, run once by [`SessionProjection::rebuild`] at boot.
//!
//! # An undecodable event
//!
//! A cold fold of a session that holds one fails, every time. The projection
//! keeps that answer rather than a state: the session's fold stops at the
//! failure and every read of it reports the same decode error a cold
//! [`SessionState::project`] would.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use crate::error::ServerError;
use crate::log::{EventLog, GlobalEvent};
use crate::state::SessionState;

/// Rows read per catch-up query.
const PAGE: u32 = 512;

/// Every session's state, folded incrementally out of one log.
///
/// Cheap to clone; clones share the one projection.
#[derive(Clone)]
pub struct SessionProjection {
    log: EventLog,
    folded: Arc<tokio::sync::Mutex<Folded>>,
}

impl std::fmt::Debug for SessionProjection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionProjection")
            .finish_non_exhaustive()
    }
}

/// What the projection has folded so far.
#[derive(Default)]
struct Folded {
    /// The last row folded; the next catch-up reads after it.
    cursor: u64,
    /// Every stream with at least one event, in the order its first event
    /// was appended — the order [`EventLog::session_ids`] lists.
    sessions: Vec<Fold>,
    /// Each stream's place in `sessions`.
    index: HashMap<String, usize>,
}

/// One stream's fold: its state, or the decode failure that stopped it.
enum Fold {
    State(SessionState),
    Unfoldable {
        session_id: String,
        error: prost::DecodeError,
    },
}

impl Fold {
    fn session_id(&self) -> &str {
        match self {
            Self::State(state) => &state.session_id,
            Self::Unfoldable { session_id, .. } => session_id,
        }
    }

    /// The state, or the error a cold fold of this stream answers.
    fn outcome(&self) -> Result<&SessionState, ServerError> {
        match self {
            Self::State(state) => Ok(state),
            Self::Unfoldable { error, .. } => Err(ServerError::Decode(error.clone())),
        }
    }
}

impl Folded {
    /// Folds one more row in, starting its stream's fold if this is the
    /// stream's first event.
    fn fold(&mut self, global: &GlobalEvent) {
        self.cursor = global.row;
        let session_id = &global.event.session_id;
        let next = self.sessions.len();
        let place = *self.index.entry(session_id.clone()).or_insert(next);
        if place == next {
            self.sessions
                .push(Fold::State(SessionState::new(session_id)));
        }
        // In bounds: `index` only ever holds positions already pushed.
        let fold = &mut self.sessions[place];
        if let Fold::State(state) = fold
            && let Err(error) = state.apply(&global.event)
        {
            *fold = Fold::Unfoldable {
                session_id: session_id.clone(),
                error,
            };
        }
    }
}

impl SessionProjection {
    /// A projection of `log`, empty until its first read catches it up.
    #[must_use]
    pub fn new(log: EventLog) -> Self {
        Self {
            log,
            folded: Arc::new(tokio::sync::Mutex::new(Folded::default())),
        }
    }

    /// Folds everything the log holds that this projection has not seen —
    /// the whole log, the first time. The boot path calls it so the first
    /// client read does not pay for the cold fold.
    ///
    /// # Errors
    ///
    /// The log failing to read.
    pub async fn rebuild(&self) -> Result<(), ServerError> {
        self.caught_up().await.map(drop)
    }

    /// The projection, advanced to the log's current end and held for the
    /// read that asked.
    ///
    /// Ends on an empty page rather than a short one, for the reason
    /// `EventLog::read_all_from` gives.
    async fn caught_up(&self) -> Result<tokio::sync::MutexGuard<'_, Folded>, ServerError> {
        let mut folded = self.folded.lock().await;
        loop {
            let page = self.log.read_global_from(folded.cursor, PAGE).await?;
            if page.is_empty() {
                return Ok(folded);
            }
            for global in &page {
                folded.fold(global);
            }
        }
    }

    /// One stream's state; `None` when it has no events.
    ///
    /// # Errors
    ///
    /// The log failing to read, or the stream holding an event that does not
    /// decode — the error a cold fold of it answers.
    pub async fn state(&self, session_id: &str) -> Result<Option<SessionState>, ServerError> {
        let folded = self.caught_up().await?;
        folded
            .index
            .get(session_id)
            .and_then(|place| folded.sessions.get(*place))
            .map(|fold| fold.outcome().cloned())
            .transpose()
    }

    /// One stream's state, or the state of a stream with no events yet —
    /// what a cold fold of an empty read answers.
    ///
    /// # Errors
    ///
    /// As [`SessionProjection::state`].
    pub async fn state_or_new(&self, session_id: &str) -> Result<SessionState, ServerError> {
        Ok(self
            .state(session_id)
            .await?
            .unwrap_or_else(|| SessionState::new(session_id)))
    }

    /// Visits every admitted stream's state, oldest stream first, until
    /// `visit` breaks — answering what it broke with.
    ///
    /// `admit` decides by id alone, before the stream's fold is looked at, so
    /// a stream it turns away cannot fail the walk (the reserved streams are
    /// turned away this way). An admitted stream that cannot be folded ends
    /// the walk with its error, exactly where a loop of cold folds would
    /// have failed.
    ///
    /// # Errors
    ///
    /// The log failing to read, or an admitted stream failing to fold.
    pub async fn walk<T>(
        &self,
        admit: impl Fn(&str) -> bool,
        mut visit: impl FnMut(&SessionState) -> ControlFlow<T>,
    ) -> Result<Option<T>, ServerError> {
        let folded = self.caught_up().await?;
        for fold in &folded.sessions {
            if !admit(fold.session_id()) {
                continue;
            }
            if let ControlFlow::Break(found) = visit(fold.outcome()?) {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    /// Every admitted stream's state, oldest first.
    ///
    /// # Errors
    ///
    /// As [`SessionProjection::walk`].
    pub async fn states(
        &self,
        admit: impl Fn(&str) -> bool,
    ) -> Result<Vec<SessionState>, ServerError> {
        let mut states = Vec::new();
        self.walk(admit, |state| {
            states.push(state.clone());
            ControlFlow::<()>::Continue(())
        })
        .await?;
        Ok(states)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;

    use horde_proto::v1::{
        AgentLifecycleState, Event, PlacementState, SessionLifecycleChanged, SessionLifecycleState,
        event::Kind,
    };
    use proptest::prelude::*;
    use prost::Message;

    use super::SessionProjection;
    use crate::log::{EventLog, PendingEvent};
    use crate::state::SessionState;
    use crate::state::fixture::{lifecycle, spawned};

    /// The ceiling on every wait here, so a hang fails fast.
    const BOUND: std::time::Duration = std::time::Duration::from_secs(10);

    fn append(session: &str, payload: Vec<u8>) -> PendingEvent {
        PendingEvent {
            session_id: session.to_owned(),
            payload,
            ..PendingEvent::default()
        }
    }

    fn placed(agent: &str, worker: &str) -> Event {
        Event {
            agent_id: Some(horde_proto::v1::AgentId::new(agent)),
            kind: Some(Kind::PlacementChanged(horde_proto::v1::PlacementChanged {
                placement: Some(horde_proto::v1::Placement {
                    agent_id: Some(horde_proto::v1::AgentId::new(agent)),
                    worker_id: Some(horde_proto::v1::WorkerId::new(worker)),
                    state: PlacementState::Placed as i32,
                    ..horde_proto::v1::Placement::default()
                }),
                ..horde_proto::v1::PlacementChanged::default()
            })),
            ..Event::default()
        }
    }

    fn session_changed(state: SessionLifecycleState) -> Event {
        Event {
            kind: Some(Kind::SessionLifecycleChanged(SessionLifecycleChanged {
                state: state as i32,
                ..SessionLifecycleChanged::default()
            })),
            ..Event::default()
        }
    }

    /// One payload the fold can meet: each event kind the projection models,
    /// an unmodelled one, and bytes that do not decode at all.
    fn payload() -> impl Strategy<Value = Vec<u8>> {
        let agents = prop::sample::select(vec!["a1", "a2", "a3"]);
        let workers = prop::sample::select(vec!["w1", "w2"]);
        prop_oneof![
            (agents.clone(), prop::option::of(agents.clone()))
                .prop_map(|(agent, parent)| spawned(agent, parent).encode_to_vec()),
            (agents.clone(), 0_i32..8).prop_map(|(agent, state)| {
                let state =
                    AgentLifecycleState::try_from(state).unwrap_or(AgentLifecycleState::Running);
                lifecycle(agent, state).encode_to_vec()
            }),
            (agents, workers).prop_map(|(agent, worker)| placed(agent, worker).encode_to_vec()),
            Just(session_changed(SessionLifecycleState::Archived).encode_to_vec()),
            Just(Event::default().encode_to_vec()),
            prop::collection::vec(any::<u8>(), 0..6),
        ]
    }

    /// One step of a run: append to a stream, or read the projection.
    #[derive(Debug, Clone)]
    enum Step {
        Append(&'static str, Vec<u8>),
        Read,
    }

    fn step() -> impl Strategy<Value = Step> {
        prop_oneof![
            4 => (prop::sample::select(vec!["s1", "s2", "s3"]), payload())
                .prop_map(|(session, payload)| Step::Append(session, payload)),
            1 => Just(Step::Read),
        ]
    }

    /// A cold fold of one stream, straight from the log — the reference.
    async fn cold(log: &EventLog, session_id: &str) -> Result<SessionState, String> {
        let events = log.read_all(session_id).await.expect("the log reads");
        SessionState::project(session_id, &events).map_err(|error| error.to_string())
    }

    /// Runs `steps` against a real log, reading the live projection at every
    /// `Read` so it is caught up in arbitrary increments, then compares it,
    /// stream by stream and in listing order, with cold folds.
    async fn incremental_equals_cold(steps: Vec<Step>) {
        let log = EventLog::in_memory().expect("a log");
        let live = SessionProjection::new(log.clone());
        for step in steps {
            match step {
                Step::Append(session, payload) => {
                    log.append(append(session, payload)).await.expect("append");
                }
                Step::Read => {
                    let _ = live.state("s1").await;
                }
            }
        }

        let ids = log.session_ids().await.expect("the ids");
        let mut walked = Vec::new();
        let walk = live
            .walk(
                |_| true,
                |state| {
                    walked.push(state.session_id.clone());
                    ControlFlow::<()>::Continue(())
                },
            )
            .await;
        let mut any_unfoldable = false;
        for session_id in ["s1", "s2", "s3"] {
            let expected = cold(&log, session_id).await;
            any_unfoldable |= expected.is_err();
            let actual = live
                .state(session_id)
                .await
                .map_err(|error| error.to_string());
            if ids.iter().any(|id| id == session_id) {
                assert_eq!(actual.map(Option::unwrap_or_default), expected);
            } else {
                assert_eq!(actual, Ok(None), "no events, no session");
            }
        }
        assert_eq!(
            walk.is_err(),
            any_unfoldable,
            "a walk fails iff a cold fold does"
        );
        if walk.is_ok() {
            assert_eq!(walked, ids, "the walk lists streams as the log does");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// The live projection, caught up in arbitrary increments, equals a
        /// cold re-fold of the same log — states, failures and order alike.
        #[test]
        fn the_live_projection_equals_a_cold_refold(steps in prop::collection::vec(step(), 0..40)) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime");
            runtime.block_on(async {
                tokio::time::timeout(BOUND, incremental_equals_cold(steps))
                    .await
                    .expect("the comparison finishes");
            });
        }
    }

    /// A walk turns streams away by id before looking at them — so an
    /// unfoldable stream it does not admit cannot fail it — stops where the
    /// visitor breaks, and an admitted unfoldable stream fails it.
    #[tokio::test]
    async fn a_walk_admits_by_id_breaks_early_and_fails_on_an_admitted_broken_stream() {
        let log = EventLog::in_memory().expect("a log");
        log.append(append("broken", vec![0xff]))
            .await
            .expect("append");
        log.append(append("s1", spawned("a1", None).encode_to_vec()))
            .await
            .expect("append");
        log.append(append("s2", spawned("a2", None).encode_to_vec()))
            .await
            .expect("append");
        let live = SessionProjection::new(log);

        let mut seen = Vec::new();
        let found = tokio::time::timeout(
            BOUND,
            live.walk(
                |id| id != "broken",
                |state| {
                    seen.push(state.session_id.clone());
                    if state.agents.contains_key("a1") {
                        ControlFlow::Break(state.session_id.clone())
                    } else {
                        ControlFlow::Continue(())
                    }
                },
            ),
        )
        .await
        .expect("bounded")
        .expect("the broken stream is not admitted");
        assert_eq!(found, Some("s1".to_owned()));
        assert_eq!(seen, vec!["s1".to_owned()], "nothing after the break");

        let failed = live.states(|_| true).await;
        assert!(failed.is_err(), "an admitted broken stream fails the walk");
        let states = live.states(|id| id != "broken").await.expect("the rest");
        let ids: Vec<&str> = states
            .iter()
            .map(|state| state.session_id.as_str())
            .collect();
        assert_eq!(ids, vec!["s1", "s2"]);
    }

    /// A stream with no events reads as the state a cold fold of nothing
    /// answers; `rebuild` folds what a later read would have.
    #[tokio::test]
    async fn an_empty_stream_reads_as_new_and_a_rebuild_folds_ahead_of_reads() {
        let log = EventLog::in_memory().expect("a log");
        let live = SessionProjection::new(log.clone());
        assert_eq!(
            live.state_or_new("nowhere").await.expect("a state"),
            SessionState::new("nowhere")
        );
        log.append(append("s1", spawned("a1", None).encode_to_vec()))
            .await
            .expect("append");
        tokio::time::timeout(BOUND, live.rebuild())
            .await
            .expect("bounded")
            .expect("the rebuild");
        assert_eq!(live.folded.lock().await.cursor, 1, "the rebuild advanced");
        assert!(format!("{live:?}").contains("SessionProjection"));
    }
}
