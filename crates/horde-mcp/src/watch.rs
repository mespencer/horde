//! One endpoint's view of its Session's log: a cursor, an index, a redial.
//!
//! Every blocking verb on the endpoint — the permission wait, `wait_for_agent`,
//! the codex elicitation lookup — is a question about the Session's log. Each
//! used to answer it with its own follow from sequence zero, which made every
//! prompt cost the whole session's history, and let a single transport error
//! end the wait as an internal error the agent could do nothing with.
//!
//! This module answers them all from one shared [`SessionIndex`], folded
//! forward from a cursor:
//!
//! * **A follow starts at the cursor, never at zero.** History the endpoint has
//!   already folded is not replayed; only what is new crosses the wire.
//! * **A transport failure redials, it does not end the wait.** The stream is
//!   re-opened from the cursor with bounded exponential backoff, so a decision
//!   or a result that lands during a partition is read on the far side of it
//!   rather than lost — DESIGN's partition-survival claim, held at this seam.
//!   Only a refusal the server *meant* (an unknown session, a rejected
//!   credential) ends a wait.
//! * **Questions are asked of a caught-up index.** A probe runs once the
//!   stream's replay has reached the server's head, and after every live event
//!   since — never against a half-replayed prefix, where "the agent's latest
//!   result" could still be followed by an assignment the replay has not
//!   reached yet.
//!
//! No clock bounds the wait itself; that stays the permission module's rule
//! (a human may take minutes to answer). The backoff only paces redials.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use horde_cli::client::{ClientError, StreamUpdate, SyncOptions};
use horde_proto::prost_types::Struct;
use horde_proto::v1::event::Kind as EventKind;
use horde_proto::v1::{AgentResult, EventEnvelope, PermissionDecision};

/// The first redial's delay.
const FIRST_REDIAL: Duration = Duration::from_millis(50);

/// The redial delay never grows past this: a partition healed after an hour is
/// noticed within this long, not after an hour of doubling.
const REDIAL_CEILING: Duration = Duration::from_secs(2);

/// Why a watch ended without its answer.
#[derive(Debug, thiserror::Error)]
#[error("the session log refused the watch: {source}")]
pub struct WatchError {
    #[source]
    pub source: ClientError,
}

/// One `PermissionRequested` the index has seen, kept for correlation.
#[derive(Debug, Clone)]
struct Raised {
    request_id: String,
    agent_id: String,
    tool_name: String,
    input: Option<Struct>,
}

/// Everything the endpoint's blocking verbs ask of the log, folded once.
///
/// Folded strictly forward by sequence: an envelope at or below the cursor has
/// been folded already (by another wait's stream) and is skipped, which is what
/// lets concurrent waits share one index without double-counting.
#[derive(Debug, Default)]
pub struct SessionIndex {
    /// The highest sequence folded.
    cursor: u64,
    /// Every decision in the log, by request id. Decisions are final, so the
    /// first one recorded for an id is the one kept.
    decisions: HashMap<String, PermissionDecision>,
    /// Requests in log order, for [`Self::claim_decided`].
    raised: Vec<Raised>,
    /// Request ids a proxied call has already consumed.
    claimed: HashSet<String>,
    /// Per agent, the sequence of the latest assignment sent to it.
    latest_assignment: HashMap<String, u64>,
    /// Per agent, the latest result it reported and where.
    latest_result: HashMap<String, (u64, AgentResult)>,
}

impl SessionIndex {
    /// Folds one envelope, unless it has been folded already.
    pub fn fold(&mut self, envelope: &EventEnvelope) {
        if envelope.sequence <= self.cursor {
            return;
        }
        self.cursor = envelope.sequence;
        let Some(event) = envelope.event.as_ref() else {
            return;
        };
        let agent_id = event
            .agent_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        match event.kind.as_ref() {
            Some(EventKind::PermissionRequested(requested)) => {
                if let Some(request) = requested.request.as_ref() {
                    self.raised.push(Raised {
                        request_id: request
                            .request_id
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                        agent_id,
                        tool_name: request
                            .tool_call
                            .as_ref()
                            .map(|call| call.tool_name.clone())
                            .unwrap_or_default(),
                        input: request
                            .tool_call
                            .as_ref()
                            .and_then(|call| call.input.clone()),
                    });
                }
            }
            Some(EventKind::PermissionDecided(decided)) => {
                if let Some(decision) = decided.decision.as_ref()
                    && let Some(request_id) = decision.request_id.as_ref()
                {
                    self.decisions
                        .entry(request_id.to_string())
                        .or_insert_with(|| decision.clone());
                }
            }
            Some(EventKind::AssignmentSent(_)) => {
                self.latest_assignment.insert(agent_id, envelope.sequence);
            }
            Some(EventKind::ResultReported(reported)) => {
                if let Some(result) = reported.result.as_ref() {
                    self.latest_result
                        .insert(agent_id, (envelope.sequence, result.clone()));
                }
            }
            _ => {}
        }
    }

    /// The decision answering `request_id`, once there is one.
    #[must_use]
    pub fn decision(&self, request_id: &str) -> Option<PermissionDecision> {
        self.decisions.get(request_id).cloned()
    }

    /// The result `agent_id` owes its latest assignment, once reported.
    ///
    /// A result only answers a wait when no assignment was sent to the agent
    /// after it: an agent that reported, was assigned again, and has not yet
    /// reported again owes a *new* result, and handing back the old one would
    /// tell the parent its second task finished before it started.
    #[must_use]
    pub fn result_owed(&self, agent_id: &str) -> Option<AgentResult> {
        let (reported_at, result) = self.latest_result.get(agent_id)?;
        let assigned_at = self.latest_assignment.get(agent_id).copied().unwrap_or(0);
        (*reported_at > assigned_at).then(|| result.clone())
    }

    /// Claims the oldest decided, unclaimed request `agent_id` raised for
    /// exactly this call — `tool_name` and `input` both equal — returning its
    /// id and decision.
    ///
    /// Claiming is what makes one decision cover one call: a second identical
    /// call cannot reuse a decision the first already consumed, and has to be
    /// asked about in its own right.
    pub fn claim_decided(
        &mut self,
        agent_id: &str,
        tool_name: &str,
        input: Option<&Struct>,
    ) -> Option<(String, PermissionDecision)> {
        let found = self.raised.iter().find(|raised| {
            raised.agent_id == agent_id
                && raised.tool_name == tool_name
                && raised.input.as_ref() == input
                && !self.claimed.contains(&raised.request_id)
                && self.decisions.contains_key(&raised.request_id)
        })?;
        let request_id = found.request_id.clone();
        let decision = self.decisions.get(&request_id)?.clone();
        self.claimed.insert(request_id.clone());
        Some((request_id, decision))
    }
}

/// The shared, redialing view of one Session's log.
///
/// Cheap to clone: every clone shares the one index, so whatever one wait has
/// folded, the next starts after.
#[derive(Clone)]
pub struct SessionWatch {
    session_id: String,
    client: horde_cli::client::Client,
    index: Arc<Mutex<SessionIndex>>,
}

impl std::fmt::Debug for SessionWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionWatch")
            .field("session_id", &self.session_id)
            .field("cursor", &self.cursor())
            .finish_non_exhaustive()
    }
}

impl SessionWatch {
    #[must_use]
    pub fn new(client: horde_cli::client::Client, session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            client,
            index: Arc::new(Mutex::new(SessionIndex::default())),
        }
    }

    /// The highest sequence folded so far.
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.with_index(|index| index.cursor)
    }

    /// Runs `f` against the index. Never held across an await.
    pub fn with_index<T>(&self, f: impl FnOnce(&mut SessionIndex) -> T) -> T {
        // A poisoned lock means another wait panicked mid-fold. The index is
        // still a valid prefix of the log (a fold mutates one entry at a time),
        // so it is read rather than abandoned.
        let mut index = self.index.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut index)
    }

    /// Folds the log up to the server's head.
    ///
    /// # Errors
    ///
    /// [`WatchError`] when the server refuses the watch outright.
    pub async fn catch_up(&self) -> Result<(), WatchError> {
        self.until(|_| Some(())).await
    }

    /// Follows the log until `probe` finds its answer in the caught-up index.
    ///
    /// # Errors
    ///
    /// [`WatchError`] when the server refuses the watch outright. A transport
    /// failure or a stream that ends is redialed from the cursor instead.
    pub async fn until<T>(
        &self,
        mut probe: impl FnMut(&mut SessionIndex) -> Option<T>,
    ) -> Result<T, WatchError> {
        let mut delay = FIRST_REDIAL;
        loop {
            let mut progressed = false;
            match self.attempt(&mut probe, &mut progressed).await {
                Ok(Some(found)) => return Ok(found),
                Ok(None) => tracing::warn!(
                    session_id = self.session_id,
                    "the session log stream ended; redialing from the cursor"
                ),
                Err(source) if !redialable(&source) => return Err(WatchError { source }),
                Err(error) => tracing::warn!(
                    session_id = self.session_id,
                    %error,
                    "the session log stream failed; redialing from the cursor"
                ),
            }
            if progressed {
                delay = FIRST_REDIAL;
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(REDIAL_CEILING);
        }
    }

    /// One stream, from the cursor: `Ok(None)` when it ended unanswered.
    async fn attempt<T>(
        &self,
        probe: &mut impl FnMut(&mut SessionIndex) -> Option<T>,
        progressed: &mut bool,
    ) -> Result<Option<T>, ClientError> {
        let options = SyncOptions::new(self.session_id.clone())
            .from_sequence(self.cursor().saturating_add(1))
            .following(true);
        let mut stream = self.client.clone().follow(&options).await?;
        let mut caught_up = false;
        while let Some(update) = stream.next_update().await? {
            *progressed = true;
            let found = match update {
                StreamUpdate::Event(envelope) => self.with_index(|index| {
                    index.fold(&envelope);
                    caught_up.then(|| probe(index)).flatten()
                }),
                StreamUpdate::CaughtUp { .. } => {
                    caught_up = true;
                    self.with_index(|index| probe(index))
                }
                StreamUpdate::Heartbeat => None,
            };
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }
}

/// Whether a failure is the network's (redial) or the server's verdict (stop).
///
/// The verdicts are the statuses a server returns on purpose: the session or
/// agent does not exist, the request was malformed, the caller is not who it
/// must be. Everything else — unreachable, reset, an h2 error surfacing as
/// `Unknown` or `Internal` — is the transport, and a redial is the answer.
fn redialable(error: &ClientError) -> bool {
    match error {
        ClientError::Connect { .. } => true,
        ClientError::Rpc(status) => !matches!(
            status.code(),
            tonic::Code::NotFound
                | tonic::Code::InvalidArgument
                | tonic::Code::PermissionDenied
                | tonic::Code::Unauthenticated
                | tonic::Code::FailedPrecondition
                | tonic::Code::Unimplemented
                | tonic::Code::OutOfRange
                | tonic::Code::AlreadyExists
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionIndex, redialable};
    use horde_cli::client::ClientError;
    use horde_proto::v1::event::Kind;

    fn at(sequence: u64, agent: &str, kind: Kind) -> horde_proto::v1::EventEnvelope {
        let mut envelope = crate::testing::envelope(agent, kind);
        envelope.sequence = sequence;
        envelope
    }

    fn assigned(sequence: u64, agent: &str) -> horde_proto::v1::EventEnvelope {
        at(sequence, agent, Kind::AssignmentSent(Default::default()))
    }

    fn reported(sequence: u64, agent: &str, summary: &str) -> horde_proto::v1::EventEnvelope {
        at(
            sequence,
            agent,
            Kind::ResultReported(horde_proto::v1::ResultReported {
                result: Some(horde_proto::v1::AgentResult {
                    summary: summary.to_owned(),
                    ..Default::default()
                }),
            }),
        )
    }

    fn raised(
        sequence: u64,
        agent: &str,
        request_id: &str,
        tool: &str,
    ) -> horde_proto::v1::EventEnvelope {
        at(
            sequence,
            agent,
            Kind::PermissionRequested(horde_proto::v1::PermissionRequested {
                request: Some(horde_proto::v1::PermissionRequest {
                    request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
                    tool_call: Some(horde_proto::v1::ProposedToolCall {
                        tool_name: tool.to_owned(),
                        input: Some(Default::default()),
                        rendered: String::new(),
                    }),
                    ..Default::default()
                }),
            }),
        )
    }

    fn decided(sequence: u64, request_id: &str, reason: &str) -> horde_proto::v1::EventEnvelope {
        at(
            sequence,
            "",
            Kind::PermissionDecided(horde_proto::v1::PermissionDecided {
                decision: Some(horde_proto::v1::PermissionDecision {
                    request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
                    reason: reason.to_owned(),
                    ..Default::default()
                }),
            }),
        )
    }

    fn summary(index: &SessionIndex, agent: &str) -> Option<String> {
        index.result_owed(agent).map(|result| result.summary)
    }

    /// The stale-result bug, pinned: after a second assignment the first
    /// result is history, not the answer.
    #[test]
    fn a_result_answers_only_the_assignment_it_follows() {
        let mut index = SessionIndex::default();
        index.fold(&assigned(1, "child"));
        assert_eq!(summary(&index, "child"), None, "nothing reported yet");
        index.fold(&reported(2, "child", "first"));
        assert_eq!(summary(&index, "child").as_deref(), Some("first"));

        index.fold(&assigned(3, "child"));
        assert_eq!(
            summary(&index, "child"),
            None,
            "the second assignment owes a new result"
        );
        index.fold(&reported(4, "sibling", "theirs"));
        assert_eq!(
            summary(&index, "child"),
            None,
            "a sibling's result is not it"
        );
        index.fold(&reported(5, "child", "second"));
        assert_eq!(summary(&index, "child").as_deref(), Some("second"));
    }

    /// An agent spawned and reporting without any recorded assignment still
    /// answers: nothing newer is owed.
    #[test]
    fn a_result_with_no_assignment_on_record_answers() {
        let mut index = SessionIndex::default();
        index.fold(&reported(1, "child", "done"));
        assert_eq!(summary(&index, "child").as_deref(), Some("done"));
    }

    /// A result event without a body is malformed, not an answer.
    #[test]
    fn an_empty_result_answers_nothing() {
        let mut index = SessionIndex::default();
        index.fold(&at(
            1,
            "child",
            Kind::ResultReported(horde_proto::v1::ResultReported { result: None }),
        ));
        assert_eq!(summary(&index, "child"), None);
    }

    /// An envelope at or below the cursor was folded by another stream; folding
    /// it again would re-raise a request or rewind an agent's result.
    #[test]
    fn an_already_folded_sequence_is_skipped() {
        let mut index = SessionIndex::default();
        index.fold(&assigned(1, "child"));
        index.fold(&reported(5, "child", "new"));
        index.fold(&reported(3, "child", "replayed"));
        index.fold(&reported(5, "child", "duplicate"));
        assert_eq!(summary(&index, "child").as_deref(), Some("new"));
        assert_eq!(index.cursor, 5);

        let mut fresh = SessionIndex::default();
        fresh.fold(&crate::testing::envelope_without_event("child"));
        assert_eq!(fresh.cursor, 1, "a malformed row still advances the cursor");
    }

    /// Decisions are final: the first one for an id is the one kept, and a
    /// decision with no request id answers nothing.
    #[test]
    fn the_first_decision_for_a_request_is_the_one_kept() {
        let mut index = SessionIndex::default();
        index.fold(&decided(1, "req-1", "first"));
        index.fold(&decided(2, "req-1", "second"));
        assert_eq!(
            index.decision("req-1").map(|decision| decision.reason),
            Some("first".to_owned())
        );
        assert_eq!(index.decision("req-2"), None);

        let mut anonymous = decided(3, "req-3", "nobody");
        if let Some(event) = anonymous.event.as_mut()
            && let Some(Kind::PermissionDecided(decided)) = event.kind.as_mut()
            && let Some(decision) = decided.decision.as_mut()
        {
            decision.request_id = None;
        }
        index.fold(&anonymous);
        assert_eq!(index.decision(""), None);
    }

    /// One decision covers one call: claimed once, by the right agent, for the
    /// right tool, only once decided.
    #[test]
    fn a_decided_request_is_claimed_once_by_its_own_call() {
        let mut index = SessionIndex::default();
        let empty = horde_proto::prost_types::Struct::default();
        index.fold(&raised(1, "agent-a", "item-1", "horde/github__x"));
        assert_eq!(
            index.claim_decided("agent-a", "horde/github__x", Some(&empty)),
            None,
            "an undecided request is not claimable"
        );
        index.fold(&decided(2, "item-1", "ok"));

        assert_eq!(
            index.claim_decided("agent-b", "horde/github__x", Some(&empty)),
            None,
            "another agent's decision covers nothing of this agent's"
        );
        assert_eq!(
            index.claim_decided("agent-a", "horde/github__y", Some(&empty)),
            None,
            "a decision for one tool covers no other"
        );
        assert_eq!(
            index.claim_decided("agent-a", "horde/github__x", None),
            None,
            "different arguments are a different call"
        );
        assert_eq!(
            index
                .claim_decided("agent-a", "horde/github__x", Some(&empty))
                .map(|(id, decision)| (id, decision.reason)),
            Some(("item-1".to_owned(), "ok".to_owned()))
        );
        assert_eq!(
            index.claim_decided("agent-a", "horde/github__x", Some(&empty)),
            None,
            "a consumed decision does not cover a second call"
        );
    }

    /// A request journaled with no tool call still folds, as a nameless entry
    /// nothing can claim.
    #[test]
    fn a_request_without_a_tool_call_claims_nothing() {
        let mut index = SessionIndex::default();
        index.fold(&at(
            1,
            "agent-a",
            Kind::PermissionRequested(horde_proto::v1::PermissionRequested {
                request: Some(horde_proto::v1::PermissionRequest::default()),
            }),
        ));
        index.fold(&decided(2, "", "blank"));
        assert_eq!(
            index.claim_decided("agent-a", "", None).map(|(id, _)| id),
            Some(String::new()),
            "the fold keeps what the row said, blanks included"
        );
        index.fold(&at(
            3,
            "agent-a",
            Kind::PermissionRequested(horde_proto::v1::PermissionRequested { request: None }),
        ));
        assert_eq!(index.raised.len(), 1, "a request-less row adds nothing");
    }

    /// The network's failures redial; the server's verdicts end the wait.
    #[test]
    fn transport_failures_redial_and_verdicts_do_not() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::Unknown,
            tonic::Code::Internal,
            tonic::Code::Cancelled,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Aborted,
            tonic::Code::ResourceExhausted,
        ] {
            assert!(
                redialable(&ClientError::Rpc(tonic::Status::new(code, "x"))),
                "{code:?} is the transport"
            );
        }
        for code in [
            tonic::Code::NotFound,
            tonic::Code::InvalidArgument,
            tonic::Code::PermissionDenied,
            tonic::Code::Unauthenticated,
            tonic::Code::FailedPrecondition,
            tonic::Code::Unimplemented,
            tonic::Code::OutOfRange,
            tonic::Code::AlreadyExists,
        ] {
            assert!(
                !redialable(&ClientError::Rpc(tonic::Status::new(code, "x"))),
                "{code:?} is a verdict"
            );
        }
        assert!(!redialable(&ClientError::Endpoint {
            endpoint: "nope".to_owned()
        }));
    }

    /// A server that cannot be reached at all is the network too.
    #[tokio::test]
    async fn an_unreachable_server_redials() {
        let error = horde_cli::client::Client::connect("http://127.0.0.1:1")
            .await
            .expect_err("nothing listens on port 1");
        assert!(matches!(error, ClientError::Connect { .. }), "{error}");
        assert!(redialable(&error));
    }
}
