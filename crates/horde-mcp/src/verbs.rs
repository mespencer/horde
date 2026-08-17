//! The orchestrator verbs, as an Agent sees them.
//!
//! `spawn_agent`, `send_message`, `report_result`, `wait_for_agent` — the whole
//! agent-facing vocabulary of DESIGN §5, which is deliberately four verbs and
//! not a taxonomy.
//!
//! # The verbs do not all go to the same place
//!
//! Which plane a verb belongs to is a design commitment, not an implementation
//! convenience, and it is the reason this module holds both a control-plane
//! client and a journal:
//!
//! * **`spawn_agent` and `send_message` are requests for orchestration.**
//!   Placement and delivery are Server decisions — where an agent runs, whether
//!   a message waits for a turn boundary — so they are RPCs, and the Server's
//!   answer is the outcome.
//! * **`report_result` is an observed fact.** It goes into the log through the
//!   Worker's journal, because DESIGN §5 prefers the orchestrator observing
//!   what happened over Agents declaring it.
//! * **`wait_for_agent` is a read.** It watches the log for the result it is
//!   waiting on, matched by the agent that owes it.
//!
//! # What `report_result` can and cannot say
//!
//! An Agent reports a status and a summary. It does **not** report its commits.
//! The Worker auto-detects the branch and commits and stamps them into the
//! result's payload (§5, "prefer the orchestrator observing what happened over
//! agents declaring what happened"), because only the Worker can see the
//! workspace — an endpoint that took the Agent's word for its own output would
//! be exactly the declaring path that principle rules out. So the payload is
//! left unset here, for the Worker to fill. That is a seam, not an omission.

use horde_cli::client::SyncOptions;
use horde_proto::v1::event::Kind as EventKind;
use horde_proto::v1::{DeliveryMode, ResultStatus};

use crate::context::AgentContext;
use crate::identity::AgentIdentity;
use crate::journal::{JournalEntry, JournalError};

/// Why a verb did not do what it said.
#[derive(Debug, thiserror::Error)]
pub enum VerbError {
    /// The control plane refused, or could not be reached.
    #[error("{source}")]
    ControlPlane {
        #[source]
        source: horde_cli::client::ClientError,
    },

    /// The result could not be journaled.
    #[error("the result could not be recorded: {source}")]
    NotRecorded {
        #[source]
        source: JournalError,
    },

    /// The stream ended before the awaited agent reported.
    ///
    /// Distinct from "it reported failure": the agent may still be working, and
    /// telling a parent its child failed when the connection dropped would
    /// invent a result nobody produced.
    #[error("the event stream ended before {agent_id} reported a result")]
    Abandoned { agent_id: String },

    /// The verb is understood but its backend is not built.
    ///
    /// An honest error rather than a fabricated success. An unbuilt verb is a
    /// roadmap fact; a stubbed one that returns `ok` is a lie an Agent will act
    /// on.
    #[error("{verb} is not built yet: {because}")]
    Unbuilt {
        verb: &'static str,
        because: &'static str,
    },
}

impl From<horde_cli::client::ClientError> for VerbError {
    fn from(source: horde_cli::client::ClientError) -> Self {
        Self::ControlPlane { source }
    }
}

/// What a message was: one bit, declared by the sender.
///
/// DESIGN §5 makes this a bit rather than a taxonomy on purpose. An assignment
/// owes exactly one result; a note owes nothing. Everything else about
/// "what kind of message is this" is the sender's prose, not the protocol's
/// business.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    /// Expects exactly one result.
    Assign,
    /// Chat, context, status. No result owed.
    Note,
}

/// What a spawn produced.
///
/// A spawn **never blocks**: an unmatched one comes back `queued` carrying the
/// reason, and the spawner decides whether to wait, relax its constraints, or
/// give up (DESIGN §6). So the placement is part of the answer rather than
/// something to hide behind a retry loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnOutcome {
    pub agent_id: String,
    /// `false` when the scheduler had nowhere to put it.
    pub placed: bool,
    /// Why it is queued, when it is — "no online worker with tag `gpu`".
    pub reason: String,
}

/// The orchestrator verbs for one Agent's endpoint.
#[derive(Debug)]
pub struct Verbs {
    context: AgentContext,
}

impl Verbs {
    #[must_use]
    pub fn new(context: AgentContext) -> Self {
        Self { context }
    }

    /// Spawns a child under the calling Agent, carrying `task` as the
    /// child's initial assignment.
    ///
    /// The task rides the spec rather than being sent afterwards: the spawn's
    /// implicit first assignment is what the worker turns into the child's
    /// first turn input, and a separate send could race the placement. An
    /// empty task spawns an idle child, honestly — some parents assign later.
    ///
    /// # Errors
    ///
    /// [`VerbError::ControlPlane`] if the Server refused. A *queued* spawn is
    /// not an error — it is a successful spawn with nowhere to run yet.
    pub async fn spawn_agent(
        &self,
        identity: &AgentIdentity,
        task: &str,
        idempotency_key: &str,
    ) -> Result<SpawnOutcome, VerbError> {
        let spec = horde_proto::v1::AgentSpec {
            initial_assignment: (!task.is_empty()).then(|| horde_proto::v1::Assignment {
                assignment_id: None,
                text: task.to_owned(),
                payload: None,
            }),
            ..horde_proto::v1::AgentSpec::default()
        };
        // The parent is the authenticated caller, never a value the Agent
        // supplied: ancestry an Agent could forge is ancestry that cannot be
        // used to attenuate capabilities down the tree (§5).
        let response = self
            .context
            .client()
            .spawn_agent_with_spec(
                self.context.session_id(),
                Some(identity.agent_id()),
                spec,
                idempotency_key,
            )
            .await?;

        let placement = response.placement.as_ref();
        Ok(SpawnOutcome {
            agent_id: response
                .agent_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            placed: placement.is_some_and(|placement| {
                placement.state == i32::from(horde_proto::v1::PlacementState::Placed)
            }),
            reason: placement.map(|p| p.reason.clone()).unwrap_or_default(),
        })
    }

    /// Sends an assignment or a note to another Agent.
    ///
    /// # Errors
    ///
    /// [`VerbError::ControlPlane`] if the Server refused.
    pub async fn send_message(
        &self,
        to_agent_id: &str,
        kind: MessageKind,
        text: &str,
        payload: Option<horde_proto::v1::Payload>,
        interrupt: bool,
        idempotency_key: &str,
    ) -> Result<u64, VerbError> {
        // Two verbs, no ambiguity (§11): a steering message waits for the turn
        // boundary, an interrupt cancels the turn and delivers now.
        let delivery = if interrupt {
            DeliveryMode::Interrupt
        } else {
            DeliveryMode::TurnBoundary
        };

        let mut client = self.context.client();
        let response = match kind {
            MessageKind::Assign => {
                client
                    .send_assignment_with_payload(
                        to_agent_id,
                        text,
                        payload,
                        delivery,
                        idempotency_key,
                    )
                    .await?
            }
            MessageKind::Note => {
                client
                    .send_note_with_payload(to_agent_id, text, payload, delivery, idempotency_key)
                    .await?
            }
        };
        Ok(response.sequence)
    }

    /// Reports the calling Agent's result.
    ///
    /// Reporting does not end the Agent: it goes idle with context retained and
    /// is revivable by another message (§5). Archiving is what ends it.
    ///
    /// # `idempotency_key` must be unique across the Session, not just this verb
    ///
    /// The Server dedupes on `(session_id, idempotency_key)` **without
    /// consulting the event kind**, so a key already used by *any* event in this
    /// Session — a spawn, a message, another result — makes this one a
    /// duplicate. It is then dropped silently: the write is acked, this returns
    /// `Ok`, and nothing appears in the log. A parent waiting on the result then
    /// waits forever, with no error anywhere to explain it.
    ///
    /// This is not hypothetical; it cost an afternoon here. Mint the key from
    /// something already unique to the event, the way the permission raise uses
    /// the harness's own `tool_use_id`.
    ///
    /// # Errors
    ///
    /// [`VerbError::NotRecorded`] if the journal would not take it. Never
    /// swallowed: a result that was not written is one the parent will wait for
    /// forever.
    pub async fn report_result(
        &self,
        identity: &AgentIdentity,
        status: ResultStatus,
        summary: &str,
        idempotency_key: &str,
    ) -> Result<u64, VerbError> {
        self.context
            .journal()
            .record(JournalEntry {
                session_id: self.context.session_id().to_owned(),
                agent_id: Some(identity.agent_id().to_owned()),
                idempotency_key: idempotency_key.to_owned(),
                kind: EventKind::ResultReported(horde_proto::v1::ResultReported {
                    result: Some(horde_proto::v1::AgentResult {
                        assignment_id: None,
                        status: status as i32,
                        summary: summary.to_owned(),
                        // The Worker's to stamp, not the Agent's to claim. See
                        // the module docs.
                        payload: None,
                    }),
                }),
            })
            .await
            .map_err(|source| VerbError::NotRecorded { source })
    }

    /// Blocks until `agent_id` reports a result.
    ///
    /// Same discipline as the permission wait: subscribe first, match by
    /// identity, and impose no clock on progress. A child may legitimately take
    /// hours, and a timeout here would report "no result" for an agent that is
    /// still working — which a parent would act on.
    ///
    /// # Errors
    ///
    /// [`VerbError::Abandoned`] if the stream ended first, which is explicitly
    /// not the same as a failed result.
    pub async fn wait_for_agent(
        &self,
        agent_id: &str,
    ) -> Result<horde_proto::v1::AgentResult, VerbError> {
        // Subscribed before anything is read, so a result that lands while this
        // is being set up is still seen. Replaying from the start is safe
        // because the match is on which agent reported, and a result already in
        // history is the answer rather than a stale one.
        let mut stream = self
            .context
            .client()
            .follow(&SyncOptions::new(self.context.session_id().to_owned()))
            .await?;

        let wanted = agent_id.to_owned();
        stream
            .next_matching(move |envelope| result_from(envelope, &wanted))
            .await?
            .ok_or_else(|| VerbError::Abandoned {
                agent_id: agent_id.to_owned(),
            })
    }
}

/// The result in this envelope, if `agent_id` is the one that reported it.
fn result_from(
    envelope: &horde_proto::v1::EventEnvelope,
    agent_id: &str,
) -> Option<horde_proto::v1::AgentResult> {
    let event = envelope.event.as_ref()?;
    let EventKind::ResultReported(reported) = event.kind.as_ref()? else {
        return None;
    };
    // `Event.agent_id` is the *reporting* agent, which is what makes this a
    // safe match: a result reported by somebody else's child must not end this
    // wait.
    let reporter = event.agent_id.as_ref().map(ToString::to_string)?;
    (reporter == agent_id).then(|| reported.result.clone())?
}

#[cfg(test)]
mod tests {
    use super::{MessageKind, VerbError, result_from};
    use horde_proto::v1::ResultStatus;

    fn reported(agent: &str, summary: &str) -> horde_proto::v1::EventEnvelope {
        crate::testing::envelope(
            agent,
            horde_proto::v1::event::Kind::ResultReported(horde_proto::v1::ResultReported {
                result: Some(horde_proto::v1::AgentResult {
                    assignment_id: None,
                    status: ResultStatus::Success as i32,
                    summary: summary.to_owned(),
                    payload: None,
                }),
            }),
        )
    }

    /// The property the whole wait turns on. A tree has many children reporting
    /// concurrently, and a parent waiting on one of them must not be woken by
    /// another.
    #[test]
    fn only_the_awaited_agents_result_ends_the_wait() {
        assert_eq!(
            result_from(&reported("child-1", "done"), "child-1").map(|r| r.summary),
            Some("done".to_owned())
        );
        assert!(
            result_from(&reported("child-2", "done"), "child-1").is_none(),
            "a sibling's result must not end this wait"
        );
    }

    #[test]
    fn a_result_reported_with_no_result_body_does_not_end_the_wait() {
        // An empty ResultReported is a malformed event, not an answer. Treating
        // it as one would hand the parent a default-everything result.
        let mut envelope = reported("child-1", "done");
        if let Some(event) = envelope.event.as_mut()
            && let Some(horde_proto::v1::event::Kind::ResultReported(reported)) =
                event.kind.as_mut()
        {
            reported.result = None;
        }
        assert!(result_from(&envelope, "child-1").is_none());
    }

    #[test]
    fn events_that_are_not_results_are_ignored() {
        let requested = crate::testing::envelope(
            "child-1",
            horde_proto::v1::event::Kind::PermissionRequested(
                horde_proto::v1::PermissionRequested::default(),
            ),
        );
        assert!(result_from(&requested, "child-1").is_none());
        assert!(
            result_from(
                &crate::testing::envelope_without_event("child-1"),
                "child-1"
            )
            .is_none(),
            "an envelope with no event must not panic or match"
        );
    }

    /// One bit, declared by the sender — the distinction the whole messaging
    /// model rests on.
    #[test]
    fn an_assignment_and_a_note_are_different_things() {
        assert_ne!(MessageKind::Assign, MessageKind::Note);
    }

    #[test]
    fn an_unbuilt_verb_says_so_rather_than_fabricating_success() {
        let error = VerbError::Unbuilt {
            verb: "list_agents",
            because: "the query surface is not built",
        };
        assert!(error.to_string().contains("list_agents"));
        assert!(error.to_string().contains("not built yet"));
    }
}
