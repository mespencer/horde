//! The permission prompt tool: the seam DESIGN §10's chain hangs from.
//!
//! Claude Code is started with `--permission-prompt-tool` pointed at this
//! endpoint, so a tool call the model wants to make becomes a `tools/call`
//! here, carrying `{tool_name, input, tool_use_id}`. Horde raises it as an
//! ordinary event, waits for the answer, and returns the harness the shape it
//! expects. The harness then runs — or refuses to run — what the answer says.
//!
//! # Blocking here is correct
//!
//! The wait can be long: a human may answer from a phone. Spike W1c measured a
//! 180-second stall answered cleanly with no timeout firing, which is what
//! makes human approval viable at all. So nothing in this module imposes a
//! deadline on how long a decision may take. The bounds that do exist are
//! honest ones:
//!
//! * the request's own `expires_at`, which is authored policy carried as data
//!   and resolves as an *event* — not a stopwatch this process runs;
//! * cancellation, by dropping the future, which drops the subscription;
//! * the stream ending, which is a real failure and reported as one.
//!
//! A clock-based timeout here would convert "the human is thinking" into "the
//! agent was denied", silently, which is the one answer nobody chose.
//!
//! # Subscribe before you write
//!
//! The decision is matched by **identity** — a `PermissionDecided` carrying the
//! `request_id` that was raised — and never by log position. Two reasons, and
//! the second is the one that bites:
//!
//! 1. The stream is opened *before* the request is journaled, so an answer that
//!    lands between the write and the listen cannot be missed. Opening it after
//!    would leave exactly that window.
//! 2. There is no offset to use even if we wanted one. The journal answers with
//!    a per-Worker `journal_sequence`; `SyncEvents` cursors are per-Session
//!    `EventEnvelope.sequence`. They are different number spaces that diverge
//!    the moment a Worker serves two Sessions, and the session sequence does
//!    not exist yet when the append returns — the Server assigns it later, in
//!    `ingest_batch`. Feeding one to the other yields a plausible offset
//!    pointing somewhere arbitrary, with nothing to notice.
//!
//! Because the match is by identity, replaying from the start of the session is
//! harmless and useful: a freshly minted `request_id` cannot collide with
//! history, and a *retried* raise — deduped server-side on the idempotency key
//! — finds the decision that was already answered rather than waiting forever
//! for a second one. That is what makes this wait resumable across a reconnect.

use horde_adapter::permission::PermissionDecision;
use horde_adapter_claude::permission::{PromptRequest, encode_tool_response};
use horde_cli::client::SyncOptions;
use horde_proto::v1::event::Kind as EventKind;

use crate::context::AgentContext;
use crate::identity::AgentIdentity;
use crate::journal::{JournalEntry, JournalError};

/// Why an escalation did not produce a decision.
#[derive(Debug, thiserror::Error)]
pub enum PermissionError {
    /// The request could not be written, so no human will ever see it.
    ///
    /// Surfaced rather than waited on: a wait for an answer to a question that
    /// was never asked does not end.
    #[error("the permission request could not be raised: {source}")]
    NotRaised {
        #[source]
        source: JournalError,
    },

    /// The control plane could not be reached or refused the subscription.
    #[error("could not watch for the decision: {source}")]
    Unwatchable {
        #[source]
        source: horde_cli::client::ClientError,
    },

    /// The event stream ended before any decision arrived.
    ///
    /// Distinct from a denial. The agent is not permitted, but it was not
    /// refused either, and reporting a shutdown as a denial would teach an
    /// operator that the chain said no when it said nothing.
    #[error("the event stream ended before {request_id} was decided")]
    Abandoned { request_id: String },

    /// A decision arrived that cannot be applied as written.
    ///
    /// Never downgraded to a plain allow: a decision that narrowed a call, whose
    /// narrowing could not be carried, would run the original wider call.
    #[error("the decision for {request_id} cannot be applied: {source}")]
    Unusable {
        request_id: String,
        #[source]
        source: horde_wire::permission::ConversionError,
    },
}

/// Raises permission escalations and waits for their answers.
///
/// One per endpoint. Holds the control-plane client it watches through and the
/// journal it writes to — the two planes DESIGN §5 splits, with this type
/// sitting on the seam because a permission escalation touches both: the
/// request is an observed fact (journal), the answer is a human act read back
/// off the log (control plane).
#[derive(Debug)]
pub struct PermissionBroker {
    context: AgentContext,
}

impl PermissionBroker {
    #[must_use]
    pub fn new(context: AgentContext) -> Self {
        Self { context }
    }

    /// Raises the harness's proposed call and blocks until it is decided.
    ///
    /// # Errors
    ///
    /// [`PermissionError`]. Every variant is a refusal to guess: none of them
    /// resolves to a default allow or a default deny, because both defaults are
    /// wrong in a way that hides itself — one widens a permission nobody
    /// granted, the other stalls an agent nobody refused.
    pub async fn adjudicate(
        &self,
        identity: &AgentIdentity,
        prompt: &PromptRequest,
    ) -> Result<PermissionDecision, PermissionError> {
        let session_id = self.context.session_id().to_owned();
        let mut client = self.context.client();
        // The harness's own id for this tool call: unique per call, and stable
        // across a replay of the same call, which is exactly what both the
        // request id and the dedupe key need to be. Minting a fresh random one
        // here would dedupe nothing.
        let request_id = prompt.tool_use_id.clone();

        // Subscribed FIRST. See the module docs — the answer can land between
        // the write and the listen, and from-zero is safe because the match is
        // by identity rather than position.
        let mut stream = client
            .follow(&SyncOptions::new(session_id.clone()))
            .await
            .map_err(|source| PermissionError::Unwatchable { source })?;

        self.context
            .journal()
            .record(JournalEntry {
                session_id,
                agent_id: Some(identity.agent_id().to_owned()),
                idempotency_key: request_id.clone(),
                kind: EventKind::PermissionRequested(requested(identity, prompt, &request_id)),
            })
            .await
            .map_err(|source| PermissionError::NotRaised { source })?;

        let wanted = request_id.clone();
        let decided = stream
            .next_matching(move |envelope| decision_for(envelope, &wanted))
            .await
            .map_err(|source| PermissionError::Unwatchable { source })?
            .ok_or_else(|| PermissionError::Abandoned {
                request_id: request_id.clone(),
            })?;

        // Claude executes whatever `updatedInput` says, so this adapter *can*
        // rewrite — which is what makes a narrowing meaningful here rather than
        // an error. A provider that could not would get `RewriteUnsupported`
        // out of this call rather than a silently widened permission.
        horde_wire::permission::permission_decision_from_wire(&decided, true)
            .map_err(|source| PermissionError::Unusable { request_id, source })
    }

    /// The complete `tools/call` result for the harness.
    ///
    /// Wraps [`Self::adjudicate`] in the Claude-specific encoding, which lives
    /// in the adapter rather than here: the `{behavior, updatedInput}` shape is
    /// that provider's contract, and it already carries the rule that an
    /// unmodified allow must echo the proposal back rather than omit the field.
    ///
    /// # Errors
    ///
    /// [`PermissionError`], for the caller to render as a tool error.
    pub async fn respond(
        &self,
        identity: &AgentIdentity,
        prompt: &PromptRequest,
    ) -> Result<serde_json::Value, PermissionError> {
        let decision = self.adjudicate(identity, prompt).await?;
        Ok(encode_tool_response(&decision, &prompt.input))
    }
}

/// The escalation event, built from the authenticated caller and the proposal.
fn requested(
    identity: &AgentIdentity,
    prompt: &PromptRequest,
    request_id: &str,
) -> horde_proto::v1::PermissionRequested {
    horde_proto::v1::PermissionRequested {
        request: Some(horde_proto::v1::PermissionRequest {
            request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
            // The authenticated identity, never anything the Agent said about
            // itself: an escalation attributed to the wrong agent is one the
            // approver reads the wrong tree path for.
            agent_id: Some(horde_proto::v1::AgentId::new(identity.agent_id())),
            tool_call: Some(horde_proto::v1::ProposedToolCall {
                tool_name: prompt.tool_name.clone(),
                input: as_struct(&prompt.input),
                rendered: rendered(prompt),
            }),
            ..horde_proto::v1::PermissionRequest::default()
        }),
    }
}

/// A one-line rendering of the call, for approval UIs.
///
/// Deliberately not the raw JSON: the approver is answering on a phone, and
/// "Write /etc/hosts" is a decision they can make where forty lines of escaped
/// arguments is not. The full input rides the event beside this.
fn rendered(prompt: &PromptRequest) -> String {
    match &prompt.input {
        serde_json::Value::Object(fields) => {
            let summary = ["file_path", "path", "command", "url", "pattern"]
                .iter()
                .find_map(|key| fields.get(*key).and_then(serde_json::Value::as_str));
            match summary {
                Some(value) => format!("{} {value}", prompt.tool_name),
                None => prompt.tool_name.clone(),
            }
        }
        _ => prompt.tool_name.clone(),
    }
}

/// The proposal as a `Struct`, or empty when it is not an object.
///
/// Tool arguments are a named bag in practice. A non-object proposal is not
/// worth failing the escalation over — the approver still sees the tool name
/// and the rendering — but it must not be smuggled in as if it were fields.
fn as_struct(input: &serde_json::Value) -> Option<horde_proto::prost_types::Struct> {
    let decision = PermissionDecision::allow_with(input.clone());
    horde_wire::permission::permission_decision_to_wire(&decision)
        .ok()
        .and_then(|wire| wire.updated_input)
}

/// The decision in this envelope, if it answers `request_id`.
fn decision_for(
    envelope: &horde_proto::v1::EventEnvelope,
    request_id: &str,
) -> Option<horde_proto::v1::PermissionDecision> {
    let EventKind::PermissionDecided(decided) = envelope
        .event
        .as_ref()
        .and_then(|event| event.kind.as_ref())?
    else {
        return None;
    };
    let decision = decided.decision.as_ref()?;
    let answered = decision.request_id.as_ref().map(ToString::to_string)?;
    (answered == request_id).then(|| decision.clone())
}

#[cfg(test)]
mod tests {
    use super::{as_struct, decision_for, rendered, requested};
    use serde_json::json;

    fn prompt(
        tool: &str,
        input: serde_json::Value,
    ) -> horde_adapter_claude::permission::PromptRequest {
        serde_json::from_value(json!({
            "tool_name": tool,
            "input": input,
            "tool_use_id": "toolu_01",
        }))
        .expect("the prompt shape must parse")
    }

    /// The escalation is attributed to the authenticated caller. An event
    /// without the agent id is one the approver reads no tree path for — and
    /// it is exactly what dropping the field produces.
    #[test]
    fn the_escalation_names_the_authenticated_agent() {
        let event = requested(
            &crate::testing::identity("agent-42"),
            &prompt("Write", json!({"file_path": "/x"})),
            "toolu_01",
        );
        let request = event.request.expect("the escalation carries its request");
        assert_eq!(
            request.agent_id.as_ref().map(ToString::to_string),
            Some("agent-42".to_owned()),
            "the identity's agent id must ride the event"
        );
    }

    /// An object proposal reaches the approver as its own fields.
    #[test]
    fn an_object_proposal_rides_as_its_fields() {
        let fields = as_struct(&json!({"file_path": "/etc/hosts"}))
            .expect("an object proposal must convert")
            .fields;
        assert!(
            fields.contains_key("file_path"),
            "the proposal's own keys must survive the conversion"
        );
    }

    /// A non-object proposal is dropped, not smuggled in as an empty bag: the
    /// approver still gets the tool name and the rendering, but no fields that
    /// were never fields.
    #[test]
    fn a_non_object_proposal_is_not_smuggled_in_as_fields() {
        assert_eq!(as_struct(&json!("rm -rf /")), None);
    }

    fn decided(request_id: &str) -> horde_proto::v1::EventEnvelope {
        crate::testing::envelope(
            "agent-42",
            horde_proto::v1::event::Kind::PermissionDecided(horde_proto::v1::PermissionDecided {
                decision: Some(horde_proto::v1::PermissionDecision {
                    request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
                    outcome: horde_proto::v1::PermissionOutcome::Allow as i32,
                    ..horde_proto::v1::PermissionDecision::default()
                }),
            }),
        )
    }

    /// The whole wait turns on this: a decision for somebody else's request
    /// must not end this agent's block. Two agents escalating at once is the
    /// ordinary case, not the exotic one.
    #[test]
    fn only_the_matching_request_id_answers_the_wait() {
        assert!(decision_for(&decided("toolu_01"), "toolu_01").is_some());
        assert!(
            decision_for(&decided("toolu_02"), "toolu_01").is_none(),
            "another agent's decision must not unblock this one"
        );
    }

    #[test]
    fn events_that_are_not_decisions_are_ignored() {
        let requested = crate::testing::envelope(
            "agent-42",
            horde_proto::v1::event::Kind::PermissionRequested(
                horde_proto::v1::PermissionRequested::default(),
            ),
        );
        assert!(decision_for(&requested, "toolu_01").is_none());
        assert!(
            decision_for(
                &crate::testing::envelope_without_event("agent-42"),
                "toolu_01"
            )
            .is_none(),
            "an envelope with no event must not panic or match"
        );
    }

    /// The approver is answering on a phone. The rendering is what they read.
    #[test]
    fn the_rendering_names_the_thing_being_acted_on() {
        assert_eq!(
            rendered(&prompt(
                "Write",
                json!({"file_path": "/etc/hosts", "content": "x"})
            )),
            "Write /etc/hosts"
        );
        assert_eq!(
            rendered(&prompt("Bash", json!({"command": "docker rm horde-test"}))),
            "Bash docker rm horde-test"
        );
    }

    #[test]
    fn a_call_with_nothing_renderable_still_names_its_tool() {
        assert_eq!(rendered(&prompt("Read", json!({"limit": 5}))), "Read");
        assert_eq!(rendered(&prompt("Read", json!("not-an-object"))), "Read");
    }
}
