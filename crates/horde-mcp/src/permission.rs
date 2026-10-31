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
//! # Matched by identity, read from a cursor
//!
//! The decision is matched by **identity** — a `PermissionDecided` carrying the
//! `request_id` that was raised — and never by log position. The journal
//! answers with a per-Worker `journal_sequence`, while the log's cursors are
//! per-Session `EventEnvelope.sequence`; the two diverge the moment a Worker
//! serves two Sessions, so there is no position to match on even if one wanted
//! to.
//!
//! The wait reads the endpoint's shared [`SessionWatch`](crate::watch::SessionWatch):
//! it follows from the cursor the endpoint has already folded to — never from
//! zero — so a prompt costs only what is new in the log. Because the watch
//! starts *behind* the write rather than at the live head, an answer that lands
//! between the write and the listen cannot be missed. Because every decision
//! the watch has ever folded stays indexed, a *retried* raise — deduped
//! server-side on the idempotency key — finds the decision already answered
//! rather than waiting forever for a second one. And because the watch redials
//! from its cursor on a transport failure, the wait survives a reconnect: a
//! human who answers during a partition is heard once it heals.

use horde_adapter::permission::PermissionDecision;
use horde_adapter_claude::permission::{PromptRequest, encode_tool_response};
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

    /// The control plane refused to let the decision be watched.
    ///
    /// Only a refusal the server meant ends the wait this way; a transport
    /// failure is redialed from the cursor instead (see the module docs).
    #[error("could not watch for the decision: {source}")]
    Unwatchable {
        #[source]
        source: crate::watch::WatchError,
    },

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
        self.adjudicate_proposal(identity, prompt, None).await
    }

    /// [`Self::adjudicate`], with a forward spec riding the raise (ADR-0006).
    ///
    /// The spec on the request is what turns an ALLOW into a journaled
    /// `ForwardOpened` server-side: an expose flows the chain's modes like
    /// any proposed action, and an inject escalates to the inbox where the
    /// first accepting client becomes the backer. This endpoint only raises
    /// and waits, exactly as for every other proposal.
    ///
    /// # Errors
    ///
    /// [`PermissionError`], same as [`Self::adjudicate`].
    pub async fn adjudicate_forward(
        &self,
        identity: &AgentIdentity,
        prompt: &PromptRequest,
        forward: horde_proto::v1::ForwardSpec,
    ) -> Result<PermissionDecision, PermissionError> {
        self.adjudicate_proposal(identity, prompt, Some(forward))
            .await
    }

    async fn adjudicate_proposal(
        &self,
        identity: &AgentIdentity,
        prompt: &PromptRequest,
        forward: Option<horde_proto::v1::ForwardSpec>,
    ) -> Result<PermissionDecision, PermissionError> {
        let session_id = self.context.session_id().to_owned();
        // The harness's own id for this tool call: unique per call, and stable
        // across a replay of the same call, which is exactly what both the
        // request id and the dedupe key need to be. Minting a fresh random one
        // here would dedupe nothing.
        let request_id = prompt.tool_use_id.clone();

        self.context
            .journal()
            .record(JournalEntry {
                session_id,
                agent_id: Some(identity.agent_id().to_owned()),
                idempotency_key: request_id.clone(),
                kind: EventKind::PermissionRequested(requested(
                    identity,
                    prompt,
                    &request_id,
                    forward,
                )),
            })
            .await
            .map_err(|source| PermissionError::NotRaised { source })?;

        let decided = self
            .context
            .watch()
            .until(|index| index.decision(&request_id))
            .await
            .map_err(|source| PermissionError::Unwatchable { source })?;
        domain_decision(&decided, request_id)
    }

    /// The decision the chain already gave this exact call at an earlier
    /// gate, consumed so it covers this call and no other.
    ///
    /// The codex half of "one tool call, one gate": codex asks before every
    /// MCP tool call through an elicitation the Worker raises under the item
    /// id and `{server}/{tool}` (ADR-0005), and its `tools/call` that follows
    /// carries no id the endpoint could correlate by. What it can match is the
    /// call itself — the same agent, the same canonical tool name, the same
    /// arguments, already decided — and a match means the chain has spoken on
    /// this call. `None` means it has not, and the caller gates the call
    /// itself, so the chain stays the authority over every call that reaches
    /// a proxied tool, whether or not codex asked first.
    ///
    /// # Errors
    ///
    /// [`PermissionError`], as for [`Self::adjudicate`].
    pub async fn decided_earlier(
        &self,
        identity: &AgentIdentity,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Result<Option<PermissionDecision>, PermissionError> {
        let watch = self.context.watch();
        watch
            .catch_up()
            .await
            .map_err(|source| PermissionError::Unwatchable { source })?;
        let input = as_struct(input);
        watch
            .with_index(|index| index.claim_decided(identity.agent_id(), tool_name, input.as_ref()))
            .map(|(request_id, decided)| domain_decision(&decided, request_id))
            .transpose()
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
    forward: Option<horde_proto::v1::ForwardSpec>,
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
            forward,
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

/// A decision off the log, in the domain's terms.
///
/// Claude executes whatever `updatedInput` says, so this adapter *can* rewrite
/// — which is what makes a narrowing meaningful here rather than an error. A
/// provider that could not would get `RewriteUnsupported` out of this call
/// rather than a silently widened permission.
fn domain_decision(
    decided: &horde_proto::v1::PermissionDecision,
    request_id: String,
) -> Result<PermissionDecision, PermissionError> {
    horde_wire::permission::permission_decision_from_wire(decided, true)
        .map_err(|source| PermissionError::Unusable { request_id, source })
}

#[cfg(test)]
mod tests {
    use super::{as_struct, rendered, requested};
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

    /// One `Write` escalation from agent-42, unwrapped to its request — the
    /// arrangement every field-attribution test below shares.
    fn escalation() -> horde_proto::v1::PermissionRequest {
        requested(
            &crate::testing::identity("agent-42"),
            &prompt("Write", json!({"file_path": "/x"})),
            "toolu_01",
            None,
        )
        .request
        .expect("the escalation carries its request")
    }

    /// A forward raise carries its spec, and an ordinary raise carries none:
    /// the spec on the request is the entire difference between "a tool call
    /// was allowed" and "a forward was opened" server-side.
    #[test]
    fn a_forward_raise_carries_the_spec_the_acceptance_will_open() {
        let spec = horde_proto::v1::ForwardSpec {
            direction: horde_proto::v1::ForwardDirection::Expose as i32,
            name: "vite".to_owned(),
            worker_port: 5173,
            client_port: 0,
        };
        let raised = requested(
            &crate::testing::identity("agent-42"),
            &prompt("expose", json!({"port": "5173", "name": "vite"})),
            "toolu_02",
            Some(spec.clone()),
        )
        .request
        .expect("the escalation carries its request");

        assert_eq!(raised.forward, Some(spec));
        assert_eq!(
            escalation().forward,
            None,
            "an ordinary proposal must not grow a forward"
        );
    }

    /// The escalation is attributed to the authenticated caller. An event
    /// without the agent id is one the approver reads no tree path for — and
    /// it is exactly what dropping the field produces.
    #[test]
    fn the_escalation_names_the_authenticated_agent() {
        assert_eq!(
            escalation().agent_id.as_ref().map(ToString::to_string),
            Some("agent-42".to_owned()),
            "the identity's agent id must ride the event"
        );
    }

    /// The escalation carries the id the answer must quote. The whole wait
    /// matches by identity, so a request journaled without its id is one no
    /// decision can ever answer — the agent blocks forever on a question
    /// nobody can address.
    #[test]
    fn the_escalation_carries_the_id_the_answer_must_quote() {
        assert_eq!(
            escalation().request_id.as_ref().map(ToString::to_string),
            Some("toolu_01".to_owned()),
            "the request id must ride the event, or no decision can match it"
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
