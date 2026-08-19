//! Newline-delimited JSON-RPC 2.0 in and out.
//!
//! Decoding classifies a line into a [`Frame`]; encoding builds the three
//! requests a Worker ever sends. Request ids are the caller's to allocate,
//! because correlation is the Worker's job — a codec that owned a counter would
//! fight the supervisor that actually multiplexes the stream.

use horde_adapter::{AdapterError, Result};
use serde_json::{Value, json};

use crate::wire::{Frame, Notification, RpcError};

/// Decodes one line of app-server output.
///
/// # Errors
///
/// [`AdapterError::Protocol`] when the line is not JSON, or is JSON that is
/// neither a response, a server request, nor a notification. Unknown *methods*
/// are not an error — see [`Notification::classify`].
pub fn decode_line(line: &str) -> Result<Frame> {
    let value: Value = serde_json::from_str(line).map_err(|source| AdapterError::Protocol {
        message: format!("app-server line is not JSON: {source}"),
        raw: line.to_owned(),
    })?;

    let id = value.get("id").and_then(Value::as_u64);
    let method = value.get("method").and_then(Value::as_str);

    match (id, method) {
        // Both id and method: the server is asking us something.
        (Some(id), Some(method)) => Ok(Frame::ServerRequest {
            id,
            method: method.to_owned(),
            params: value.get("params").cloned().unwrap_or(Value::Null),
        }),
        // id without method: a reply to us.
        (Some(id), None) => Ok(Frame::Response {
            id,
            result: value.get("result").cloned(),
            error: value
                .get("error")
                .and_then(|e| serde_json::from_value::<RpcError>(e.clone()).ok()),
        }),
        // method without id: a notification.
        (None, Some(method)) => Ok(Frame::Notification(Notification::classify(
            method,
            value.get("params").cloned().unwrap_or(Value::Null),
        ))),
        (None, None) => Err(AdapterError::Protocol {
            message: "app-server frame has neither id nor method".to_owned(),
            raw: line.to_owned(),
        }),
    }
}

/// The one `approvalsReviewer` this codec will ever send.
///
/// `auto_review` is the transcript-23 hazard: under it, codex's own review
/// subagent approved a sandbox escalation and the write RAN under a read-only
/// sandbox, with no client-facing request at all — the entire permission chain
/// silently bypassed by codex reviewing itself. `user` (also the measured
/// default when unset, transcript 22) routes every approval to this client.
/// The value is a constant rather than a field so no configuration path can
/// ever reach the wire with anything else; [`approvals_reviewer_from_config`]
/// is the parse-time gate for anyone adding such a path.
pub const APPROVALS_REVIEWER: &str = "user";

/// Validates an operator-supplied `approvalsReviewer`, admitting only the
/// value the codec sends anyway.
///
/// # Errors
///
/// Anything but `None` or `"user"` — `auto_review` and `guardian` by name,
/// and unknown values by refusal to guess — with an error naming the
/// measurement, so the config that tried it learns *why* the door is shut.
pub fn approvals_reviewer_from_config(
    requested: Option<&str>,
) -> core::result::Result<&'static str, String> {
    match requested {
        None | Some("user") => Ok(APPROVALS_REVIEWER),
        Some(hazard @ ("auto_review" | "guardian")) => Err(format!(
            "approvalsReviewer {hazard:?} is refused: under auto_review codex's own subagent \
             approved a sandbox escalation and the write ran under a read-only sandbox with no \
             client-facing request (docs/spikes/codex-app-server/transcripts/\
             23-exec-autoreview-onrequest-ro-horde-write.jsonl). Only \"user\" routes approvals \
             to Horde's permission chain."
        )),
        Some(other) => Err(format!(
            "approvalsReviewer {other:?} is not a value any spike measured; only \"user\" is \
             accepted (see transcript 23 for why this knob fails closed)"
        )),
    }
}

/// What one thread is started (or resumed) with.
///
/// The approval policy is deliberately not a field. The only policy measured
/// to route MCP elicitations to the client is the granular one with
/// `mcp_elicitations: true` — under plain `"on-request"` the thread parks at
/// `waitingOnApproval` forever and the turn never completes (transcript 19).
/// A configurable policy would be a knob whose other settings are all broken,
/// so the encoder always sends the one that works. The reviewer is likewise
/// pinned to [`APPROVALS_REVIEWER`] — see transcript 23 for the hazard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ThreadConfig {
    /// The agent's working directory — half of the real enforcement, with the
    /// sandbox tier (ADR-0005).
    pub cwd: Option<String>,
    pub sandbox: crate::sandbox::SandboxTier,
    /// Concrete model name; `None` leaves the harness default.
    pub model: Option<String>,
    /// Horde's prompt overlay, sent as `developerInstructions` — an *added*
    /// developer message. Never `baseInstructions`, which would replace the
    /// harness's own prompt and break its tools.
    pub developer_instructions: Option<String>,
}

impl ThreadConfig {
    fn params(&self) -> Value {
        let mut params = json!({
            "approvalPolicy": {
                "granular": {
                    // The interception point (transcripts 17-18). Off, no MCP
                    // call is ever offered to Horde.
                    "mcp_elicitations": true,
                    // The measured-working combination from the captures. The
                    // shell/exec half still never reaches the client
                    // (guardian_approval adjudicates internally, ADR-0005);
                    // anything that does arrive is declined by the session.
                    "rules": true,
                    "sandbox_approval": true,
                }
            },
            "approvalsReviewer": APPROVALS_REVIEWER,
            "sandbox": self.sandbox.wire_name(),
        });
        if let Some(cwd) = &self.cwd {
            params["cwd"] = json!(cwd);
        }
        if let Some(model) = &self.model {
            params["model"] = json!(model);
        }
        if let Some(instructions) = &self.developer_instructions {
            params["developerInstructions"] = json!(instructions);
        }
        params
    }
}

/// `initialize` — the app-server handshake request.
#[must_use]
pub fn encode_initialize(id: u64) -> String {
    request(
        id,
        "initialize",
        json!({
            "clientInfo": {
                "name": "horde-worker",
                "title": "Horde",
                "version": env!("CARGO_PKG_VERSION"),
            },
            // Declares the ability to answer server->client requests, which is
            // what separates app-server from `codex exec`: exec never declares
            // it and its MCP calls are auto-cancelled (ADR-0005 amendment).
            "capabilities": { "experimentalApi": true },
        }),
    )
}

/// `initialized` — the notification that completes the handshake.
#[must_use]
pub fn encode_initialized() -> String {
    json!({ "jsonrpc": "2.0", "method": "initialized" }).to_string()
}

/// `thread/start` — begin a new conversation under an explicit sandbox tier.
#[must_use]
pub fn encode_thread_start(id: u64, config: &ThreadConfig) -> String {
    request(id, "thread/start", config.params())
}

/// `thread/resume` — reattach to a thread recorded by a previous process.
///
/// Takes the same config as a start: the sandbox tier, cwd, and overlay are
/// per-process facts the resumed thread must be re-pinned to, not properties
/// the rollout file remembers on Horde's behalf.
#[must_use]
pub fn encode_thread_resume(id: u64, thread_id: &str, config: &ThreadConfig) -> String {
    let mut params = config.params();
    params["threadId"] = json!(thread_id);
    request(id, "thread/resume", params)
}

/// The reply that lets an MCP tool call proceed, or refuses it.
///
/// The result field is `action` — an MCP elicitation result — and **not** the
/// `decision` field codex's sandbox/exec approvals use; the wrong shape fails
/// to deserialize server-side (measured, rmcp spike §4). There is nowhere to
/// carry a deny message: a declined call surfaces to the agent as the fixed
/// string "user rejected MCP tool call" (transcript 18).
#[must_use]
pub fn encode_elicitation_response(id: u64, allow: bool) -> String {
    let result = if allow {
        json!({ "action": "accept", "content": {} })
    } else {
        json!({ "action": "decline" })
    };
    json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string()
}

/// One of codex's `decision`-vocabulary answers to an exec approval request.
///
/// The full set the re-probe measured or the schema advertises
/// (`availableDecisions`, transcripts 20–33): `accept` ran the command
/// (transcript 29), `acceptWithExecpolicyAmendment` ran it and persisted the
/// echoed argv as a rule a second turn no longer asked about (transcript 30),
/// `decline` blocked it with `status: "declined"` and no exit code
/// (transcripts 20–22, 25–27), and `cancel` appears in every recorded
/// `availableDecisions` list. `acceptForSession` is the session-scoped grant
/// in the same vocabulary — the encoding for a chain allow that carried
/// `remember_for_session`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecDecision {
    /// Run the command, this once.
    Accept,
    /// Run it, and let codex cache the grant for the rest of the session.
    AcceptForSession,
    /// Run it, and persist this argv as an execpolicy rule. Broader than a
    /// session grant — the rule outlives the session — so it is only encoded
    /// when the chain's narrowing named exactly this argv.
    AcceptWithExecpolicyAmendment {
        /// The rule to persist, echoed from `proposedExecpolicyAmendment`.
        execpolicy_amendment: Vec<String>,
    },
    /// Refuse the command. The exec item completes as `declined`.
    Decline,
    /// Withdraw the question — the encoding for an interrupt that found an
    /// approval still parked.
    Cancel,
}

impl ExecDecision {
    /// The wire name (or shape) this decision serializes as, for journals.
    #[must_use]
    pub fn wire_name(&self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::AcceptForSession => "acceptForSession",
            Self::AcceptWithExecpolicyAmendment { .. } => "acceptWithExecpolicyAmendment",
            Self::Decline => "decline",
            Self::Cancel => "cancel",
        }
    }

    fn wire_value(&self) -> Value {
        match self {
            Self::AcceptWithExecpolicyAmendment {
                execpolicy_amendment,
            } => json!({
                "acceptWithExecpolicyAmendment": {
                    "execpolicy_amendment": execpolicy_amendment,
                }
            }),
            unit => json!(unit.wire_name()),
        }
    }
}

/// The reply to an exec approval request, in the `decision` vocabulary.
///
/// Never the elicitation `action` — the wrong shape fails to deserialize
/// server-side and then nothing was answered at all, which parks the harness
/// at `waitingOnApproval` forever (the transcript-19 hang, same mechanism).
#[must_use]
pub fn encode_exec_approval_response(id: u64, decision: &ExecDecision) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "result": { "decision": decision.wire_value() } })
        .to_string()
}

/// The deny-by-default reply to a sandbox/exec approval request.
///
/// These use the `decision` shape, not the elicitation `action`. Kept for the
/// approval kinds Horde still cannot adjudicate (file changes, and anything a
/// future codex invents): whatever reaches the client without a chain seam is
/// refused, so the sandbox tier stays the only granted authority.
#[must_use]
pub fn encode_decision_decline(id: u64) -> String {
    encode_exec_approval_response(id, &ExecDecision::Decline)
}

/// A JSON-RPC error reply for a server request this codec cannot answer.
///
/// Sent rather than left silent, because an unanswered request can park the
/// harness forever — the `waitingOnApproval` state transcript 19 recorded.
#[must_use]
pub fn encode_method_not_found(id: u64, method: &str) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": -32601, "message": format!("horde does not answer {method}") },
    })
    .to_string()
}

/// `turn/start` — send a message and begin a turn.
#[must_use]
pub fn encode_turn_start(id: u64, thread_id: &str, text: &str) -> String {
    request(
        id,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": text }],
        }),
    )
}

/// `turn/steer` — deliver a message into a turn already running.
///
/// `expected_turn_id` is a precondition the server enforces: the request fails
/// if that turn is no longer the active one, which is what stops a steer racing
/// a turn that ended while it was in flight.
#[must_use]
pub fn encode_steer(id: u64, thread_id: &str, expected_turn_id: &str, text: &str) -> String {
    request(
        id,
        "turn/steer",
        json!({
            "threadId": thread_id,
            "expectedTurnId": expected_turn_id,
            "input": [{ "type": "text", "text": text }],
        }),
    )
}

/// `turn/interrupt` — cancel the turn in flight.
///
/// The caller must not then wait for `turn/completed`; see [`crate::turn`].
#[must_use]
pub fn encode_interrupt(id: u64, thread_id: &str) -> String {
    request(id, "turn/interrupt", json!({ "threadId": thread_id }))
}

fn request(id: u64, method: &str, params: Value) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{decode_line, encode_interrupt, encode_steer, encode_turn_start};
    use crate::wire::{Frame, Notification};

    #[test]
    fn a_notification_is_a_method_without_an_id() {
        let frame = decode_line(
            &json!({"method": "turn/started", "params": {"turn": {"id": "t1"}}}).to_string(),
        )
        .expect("recorded notifications must decode");

        match frame {
            Frame::Notification(Notification::TurnStarted(started)) => {
                assert_eq!(started.turn.id, "t1");
            }
            other => panic!("expected a turn/started notification, got {other:?}"),
        }
    }

    #[test]
    fn a_server_request_carries_both_an_id_and_a_method() {
        // Codex 0.145.0 never sent one of these (ADR-0005), but the frame kind
        // exists and must not be mistaken for a response to something we asked.
        let frame = decode_line(
            &json!({"id": 7, "method": "item/commandExecution/requestApproval",
                    "params": {"command": "rm -rf /"}})
            .to_string(),
        )
        .expect("decodes");

        assert!(
            matches!(frame, Frame::ServerRequest { id: 7, .. }),
            "an id plus a method is a request to us, not a reply to us"
        );
    }

    #[test]
    fn a_response_is_an_id_without_a_method() {
        let frame = decode_line(&json!({"id": 3, "result": {"turnId": "t1"}}).to_string())
            .expect("decodes");
        assert!(matches!(frame, Frame::Response { id: 3, .. }));
    }

    #[test]
    fn a_frame_with_neither_id_nor_method_is_a_protocol_error() {
        let error = decode_line(&json!({"jsonrpc": "2.0"}).to_string())
            .expect_err("an unaddressable frame cannot be acted on");
        assert!(error.to_string().contains("neither id nor method"));
    }

    #[test]
    fn a_line_that_is_not_json_is_a_protocol_error() {
        let error = decode_line("not json at all").expect_err("garbage must not parse");
        assert!(error.to_string().contains("not JSON"));
    }

    #[test]
    fn steer_carries_the_precondition_that_stops_it_racing_a_finished_turn() {
        let encoded = encode_steer(4, "thread-1", "turn-9", "stop counting");
        let value: Value = serde_json::from_str(&encoded).expect("we produced this");

        assert_eq!(value["method"], "turn/steer");
        assert_eq!(value["params"]["expectedTurnId"], "turn-9");
        assert_eq!(value["params"]["input"][0]["text"], "stop counting");
    }

    #[test]
    fn the_encoders_produce_addressable_jsonrpc() {
        let config = super::ThreadConfig::default();
        for encoded in [
            encode_turn_start(1, "thread-1", "hello"),
            encode_interrupt(2, "thread-1"),
            super::encode_initialize(3),
            super::encode_thread_start(4, &config),
            super::encode_thread_resume(5, "thread-1", &config),
        ] {
            let value: Value = serde_json::from_str(&encoded).expect("we produced this");
            assert_eq!(value["jsonrpc"], "2.0");
            assert!(
                value["id"].is_u64(),
                "a request without an id cannot be correlated"
            );
            assert!(value["method"].is_string());
        }
    }

    /// `initialized` is the notification that completes the handshake; a
    /// wrong or empty line here leaves the server considering the client
    /// uninitialized, which fails silently much later.
    #[test]
    fn the_initialized_notification_is_the_protocols_own() {
        let value: Value =
            serde_json::from_str(&super::encode_initialized()).expect("we produced this");
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["method"], "initialized");
        assert!(
            value.get("id").is_none(),
            "a notification carries no id — an id would make it a request"
        );
    }

    /// The approval policy is the one measured to route elicitations, always.
    /// Plain "on-request" parks the thread at waitingOnApproval forever
    /// (transcript 19), so sending anything else here is a hang in waiting.
    /// And the reviewer is pinned to `user` on the same requests — the
    /// structural half of the transcript-23 pin: the codec can never emit
    /// `auto_review`, because the value is a constant, not a field.
    #[test]
    fn every_thread_is_started_with_the_routing_policy_and_the_pinned_reviewer() {
        let config = super::ThreadConfig {
            cwd: Some("/work".to_owned()),
            sandbox: crate::sandbox::SandboxTier::WorkspaceWrite,
            model: Some("gpt-5.6-sol".to_owned()),
            developer_instructions: Some("overlay".to_owned()),
        };
        for encoded in [
            super::encode_thread_start(1, &config),
            super::encode_thread_resume(2, "t1", &config),
        ] {
            let value: Value = serde_json::from_str(&encoded).expect("we produced this");
            let params = &value["params"];
            assert_eq!(
                params["approvalPolicy"]["granular"]["mcp_elicitations"],
                json!(true),
                "without this, no MCP call is ever offered to Horde"
            );
            assert_eq!(params["sandbox"], "workspace-write");
            assert_eq!(params["cwd"], "/work");
            assert_eq!(params["model"], "gpt-5.6-sol");
            assert_eq!(params["developerInstructions"], "overlay");
            assert!(
                params.get("baseInstructions").is_none(),
                "replacing the harness prompt would break its tools"
            );
            assert_eq!(params["approvalsReviewer"], "user");
            assert!(
                !encoded.contains("auto_review"),
                "the self-approving reviewer must be unrepresentable: {encoded}"
            );
        }
    }

    /// A default config omits what it does not know rather than sending empty
    /// strings — an empty model name is not "the harness default", it is a
    /// model named "".
    #[test]
    fn an_unconfigured_thread_omits_optional_fields_and_fails_closed() {
        let value: Value = serde_json::from_str(&super::encode_thread_start(
            1,
            &super::ThreadConfig::default(),
        ))
        .expect("we produced this");
        let params = &value["params"];
        assert_eq!(params["sandbox"], "read-only", "the default fails closed");
        for absent in ["cwd", "model", "developerInstructions"] {
            assert!(params.get(absent).is_none(), "{absent} must be omitted");
        }
    }

    /// A resume re-pins the thread to this process's sandbox and cwd; the
    /// thread id must ride the same params, or the harness starts a fresh
    /// conversation that has silently lost its context.
    #[test]
    fn a_resume_names_the_thread_it_is_reviving() {
        let value: Value = serde_json::from_str(&super::encode_thread_resume(
            7,
            "thread-abc",
            &super::ThreadConfig::default(),
        ))
        .expect("we produced this");
        assert_eq!(value["method"], "thread/resume");
        assert_eq!(value["params"]["threadId"], "thread-abc");
    }

    /// The elicitation reply's field is `action`, not `decision` — the wrong
    /// shape fails to deserialize server-side (rmcp spike §4, measured).
    #[test]
    fn elicitation_replies_use_action_and_approvals_use_decision() {
        let accept: Value = serde_json::from_str(&super::encode_elicitation_response(9, true))
            .expect("we produced this");
        assert_eq!(accept["result"]["action"], "accept");
        assert_eq!(accept["id"], 9, "the reply must correlate to the request");
        assert!(accept["result"].get("decision").is_none());

        let decline: Value = serde_json::from_str(&super::encode_elicitation_response(9, false))
            .expect("we produced this");
        assert_eq!(decline["result"]["action"], "decline");

        let refused: Value =
            serde_json::from_str(&super::encode_decision_decline(4)).expect("we produced this");
        assert_eq!(refused["result"]["decision"], "decline");
        assert!(refused["result"].get("action").is_none());
    }

    /// All five exec decisions encode in the `decision` vocabulary, unit
    /// variants as the bare strings the schema's `availableDecisions` lists
    /// and the amendment as the echo-back object transcript 30 proved is
    /// honored. None of them may leak the elicitation `action` shape.
    #[test]
    fn every_exec_decision_encodes_in_the_decision_vocabulary() {
        use super::ExecDecision;

        let argv = vec![
            "/bin/zsh".to_owned(),
            "-lc".to_owned(),
            "printf ESCALATED > probe.txt".to_owned(),
        ];
        let cases: [(ExecDecision, Value); 5] = [
            (ExecDecision::Accept, json!("accept")),
            (ExecDecision::AcceptForSession, json!("acceptForSession")),
            (
                ExecDecision::AcceptWithExecpolicyAmendment {
                    execpolicy_amendment: argv.clone(),
                },
                json!({"acceptWithExecpolicyAmendment": {"execpolicy_amendment": argv}}),
            ),
            (ExecDecision::Decline, json!("decline")),
            (ExecDecision::Cancel, json!("cancel")),
        ];

        for (decision, expected) in cases {
            let encoded: Value =
                serde_json::from_str(&super::encode_exec_approval_response(7, &decision))
                    .expect("we produced this");
            assert_eq!(encoded["id"], 7, "{decision:?} must correlate");
            assert_eq!(encoded["result"]["decision"], expected, "{decision:?}");
            assert!(
                encoded["result"].get("action").is_none(),
                "{decision:?}: the elicitation vocabulary deserializes to \
                 nothing on this method"
            );
        }
    }

    /// The wire names the journal quotes must match what actually went out.
    #[test]
    fn exec_decision_wire_names_match_their_encodings() {
        use super::ExecDecision;
        for decision in [
            ExecDecision::Accept,
            ExecDecision::AcceptForSession,
            ExecDecision::Decline,
            ExecDecision::Cancel,
            ExecDecision::AcceptWithExecpolicyAmendment {
                execpolicy_amendment: vec!["true".to_owned()],
            },
        ] {
            let encoded = super::encode_exec_approval_response(1, &decision);
            assert!(
                encoded.contains(decision.wire_name()),
                "the journaled name {} must appear in {encoded}",
                decision.wire_name()
            );
        }
    }

    /// The transcript-23 hazard stays parse-refused: only `user` (or nothing)
    /// passes, `auto_review` and `guardian` are named refusals that cite the
    /// measurement, and unknown values fail closed rather than reach codex.
    #[test]
    fn the_reviewer_gate_admits_user_and_refuses_the_self_approval_hazard() {
        use super::approvals_reviewer_from_config;

        assert_eq!(approvals_reviewer_from_config(None), Ok("user"));
        assert_eq!(approvals_reviewer_from_config(Some("user")), Ok("user"));

        for hazard in ["auto_review", "guardian"] {
            let refusal = approvals_reviewer_from_config(Some(hazard))
                .expect_err("the self-approval reviewer must be refused");
            assert!(
                refusal.contains("23-exec-autoreview-onrequest-ro-horde-write"),
                "the refusal names the transcript that measured the hazard: {refusal}"
            );
        }

        let unknown = approvals_reviewer_from_config(Some("committee"))
            .expect_err("unmeasured values fail closed");
        assert!(unknown.contains("transcript 23"), "{unknown}");
    }

    /// An unanswerable server request gets an error reply, never silence —
    /// silence is the waitingOnApproval hang transcript 19 recorded.
    #[test]
    fn an_unknown_server_request_is_answered_with_an_error() {
        let value: Value =
            serde_json::from_str(&super::encode_method_not_found(11, "attestation/generate"))
                .expect("we produced this");
        assert_eq!(value["id"], 11);
        assert_eq!(value["error"]["code"], -32601);
        assert!(
            value["error"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("attestation/generate")),
            "the refusal names the method so the log is actionable"
        );
    }
}
