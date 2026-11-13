//! Claude's permission prompts, on both routes.
//!
//! Claude asks before running a gated tool, and Horde's chain answers. Two
//! routes carry the question, and this module owns both payloads:
//!
//! * **stdio** (W12c, the default): `--permission-prompt-tool stdio` makes the
//!   harness write a `control_request` with subtype `can_use_tool` to stdout
//!   and wait for a `control_response` on stdin — the same exchange the Agent
//!   SDK's `canUseTool` callback rides. One channel, no MCP round trip
//!   (transcripts `12a`–`13`, claude 2.1.323).
//! * **MCP tool** (spike W1c, the fallback): `--permission-prompt-tool
//!   mcp__horde__approve` makes the harness call a tool Horde serves and read
//!   the decision out of the tool result's text block (transcripts `04*`,
//!   `06`, `17`).
//!
//! The decision body is the same on both: `{"behavior":"allow",
//! "updatedInput":…}` or `{"behavior":"deny","message":…}`. What W1c
//! established still holds on both, re-measured at 2.1.323:
//!
//! * A decision may **rewrite** the tool's arguments — the harness executes
//!   `updatedInput` (`04c`, `12c`), so this is where the chain narrows a call.
//! * The harness will **wait** — 180 s on the MCP route (`04d`), 90 s on stdio
//!   (`12e`), no timeout either way.
//!
//! `AskUserQuestion` rides the same prompt: the "tool call" is the question,
//! and answering it is an allow whose `updatedInput` adds an `answers` map
//! keyed by question text (`13`). [`ask_user_questions`] lifts the questions
//! into the domain's [`Question`]s so a client can answer them.

use horde_adapter::{
    Answers, PermissionDecision, ProposedToolCall, Question, QuestionOption, ToolCallId,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

/// The tool whose "permission" is a question to the human.
pub const ASK_USER_QUESTION: &str = "AskUserQuestion";

/// The `--permission-prompt-tool` value that selects the stdio route.
pub const STDIO_PROMPT_TOOL: &str = "stdio";

/// The arguments the harness passes to the MCP permission tool.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PromptRequest {
    pub tool_name: String,
    /// The arguments the model proposed, structured.
    pub input: Value,
    #[serde(default)]
    pub tool_use_id: String,
}

impl PromptRequest {
    /// Lifts the harness's request into the provider-neutral domain type.
    #[must_use]
    pub fn to_proposed_call(&self) -> ProposedToolCall {
        proposed_call(&self.tool_use_id, &self.tool_name, &self.input)
    }
}

/// A `can_use_tool` control request, as claude 2.1.323 writes it to stdout
/// under `--permission-prompt-tool stdio` (transcripts `12a`–`13`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CanUseTool {
    pub tool_name: String,
    #[serde(default)]
    pub input: Value,
    #[serde(default)]
    pub tool_use_id: String,
    /// Claude's own suggestions for widening its permission state, e.g.
    /// `{"type":"setMode","mode":"acceptEdits","destination":"session"}`.
    /// Kept for display; never applied — see [`encode_decision`].
    #[serde(default)]
    pub permission_suggestions: Vec<Value>,
    /// Claude's one-line summary of the call, e.g. the file name.
    #[serde(default)]
    pub description: Option<String>,
}

impl CanUseTool {
    /// Lifts the request into the provider-neutral domain type.
    #[must_use]
    pub fn to_proposed_call(&self) -> ProposedToolCall {
        proposed_call(&self.tool_use_id, &self.tool_name, &self.input)
    }
}

/// The domain call for one prompt, with `AskUserQuestion`'s questions lifted.
fn proposed_call(tool_use_id: &str, tool_name: &str, input: &Value) -> ProposedToolCall {
    let call = ProposedToolCall::new(ToolCallId::new(tool_use_id), tool_name, input.clone());
    if tool_name == ASK_USER_QUESTION {
        call.asking(ask_user_questions(input))
    } else {
        call
    }
}

/// The questions an `AskUserQuestion` input asks, as domain [`Question`]s.
///
/// Claude keys answers by the question text, so the text is the id. Free
/// text is always acceptable: Claude's own UI offers "Other" on every
/// question, and the tool result reads the answer back verbatim (`13`).
#[must_use]
pub fn ask_user_questions(input: &Value) -> Vec<Question> {
    let Some(questions) = input.get("questions").and_then(Value::as_array) else {
        return Vec::new();
    };
    questions
        .iter()
        .filter_map(|question| {
            let text = question.get("question").and_then(Value::as_str)?;
            Some(Question {
                id: text.to_owned(),
                header: string_field(question, "header"),
                question: text.to_owned(),
                options: question
                    .get("options")
                    .and_then(Value::as_array)
                    .map(|options| {
                        options
                            .iter()
                            .map(|option| QuestionOption {
                                label: string_field(option, "label"),
                                description: string_field(option, "description"),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                multi_select: question
                    .get("multiSelect")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                allows_free_text: true,
                secret: false,
            })
        })
        .collect()
}

fn string_field(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The decision body both routes carry, or `None` when the decision cannot
/// be expressed for this call.
///
/// * Allow → `updatedInput` is the narrowing, or the proposal echoed back:
///   the harness runs whatever `updatedInput` says, so omitting it would run
///   nothing.
/// * Deny → the message is what the agent reads as the tool's error (`12b`).
/// * Answer → an allow whose `updatedInput` is the proposal plus an
///   `answers` map keyed by question text, several choices joined with
///   `", "` — the shape the harness reads back to the model (`13`). Only an
///   `AskUserQuestion` can be answered; anywhere else it is `None`.
///
/// `updatedPermissions` is deliberately never sent, though the harness honours
/// it (`12d`: a session `addRules` grant made the second write ask nothing).
/// Remember-for-session belongs to the chain, which remembers the *exact*
/// call; Claude's grants are tool- or mode-wide (its own suggestion for a
/// Write is `setMode acceptEdits`), so mirroring one would let later calls the
/// chain never saw run unasked.
#[must_use]
pub fn encode_decision(
    decision: &PermissionDecision,
    tool_name: &str,
    proposed_input: &Value,
) -> Option<Value> {
    match decision {
        PermissionDecision::Allow { updated_input, .. } => Some(json!({
            "behavior": "allow",
            "updatedInput": updated_input.as_ref().unwrap_or(proposed_input),
        })),
        PermissionDecision::Deny { message } => Some(json!({
            "behavior": "deny",
            "message": message,
        })),
        PermissionDecision::Answer { answers } if tool_name == ASK_USER_QUESTION => {
            let mut input = match proposed_input {
                Value::Object(map) => map.clone(),
                _ => Map::new(),
            };
            input.insert("answers".to_owned(), answer_map(answers));
            Some(json!({ "behavior": "allow", "updatedInput": Value::Object(input) }))
        }
        PermissionDecision::Answer { .. } => None,
    }
}

/// Claude's `answers` object: question text → the chosen labels as one
/// string. Claude reads a single string per question (`13`).
fn answer_map(answers: &Answers) -> Value {
    Value::Object(
        answers
            .iter()
            .map(|(question, chosen)| (question.clone(), Value::String(chosen.join(", "))))
            .collect(),
    )
}

/// Wraps a decision body as a complete MCP `tools/call` result — the
/// fallback route, which wants the body *stringified inside a text block*.
#[must_use]
pub fn encode_tool_response(body: &Value) -> Value {
    json!({
        "content": [{ "type": "text", "text": body.to_string() }],
        "isError": false,
    })
}

/// The stdio route's answer: a `control_response` correlated by the
/// harness's own `request_id`, as one line.
#[must_use]
pub fn encode_control_response(request_id: &str, body: &Value) -> String {
    json!({
        "type": "control_response",
        "response": { "subtype": "success", "request_id": request_id, "response": body },
    })
    .to_string()
}

/// Refuses a control request this adapter has no answer for, as one line.
///
/// Every control request must be answered — the harness waits on it — so a
/// subtype Horde does not handle gets an error rather than silence.
#[must_use]
pub fn encode_control_error(request_id: &str, message: &str) -> String {
    json!({
        "type": "control_response",
        "response": { "subtype": "error", "request_id": request_id, "error": message },
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use horde_adapter::{Answers, PermissionDecision};
    use serde_json::{Value, json};

    use super::{
        CanUseTool, PromptRequest, ask_user_questions, encode_control_error,
        encode_control_response, encode_decision, encode_tool_response,
    };

    /// Verbatim from `docs/spikes/transcripts/perm-deny.jsonl`.
    fn recorded_request() -> PromptRequest {
        serde_json::from_value(json!({
            "tool_name": "Write",
            "input": {
                "file_path": "/Users/myles/Code/horde/.claude/worktrees/spike-stream-json/scratch/work/denied.txt",
                "content": "BLOCKED"
            },
            "tool_use_id": "toolu_01DGwHJdHLbBM9tDFRAJA64L"
        }))
        .expect("the recorded request shape must parse")
    }

    /// Verbatim request body from transcript `13-ask-user-question.jsonl`.
    fn recorded_question() -> CanUseTool {
        serde_json::from_value(json!({
            "subtype": "can_use_tool",
            "tool_name": "AskUserQuestion",
            "display_name": "AskUserQuestion",
            "input": {"questions": [{
                "question": "Which colour do you prefer?",
                "header": "Colour",
                "options": [
                    {"label": "Red", "description": "Choose red."},
                    {"label": "Blue", "description": "Choose blue."}
                ],
                "multiSelect": false
            }]},
            "tool_use_id": "toolu_01GD28jHPCjs1GURDw4nvFRe",
            "requires_user_interaction": true
        }))
        .expect("the recorded shape must parse")
    }

    fn parsed(body: Option<Value>) -> Value {
        body.expect("the decision is expressible")
    }

    #[test]
    fn a_recorded_request_lifts_into_the_domain_type() {
        let call = recorded_request().to_proposed_call();
        assert_eq!(call.tool_name, "Write");
        assert_eq!(call.tool_call_id.as_str(), "toolu_01DGwHJdHLbBM9tDFRAJA64L");
        assert_eq!(call.input["content"], "BLOCKED");
        assert!(!call.is_question(), "a Write asks nothing");
    }

    #[test]
    fn a_recorded_can_use_tool_request_lifts_into_the_domain_type() {
        let request: CanUseTool = serde_json::from_value(json!({
            "subtype": "can_use_tool", "tool_name": "Write", "display_name": "Write",
            "input": {"file_path": "/w/probe.txt", "content": "hello"},
            "description": "probe.txt",
            "permission_suggestions": [{"type": "setMode", "mode": "acceptEdits", "destination": "session"}],
            "server_policy_ask_only": true,
            "tool_use_id": "toolu_01KPip6d4uHLM3smJUQAPZNz"
        }))
        .expect("the 12a shape must parse");
        assert_eq!(request.description.as_deref(), Some("probe.txt"));
        assert_eq!(request.permission_suggestions.len(), 1);
        let call = request.to_proposed_call();
        assert_eq!(call.tool_call_id.as_str(), "toolu_01KPip6d4uHLM3smJUQAPZNz");
        assert_eq!(call.input["file_path"], "/w/probe.txt");
        assert!(!call.is_question());
    }

    /// The question lifts field for field, keyed by its text, and the MCP
    /// route lifts it the same way the stdio route does.
    #[test]
    fn an_ask_user_question_request_lifts_its_questions() {
        let call = recorded_question().to_proposed_call();
        assert!(call.is_question());
        let [question] = call.questions.as_slice() else {
            panic!("one question, got {:?}", call.questions);
        };
        assert_eq!(question.id, "Which colour do you prefer?");
        assert_eq!(question.question, "Which colour do you prefer?");
        assert_eq!(question.header, "Colour");
        assert_eq!(question.options.len(), 2);
        assert_eq!(question.options[1].label, "Blue");
        assert_eq!(question.options[1].description, "Choose blue.");
        assert!(!question.multi_select);
        assert!(question.allows_free_text);
        assert!(!question.secret);

        let over_mcp = PromptRequest {
            tool_name: "AskUserQuestion".to_owned(),
            input: recorded_question().input,
            tool_use_id: "t".to_owned(),
        };
        assert_eq!(over_mcp.to_proposed_call().questions, call.questions);
    }

    #[test]
    fn malformed_questions_are_skipped_not_invented() {
        assert!(ask_user_questions(&json!({})).is_empty());
        assert!(ask_user_questions(&json!({"questions": "nope"})).is_empty());
        let lifted = ask_user_questions(&json!({"questions": [
            {"header": "no text"},
            {"question": "Bare?", "multiSelect": true}
        ]}));
        assert_eq!(lifted.len(), 1, "a question with no text cannot be keyed");
        assert_eq!(lifted[0].id, "Bare?");
        assert!(lifted[0].multi_select);
        assert!(lifted[0].options.is_empty());
        assert_eq!(lifted[0].header, "");
    }

    #[test]
    fn a_denial_encodes_the_behavior_and_message_the_harness_reads() {
        let body = parsed(encode_decision(
            &PermissionDecision::deny("no"),
            "Write",
            &json!({}),
        ));
        assert_eq!(body, json!({"behavior": "deny", "message": "no"}));
    }

    #[test]
    fn an_unmodified_allow_echoes_the_proposal_back() {
        let proposed = json!({"file_path": "/tmp/x", "content": "hi"});
        let body = parsed(encode_decision(
            &PermissionDecision::allow(),
            "Write",
            &proposed,
        ));
        assert_eq!(
            body,
            json!({"behavior": "allow", "updatedInput": proposed}),
            "the harness runs updatedInput; omitting it would run nothing"
        );
    }

    /// A remembered allow still sends no `updatedPermissions`: the chain's
    /// memory is exact-call, Claude's grants are wider.
    #[test]
    fn a_remembered_allow_mirrors_no_harness_grant() {
        let body = parsed(encode_decision(
            &PermissionDecision::allow().remembered(),
            "Write",
            &json!({"a": 1}),
        ));
        assert_eq!(body, json!({"behavior": "allow", "updatedInput": {"a": 1}}));
    }

    #[test]
    fn a_narrowing_allow_replaces_the_proposal() {
        let decision = PermissionDecision::allow_with(json!({"content": "REWRITTEN_BY_HORDE"}));
        let body = parsed(encode_decision(
            &decision,
            "Write",
            &json!({"content": "ORIGINAL"}),
        ));
        assert_eq!(
            body["updatedInput"],
            json!({"content": "REWRITTEN_BY_HORDE"})
        );
    }

    /// The `13` shape: the questions echoed back with an `answers` map keyed
    /// by question text; several choices are joined into one string.
    #[test]
    fn an_answer_encodes_the_questions_plus_answers_keyed_by_text() {
        let input = recorded_question().input;
        let answers = Answers::from([
            (
                "Which colour do you prefer?".to_owned(),
                vec!["Red".to_owned()],
            ),
            ("Sizes?".to_owned(), vec!["S".to_owned(), "M".to_owned()]),
        ]);
        let body = parsed(encode_decision(
            &PermissionDecision::answer(answers),
            "AskUserQuestion",
            &input,
        ));
        assert_eq!(body["behavior"], "allow");
        assert_eq!(body["updatedInput"]["questions"], input["questions"]);
        assert_eq!(
            body["updatedInput"]["answers"],
            json!({"Which colour do you prefer?": "Red", "Sizes?": "S, M"})
        );

        let from_nothing = parsed(encode_decision(
            &PermissionDecision::answer(Answers::new()),
            "AskUserQuestion",
            &json!(null),
        ));
        assert_eq!(from_nothing["updatedInput"], json!({"answers": {}}));
    }

    #[test]
    fn an_answer_to_a_tool_that_asked_nothing_is_inexpressible() {
        assert_eq!(
            encode_decision(
                &PermissionDecision::answer(Answers::new()),
                "Write",
                &json!({}),
            ),
            None
        );
    }

    #[test]
    fn the_tool_response_stringifies_the_decision_inside_a_text_block() {
        let response = encode_tool_response(&json!({"behavior": "deny", "message": "nope"}));
        let text = response["content"][0]["text"]
            .as_str()
            .expect("the decision must be a string, not a nested object");
        let decoded: Value = serde_json::from_str(text).expect("valid json");
        assert_eq!(decoded["behavior"], "deny");
        assert_eq!(response["isError"], false);
    }

    /// The `12a` reply shape, byte for byte in structure.
    #[test]
    fn a_control_response_correlates_by_the_harness_request_id() {
        let line = encode_control_response("7a32", &json!({"behavior": "deny", "message": "m"}));
        assert!(!line.contains('\n'), "one line per message");
        let value: Value = serde_json::from_str(&line).expect("valid json");
        assert_eq!(
            value,
            json!({"type": "control_response", "response": {
                "subtype": "success", "request_id": "7a32",
                "response": {"behavior": "deny", "message": "m"}}})
        );
    }

    #[test]
    fn a_control_error_answers_without_a_decision() {
        let value: Value =
            serde_json::from_str(&encode_control_error("r1", "unsupported")).expect("valid json");
        assert_eq!(
            value,
            json!({"type": "control_response", "response": {
                "subtype": "error", "request_id": "r1", "error": "unsupported"}})
        );
    }
}
