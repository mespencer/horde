//! The `--permission-prompt-tool` contract.
//!
//! Permission prompts do **not** ride the stdout event stream. The harness
//! calls a named MCP tool that Horde serves, waits for the answer, and only
//! then runs (or refuses) the tool. That means a Claude session has two inbound
//! channels — stdout for output, the MCP endpoint for permissions — and this
//! module owns the second one's payloads.
//!
//! Spike W1c established three things worth restating here, because they are
//! what the permission chain (DESIGN.md §10) is built on:
//!
//! * A decision may **rewrite** the tool's arguments, not merely allow or deny.
//!   The harness executes `updatedInput`, so this is the seam where a policy
//!   narrows a call.
//! * The harness will **wait**. A 180-second stall was answered without any
//!   timeout firing, which is what makes human approval viable.
//! * Both stdio and HTTP MCP servers work, so the Worker may host the endpoint
//!   however it likes.
//!
//! The flag itself is absent from `claude --help`. It is real — passing it
//! without an argument reports `option '--permission-prompt-tool <tool>'
//! argument missing`, while an invented flag reports `unknown option` — but
//! being undocumented, it is exactly the kind of thing a CLI upgrade breaks
//! quietly, which is why the conformance suite pins its payloads.

use horde_adapter::{PermissionDecision, ProposedToolCall, ToolCallId};
use serde::Deserialize;
use serde_json::{Value, json};

/// The arguments the harness passes to the permission tool.
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
        ProposedToolCall {
            tool_call_id: ToolCallId::new(self.tool_use_id.clone()),
            tool_name: self.tool_name.clone(),
            input: self.input.clone(),
        }
    }
}

/// Renders a decision as the JSON string the harness expects.
///
/// The harness wants this *stringified inside a text content block*, not as a
/// nested object; [`encode_tool_response`] wraps it accordingly.
#[must_use]
pub fn encode_decision(decision: &PermissionDecision, proposed_input: &Value) -> String {
    let body = match decision {
        PermissionDecision::Allow { updated_input, .. } => json!({
            "behavior": "allow",
            // The harness runs whatever is in `updatedInput`, so an unmodified
            // allow must echo the proposal back rather than omit the field.
            "updatedInput": updated_input.as_ref().unwrap_or(proposed_input),
        }),
        PermissionDecision::Deny { message } => json!({
            "behavior": "deny",
            "message": message,
        }),
    };
    body.to_string()
}

/// Wraps a decision as a complete MCP `tools/call` result.
#[must_use]
pub fn encode_tool_response(decision: &PermissionDecision, proposed_input: &Value) -> Value {
    json!({
        "content": [{ "type": "text", "text": encode_decision(decision, proposed_input) }],
        "isError": false,
    })
}

#[cfg(test)]
mod tests {
    use horde_adapter::PermissionDecision;
    use serde_json::json;

    use super::{PromptRequest, encode_decision, encode_tool_response};

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

    #[test]
    fn a_recorded_request_lifts_into_the_domain_type() {
        let call = recorded_request().to_proposed_call();
        assert_eq!(call.tool_name, "Write");
        assert_eq!(call.tool_call_id.as_str(), "toolu_01DGwHJdHLbBM9tDFRAJA64L");
        assert_eq!(call.input["content"], "BLOCKED");
    }

    #[test]
    fn a_denial_encodes_the_behavior_and_message_the_harness_reads() {
        let encoded = encode_decision(&PermissionDecision::deny("no"), &json!({}));
        let value: serde_json::Value = serde_json::from_str(&encoded).expect("valid json");
        assert_eq!(value["behavior"], "deny");
        assert_eq!(value["message"], "no");
    }

    #[test]
    fn an_unmodified_allow_echoes_the_proposal_back() {
        let proposed = json!({"file_path": "/tmp/x", "content": "hi"});
        let encoded = encode_decision(&PermissionDecision::allow(), &proposed);
        let value: serde_json::Value = serde_json::from_str(&encoded).expect("valid json");
        assert_eq!(
            value["updatedInput"], proposed,
            "the harness runs updatedInput; omitting it would run nothing"
        );
    }

    #[test]
    fn a_narrowing_allow_replaces_the_proposal() {
        let proposed = json!({"content": "ORIGINAL"});
        let decision = PermissionDecision::allow_with(json!({"content": "REWRITTEN_BY_HORDE"}));
        let encoded = encode_decision(&decision, &proposed);
        let value: serde_json::Value = serde_json::from_str(&encoded).expect("valid json");
        assert_eq!(value["updatedInput"]["content"], "REWRITTEN_BY_HORDE");
    }

    #[test]
    fn the_tool_response_stringifies_the_decision_inside_a_text_block() {
        let response = encode_tool_response(&PermissionDecision::deny("nope"), &json!({}));
        let text = response["content"][0]["text"]
            .as_str()
            .expect("the decision must be a string, not a nested object");
        let decoded: serde_json::Value = serde_json::from_str(text).expect("valid json");
        assert_eq!(decoded["behavior"], "deny");
    }
}
