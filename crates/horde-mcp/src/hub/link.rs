//! The seam between an Agent's endpoint and the hub.
//!
//! ADR-0004's 2026-08-19 amendment fixes how hub traffic travels: the Worker
//! forwards each call over its own authenticated mTLS channel, with the
//! Agent's identity riding the request. This trait is where that ruling meets
//! the endpoint — [`crate::hub::ToolProxy`] aggregates *some* hub link, and
//! which wire it rides is the host's business:
//!
//! * production: the Worker's forwarded link (`horde-worker`), one per Agent,
//!   carrying the Agent's identity on every RPC;
//! * tests: a directly dialed [`Upstream`], which is also what keeps the
//!   protocol-level e2e (`tests/hub_e2e.rs`) runnable without a Worker.
//!
//! # The tool list is a snapshot
//!
//! `tools()` is whatever the link learned when it was built — the same
//! connect-time-snapshot contract [`Upstream`] has, and for the same honest
//! reason: `tools/list_changed` is deliberately not built yet.

use rmcp::model::{CallToolResult, JsonObject, Tool};

use super::upstream::{HubError, Upstream};

/// One Agent's route to the hub's tools.
#[async_trait::async_trait]
pub trait HubLink: Send + Sync + std::fmt::Debug {
    /// The hub tools this link serves, already namespaced `{server}__{tool}`
    /// — the snapshot taken when the link was built.
    fn tools(&self) -> &[Tool];

    /// Calls one hub tool by its namespaced name.
    ///
    /// # Errors
    ///
    /// [`HubError::UnknownTool`] when the hub routes nothing by this name;
    /// [`HubError::Upstream`] when the transport or the serving side failed
    /// the call outright. A failure the upstream *reported* (`is_error`) is a
    /// result, not an error — the model must read it.
    async fn call(
        &self,
        name: &str,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, HubError>;
}

/// A directly connected upstream is itself a hub link: its advertised names
/// pass through whole, which is exactly right when the peer's tools are
/// already namespaced (a hub serving surface, or a stub standing in for one).
#[async_trait::async_trait]
impl HubLink for Upstream {
    fn tools(&self) -> &[Tool] {
        Upstream::tools(self)
    }

    async fn call(
        &self,
        name: &str,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, HubError> {
        Upstream::call(self, name, arguments).await
    }
}

// ---------------------------------------------------------------------------
// The forwarded wire's marshalling
// ---------------------------------------------------------------------------
//
// The forwarded RPCs (`ListHubTools`/`CallHubTool`, worker_service.proto)
// carry MCP payloads as JSON text. Both ends — the Worker's forwarded link
// and the Server's routing — encode and decode through these functions, so
// the two cannot drift: one owner, per the wire's own comment on why the
// payloads are JSON rather than `google.protobuf.Struct` (nothing Server-side
// reads the values, and Struct's doubles-only numbers would corrupt integer
// arguments in transit).

/// One MCP tool as the forwarded wire carries it.
///
/// # Errors
///
/// `serde_json` refusing the tool, which for these serde-native types means
/// a non-string map key somewhere in a schema — malformed upstream output.
pub fn tool_to_json(tool: &Tool) -> Result<String, serde_json::Error> {
    serde_json::to_string(tool)
}

/// The inverse of [`tool_to_json`].
///
/// # Errors
///
/// `serde_json` refusing the text — a peer that is not speaking this wire.
pub fn tool_from_json(json: &str) -> Result<Tool, serde_json::Error> {
    serde_json::from_str(json)
}

/// A call's arguments as the forwarded wire carries them: a JSON object, or
/// the empty string for "called with no arguments" — the two states MCP
/// distinguishes, kept distinct on the wire.
#[must_use]
pub fn arguments_to_json(arguments: Option<&JsonObject>) -> String {
    arguments
        .map(|fields| serde_json::Value::Object(fields.clone()).to_string())
        .unwrap_or_default()
}

/// The inverse of [`arguments_to_json`].
///
/// # Errors
///
/// The refusal for text that is neither empty nor a JSON object — anything
/// else cannot be tool arguments, and guessing would run a call the chain
/// never judged in this shape.
pub fn arguments_from_json(json: &str) -> Result<Option<JsonObject>, String> {
    if json.is_empty() {
        return Ok(None);
    }
    match serde_json::from_str(json) {
        Ok(serde_json::Value::Object(fields)) => Ok(Some(fields)),
        Ok(other) => Err(format!("tool arguments must be a JSON object, got {other}")),
        Err(error) => Err(format!("tool arguments are not JSON: {error}")),
    }
}

/// One call's result as the forwarded wire carries it: the MCP
/// `CallToolResult`, verbatim — upstream-reported failures ride inside it as
/// `isError`, because the model is the one who must read them.
///
/// # Errors
///
/// `serde_json` refusing the result — malformed upstream output.
pub fn result_to_json(result: &CallToolResult) -> Result<String, serde_json::Error> {
    serde_json::to_string(result)
}

/// The inverse of [`result_to_json`].
///
/// # Errors
///
/// `serde_json` refusing the text — a peer that is not speaking this wire.
pub fn result_from_json(json: &str) -> Result<CallToolResult, serde_json::Error> {
    serde_json::from_str(json)
}

/// The text of a result's first content block — the line a model (and every
/// assertion about one) reads first. `None` when the result leads with
/// something that is not text, which callers must not conflate with empty
/// text.
#[must_use]
pub fn first_text(result: &CallToolResult) -> Option<&str> {
    result
        .content
        .first()
        .and_then(|block| block.as_text())
        .map(|text| text.text.as_str())
}

/// A hub failure the model should read, as a tool result.
///
/// The in-band half of `hub::endpoint::render_failure`'s split: everything
/// except an unroutable name is a *result* carrying `isError`, because a
/// protocol error reaches the model as an opaque "internal error" and
/// teaches it nothing. Unroutable names stay protocol errors and never come
/// here.
#[must_use]
pub fn readable_failure(error: &HubError) -> CallToolResult {
    CallToolResult::error(vec![rmcp::model::ContentBlock::text(error.to_string())])
}

#[cfg(test)]
mod tests {
    // `HubLink` itself is a vocabulary with one production implementor per
    // host crate. The `Upstream` impl is two delegations, proven where the
    // real wires are: `tests/hub_e2e.rs` drives it as the link of a served
    // endpoint, and the Worker's forwarded link is proven in `horde-worker`
    // against a real server.

    use rmcp::model::Tool;

    /// The wire round trip, both payload kinds: what one end encodes the
    /// other decodes to the same value, schema and error flag included.
    #[test]
    fn tools_and_results_survive_the_forwarded_wire() {
        let mut tool = Tool::default();
        tool.name = "linear__echo".to_owned().into();
        tool.description = Some("echoes".into());
        tool.input_schema = std::sync::Arc::new(
            serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": { "limit": { "type": "integer" } },
            }))
            .unwrap_or_else(|error| panic!("a literal schema parses: {error}")),
        );

        let json = super::tool_to_json(&tool).unwrap_or_else(|error| panic!("encodes: {error}"));
        let back = super::tool_from_json(&json).unwrap_or_else(|error| panic!("decodes: {error}"));
        assert_eq!(back.name, "linear__echo");
        assert_eq!(back.description.as_deref(), Some("echoes"));
        assert_eq!(
            back.input_schema, tool.input_schema,
            "the schema is what the model reads; it must survive whole"
        );

        let result = rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text(
            "rate limited",
        )]);
        let json =
            super::result_to_json(&result).unwrap_or_else(|error| panic!("encodes: {error}"));
        let back =
            super::result_from_json(&json).unwrap_or_else(|error| panic!("decodes: {error}"));
        assert_eq!(back.is_error, Some(true));
        assert_eq!(
            super::first_text(&back),
            Some("rate limited"),
            "the first text block survives the wire and reads back whole"
        );
        assert_eq!(
            super::first_text(&rmcp::model::CallToolResult::success(vec![])),
            None,
            "no leading text block is None, never empty text"
        );
    }

    /// The argument wire keeps MCP's two no-ish states distinct, keeps
    /// integers integers, and refuses non-objects rather than guessing.
    #[test]
    fn arguments_round_trip_with_none_and_integers_intact() {
        assert_eq!(super::arguments_to_json(None), "");
        assert_eq!(
            super::arguments_from_json("").unwrap_or_else(|refusal| panic!("{refusal}")),
            None,
            "no arguments is not an empty object"
        );

        let mut fields = rmcp::model::JsonObject::new();
        fields.insert("limit".to_owned(), serde_json::json!(3));
        let json = super::arguments_to_json(Some(&fields));
        assert_eq!(
            json, r#"{"limit":3}"#,
            "an integer must not become 3.0 in transit — the wire is JSON \
             text precisely to avoid Struct's doubles"
        );
        let back = super::arguments_from_json(&json)
            .unwrap_or_else(|refusal| panic!("{refusal}"))
            .unwrap_or_else(|| panic!("an object came through"));
        assert_eq!(back.get("limit"), Some(&serde_json::json!(3)));

        let refused = super::arguments_from_json("[1,2]").expect_err("an array is not arguments");
        assert!(refused.contains("must be a JSON object"), "{refused}");
        let garbled = super::arguments_from_json("{not json").expect_err("garble is refused");
        assert!(garbled.contains("not JSON"), "{garbled}");
    }

    /// A readable failure carries the error's own words and the flag the
    /// model branches on.
    #[test]
    fn a_readable_failure_is_an_error_result_in_the_hubs_words() {
        let failure = super::readable_failure(&crate::hub::HubError::Upstream {
            server: "linear".to_owned(),
            tool: "echo".to_owned(),
            message: "rate limited".to_owned(),
        });
        assert_eq!(failure.is_error, Some(true));
        let text = format!("{:?}", failure.content);
        assert!(text.contains("rate limited"), "{text}");
        assert!(text.contains("linear"), "{text}");
    }
}
