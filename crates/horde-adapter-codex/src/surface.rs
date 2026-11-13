//! The slice of codex's app-server protocol this codec depends on, and how
//! to read it out of codex's own generated schema (W12c drift check).
//!
//! ADR-0005 chose `app-server` partly because its protocol is
//! machine-generated (`codex app-server generate-json-schema --out DIR`), so
//! version skew is diffable per release instead of discovered in production.
//! This module makes that concrete:
//!
//! * [`SURFACE`] declares every method and field path [`crate::wire`] reads
//!   and [`crate::codec`] sends — by hand, deliberately, because it is a
//!   statement of what the code depends on, not something to infer.
//! * [`extract`] resolves each declared path against a generated schema
//!   bundle into a one-line type summary (`string|null`,
//!   `enum:accept,decline`, …), or `MISSING`.
//! * `schema/surface.json` beside this crate is that extraction at the codex
//!   version the adapter was last measured against. The offline test checks
//!   the snapshot covers [`SURFACE`] exactly; the ignored live test
//!   regenerates it from the installed codex and diffs:
//!
//! ```sh
//! cargo test -p horde-adapter-codex --test schema_drift -- --ignored
//! # after reviewing a diff, refresh the snapshot:
//! HORDE_UPDATE_CODEX_SURFACE=1 cargo test -p horde-adapter-codex --test schema_drift -- --ignored
//! ```
//!
//! A path inside a union (`item.command` exists only on `commandExecution`
//! items) resolves if any branch has it; arrays are transparent (a path walks
//! into `items`); `*` walks into a map's values (`additionalProperties`).

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::Value;

/// Where one used method's schema lives in the bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// A variant of a method union (`ServerNotification.json`, …), chosen by
    /// its `method`; the paths start at its `params`.
    Method {
        file: &'static str,
        method: &'static str,
    },
    /// A whole schema file whose root is the object the paths start at — a
    /// response Horde sends or reads.
    File { file: &'static str },
}

impl Source {
    /// The snapshot key: the file, and the method when there is one.
    #[must_use]
    pub fn key(&self) -> String {
        match self {
            Self::Method { file, method } => format!("{file} {method}"),
            Self::File { file } => (*file).to_owned(),
        }
    }
}

/// One used method or message, and the field paths the codec touches in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Used {
    pub source: Source,
    pub paths: &'static [&'static str],
}

const fn notification(method: &'static str, paths: &'static [&'static str]) -> Used {
    Used {
        source: Source::Method {
            file: "ServerNotification.json",
            method,
        },
        paths,
    }
}

const fn server_request(method: &'static str, paths: &'static [&'static str]) -> Used {
    Used {
        source: Source::Method {
            file: "ServerRequest.json",
            method,
        },
        paths,
    }
}

const fn client_request(method: &'static str, paths: &'static [&'static str]) -> Used {
    Used {
        source: Source::Method {
            file: "ClientRequest.json",
            method,
        },
        paths,
    }
}

const fn file(file: &'static str, paths: &'static [&'static str]) -> Used {
    Used {
        source: Source::File { file },
        paths,
    }
}

/// Item fields both `item/started` and `item/completed` are read for.
const ITEM_PATHS: &[&str] = &[
    "turnId",
    "item.type",
    "item.id",
    "item.text",
    "item.phase",
    "item.command",
    "item.status",
    "item.exitCode",
    "item.server",
    "item.tool",
    "item.arguments",
    "item.error.message",
    "item.changes.path",
    "item.changes.kind",
    "item.changes.diff",
];

/// The thread config `thread/start` and `thread/resume` both send.
const THREAD_CONFIG_PATHS: &[&str] = &[
    "approvalPolicy",
    "approvalPolicy.granular.mcp_elicitations",
    "approvalPolicy.granular.rules",
    "approvalPolicy.granular.sandbox_approval",
    "approvalPolicy.granular.request_permissions",
    "approvalsReviewer",
    "sandbox",
    "cwd",
    "model",
    "developerInstructions",
];

/// Every method and field path the codec depends on.
pub const SURFACE: &[Used] = &[
    // What the assembler reads (wire.rs `Notification::classify`).
    notification("thread/started", &["thread.id"]),
    notification("thread/status/changed", &["threadId", "status.type"]),
    notification("turn/started", &["threadId", "turn.id"]),
    notification(
        "turn/completed",
        &[
            "threadId",
            "turn.id",
            "turn.status",
            "turn.durationMs",
            "turn.error.message",
            "turn.error.codexErrorInfo",
        ],
    ),
    notification("item/agentMessage/delta", &["turnId", "itemId", "delta"]),
    notification("item/started", ITEM_PATHS),
    notification("item/completed", ITEM_PATHS),
    notification(
        "thread/tokenUsage/updated",
        &[
            "threadId",
            "turnId",
            "tokenUsage.last.inputTokens",
            "tokenUsage.last.outputTokens",
            "tokenUsage.last.cachedInputTokens",
            "tokenUsage.last.cacheWriteInputTokens",
            "tokenUsage.last.reasoningOutputTokens",
            "tokenUsage.total.inputTokens",
            "tokenUsage.total.outputTokens",
            "tokenUsage.total.cachedInputTokens",
            "tokenUsage.total.cacheWriteInputTokens",
            "tokenUsage.total.reasoningOutputTokens",
        ],
    ),
    notification(
        "account/rateLimits/updated",
        &[
            "rateLimits.limitId",
            "rateLimits.primary.usedPercent",
            "rateLimits.primary.windowDurationMins",
            "rateLimits.primary.resetsAt",
        ],
    ),
    notification(
        "error",
        &[
            "error.message",
            "error.codexErrorInfo",
            "threadId",
            "turnId",
            "willRetry",
        ],
    ),
    notification("serverRequest/resolved", &["threadId", "requestId"]),
    // What the assembler turns into permission events.
    server_request(
        "mcpServer/elicitation/request",
        &["threadId", "turnId", "serverName", "message", "_meta"],
    ),
    server_request(
        "item/commandExecution/requestApproval",
        &[
            "threadId",
            "turnId",
            "itemId",
            "reason",
            "command",
            "cwd",
            "commandActions.command",
            "proposedExecpolicyAmendment",
        ],
    ),
    server_request(
        "item/fileChange/requestApproval",
        &["threadId", "turnId", "itemId", "reason", "grantRoot"],
    ),
    server_request(
        "item/permissions/requestApproval",
        &[
            "threadId",
            "turnId",
            "itemId",
            "cwd",
            "reason",
            "permissions",
        ],
    ),
    server_request(
        "item/tool/requestUserInput",
        &[
            "threadId",
            "turnId",
            "itemId",
            "questions.id",
            "questions.header",
            "questions.question",
            "questions.isOther",
            "questions.isSecret",
            "questions.options.label",
            "questions.options.description",
        ],
    ),
    // What the codec sends (codec.rs encoders).
    client_request(
        "initialize",
        &[
            "clientInfo.name",
            "clientInfo.title",
            "clientInfo.version",
            "capabilities.experimentalApi",
        ],
    ),
    client_request("thread/start", THREAD_CONFIG_PATHS),
    client_request("thread/resume", THREAD_CONFIG_PATHS),
    client_request("turn/start", &["threadId", "input"]),
    client_request("turn/steer", &["threadId", "expectedTurnId", "input"]),
    client_request("turn/interrupt", &["threadId"]),
    // The replies the codec sends to server requests.
    file(
        "McpServerElicitationRequestResponse.json",
        &["action", "content"],
    ),
    file(
        "CommandExecutionRequestApprovalResponse.json",
        &["decision"],
    ),
    file("FileChangeRequestApprovalResponse.json", &["decision"]),
    file(
        "PermissionsRequestApprovalResponse.json",
        &["permissions", "scope"],
    ),
    file("ToolRequestUserInputResponse.json", &["answers.*.answers"]),
    // What the live session reads off the thread response.
    file("v2/ThreadStartResponse.json", &["thread.id", "model"]),
    file("v2/ThreadResumeResponse.json", &["thread.id", "model"]),
];

/// The extraction: each source's key to its paths' type summaries.
pub type Extracted = BTreeMap<String, BTreeMap<String, String>>;

/// The summary a path that resolves nowhere gets.
pub const MISSING: &str = "MISSING";

/// Resolves [`SURFACE`] against the schema bundle in `schema_dir`.
///
/// # Errors
///
/// A sentence naming the file or method that could not be found — a
/// missing method is drift too, but it is reported rather than summarized,
/// since there is no schema to read paths from.
pub fn extract(schema_dir: &Path) -> Result<Extracted, String> {
    let mut extracted = Extracted::new();
    for used in SURFACE {
        let (root, start) = match used.source {
            Source::Method { file, method } => {
                let root = load(schema_dir, file)?;
                let params = method_params(&root, method)
                    .ok_or_else(|| format!("{file} has no method {method:?}"))?;
                (root, params)
            }
            Source::File { file } => {
                let root = load(schema_dir, file)?;
                let start = root.clone();
                (root, start)
            }
        };
        let paths = used
            .paths
            .iter()
            .map(|path| ((*path).to_owned(), summarize_path(&root, &start, path)))
            .collect();
        extracted.insert(used.source.key(), paths);
    }
    Ok(extracted)
}

fn load(schema_dir: &Path, file: &str) -> Result<Value, String> {
    let path = schema_dir.join(file);
    let text = std::fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    serde_json::from_str(&text).map_err(|error| format!("{} is not JSON: {error}", path.display()))
}

/// The `params` schema of the union variant whose `method` is `method`.
fn method_params(root: &Value, method: &str) -> Option<Value> {
    root.get("oneOf")?.as_array()?.iter().find_map(|variant| {
        let methods = variant.pointer("/properties/method")?;
        let named = methods
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|names| names.iter().any(|name| name == method))
            || methods.get("const").is_some_and(|name| name == method);
        if named {
            Some(
                variant
                    .pointer("/properties/params")
                    .cloned()
                    .unwrap_or(Value::Null),
            )
        } else {
            None
        }
    })
}

/// Follows `$ref`s (to `#/definitions/…` in `root`) until a concrete schema.
/// Bounded, so a self-referential schema cannot loop.
fn resolve(root: &Value, schema: &Value) -> Value {
    let mut current = schema.clone();
    for _ in 0..32 {
        let Some(reference) = current.get("$ref").and_then(Value::as_str) else {
            return current;
        };
        let Some(name) = reference.strip_prefix("#/definitions/") else {
            return current;
        };
        current = root
            .pointer(&format!("/definitions/{name}"))
            .cloned()
            .unwrap_or(Value::Null);
    }
    current
}

/// Every concrete schema a node stands for: itself, its union branches
/// (`anyOf`/`oneOf`/`allOf`), and — arrays being transparent — its items.
fn branches(root: &Value, schema: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    let mut queue = vec![schema.clone()];
    let mut steps = 0;
    while let Some(next) = queue.pop() {
        steps += 1;
        if steps > 256 {
            break;
        }
        let node = resolve(root, &next);
        for key in ["anyOf", "oneOf", "allOf"] {
            if let Some(alternatives) = node.get(key).and_then(Value::as_array) {
                queue.extend(alternatives.iter().cloned());
            }
        }
        if let Some(items) = node.get("items") {
            queue.push(items.clone());
        }
        out.push(node);
    }
    out
}

/// The schemas one path segment leads to from `schema`.
fn children(root: &Value, schema: &Value, segment: &str) -> Vec<Value> {
    branches(root, schema)
        .iter()
        .filter_map(|node| {
            if segment == "*" {
                node.get("additionalProperties")
                    .filter(|value| value.is_object())
                    .cloned()
            } else {
                node.pointer(&format!("/properties/{segment}")).cloned()
            }
        })
        .collect()
}

/// Walks `path` from `start` and summarizes what it reaches.
fn summarize_path(root: &Value, start: &Value, path: &str) -> String {
    let mut frontier = vec![start.clone()];
    for segment in path.split('.') {
        frontier = frontier
            .iter()
            .flat_map(|schema| children(root, schema, segment))
            .collect();
        if frontier.is_empty() {
            return MISSING.to_owned();
        }
    }
    summarize(root, &frontier)
}

/// A leaf's types and enum values, sorted and joined: `string|null`,
/// `enum:accept,decline|object`, …
fn summarize(root: &Value, leaves: &[Value]) -> String {
    let mut types = std::collections::BTreeSet::new();
    let mut values = std::collections::BTreeSet::new();
    for leaf in leaves {
        for node in branches(root, leaf) {
            if let Some(found) = node.get("enum").and_then(Value::as_array) {
                values.extend(found.iter().map(enum_text));
            }
            if let Some(constant) = node.get("const") {
                values.insert(enum_text(constant));
            }
            match node.get("type") {
                Some(Value::String(kind)) => {
                    types.insert(kind.clone());
                }
                Some(Value::Array(kinds)) => {
                    types.extend(kinds.iter().filter_map(Value::as_str).map(str::to_owned));
                }
                _ => {}
            }
        }
    }
    let mut parts: Vec<String> = Vec::new();
    if !values.is_empty() {
        parts.push(format!(
            "enum:{}",
            values.into_iter().collect::<Vec<_>>().join(",")
        ));
    }
    parts.extend(types);
    if parts.is_empty() {
        "any".to_owned()
    } else {
        parts.join("|")
    }
}

fn enum_text(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{MISSING, method_params, summarize_path};

    /// A miniature bundle with every construct the real one uses: a method
    /// union, `$ref`s, nullable `anyOf`, a tagged `oneOf`, an array, a map.
    fn root() -> serde_json::Value {
        json!({
            "oneOf": [
                {"properties": {"method": {"enum": ["a/b"]},
                                "params": {"$ref": "#/definitions/AB"}}},
                {"properties": {"method": {"const": "c/d"}, "params": {"type": "object"}}}
            ],
            "definitions": {
                "AB": {"type": "object", "properties": {
                    "id": {"type": "string"},
                    "maybe": {"anyOf": [{"$ref": "#/definitions/Inner"}, {"type": "null"}]},
                    "item": {"oneOf": [
                        {"type": "object", "properties": {"kind": {"enum": ["x"]}, "only_x": {"type": "integer"}}},
                        {"type": "object", "properties": {"kind": {"enum": ["y"]}}}
                    ]},
                    "list": {"type": "array", "items": {"$ref": "#/definitions/Inner"}},
                    "map": {"type": "object", "additionalProperties": {"$ref": "#/definitions/Inner"}},
                    "loop": {"$ref": "#/definitions/Loop"}
                }},
                "Inner": {"type": "object", "properties": {"v": {"type": ["string", "null"]}}},
                "Loop": {"$ref": "#/definitions/Loop"}
            }
        })
    }

    #[test]
    fn methods_are_found_by_enum_or_const() {
        let root = root();
        assert_eq!(
            method_params(&root, "a/b"),
            Some(json!({"$ref": "#/definitions/AB"}))
        );
        assert_eq!(method_params(&root, "c/d"), Some(json!({"type": "object"})));
        assert_eq!(method_params(&root, "e/f"), None);
    }

    #[test]
    fn paths_resolve_through_refs_unions_arrays_and_maps() {
        let root = root();
        let start = json!({"$ref": "#/definitions/AB"});
        let at = |path: &str| summarize_path(&root, &start, path);
        assert_eq!(at("id"), "string");
        assert_eq!(at("maybe.v"), "null|string");
        assert_eq!(at("item.kind"), "enum:x,y");
        assert_eq!(
            at("item.only_x"),
            "integer",
            "any branch may carry the path"
        );
        assert_eq!(at("list.v"), "null|string", "arrays are transparent");
        assert_eq!(at("map.*.v"), "null|string");
        assert_eq!(at("list"), "array|object");
        assert_eq!(at("nope"), MISSING);
        assert_eq!(at("id.deeper"), MISSING);
        assert_eq!(at("loop"), "any", "a self-reference ends, unsummarized");
    }
}
