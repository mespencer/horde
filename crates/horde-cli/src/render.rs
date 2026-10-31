//! The CLI's two presentations of a wire answer.
//!
//! `--json` is here from the first commit rather than retrofitted, because a
//! scriptable verb whose only output is prose is not scriptable, and adding
//! machine output later means every existing script has already grown a parser
//! for the human format.
//!
//! Rendering is deliberately total: an event kind this build does not model
//! prints as its kind name rather than vanishing. A client that silently drops
//! events it cannot pretty-print is worse than one that admits it saw
//! something.

use horde_proto::v1::{EventEnvelope, SessionLifecycleState, SessionSummary, event::Kind};
use serde_json::{Value, json};

/// How to print.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    /// Aligned prose for a terminal.
    #[default]
    Human,
    /// One JSON value per answer, for scripts.
    Json,
}

impl Format {
    #[must_use]
    pub fn from_flag(json: bool) -> Self {
        if json { Self::Json } else { Self::Human }
    }
}

/// Renders one session summary.
#[must_use]
pub fn session(summary: &SessionSummary, format: Format) -> String {
    match format {
        Format::Json => json!({
            "session_id": id(summary.session_id.as_ref().map(ToString::to_string)),
            // The handle resolution accepts (ADR-0008): JSON carries both id
            // and name so scripts never re-resolve.
            "name": crate::resolve::name_of(summary),
            "title": summary.title,
            "root_agent_id": id(summary.root_agent_id.as_ref().map(ToString::to_string)),
            "state": session_state(summary.state),
            "head_sequence": summary.head_sequence,
            "agent_count": summary.agent_count,
        })
        .to_string(),
        Format::Human => format!(
            "{}  {}  agents={} head={} {}",
            summary
                .session_id
                .as_ref()
                .map_or_else(|| "-".to_owned(), ToString::to_string),
            session_state(summary.state),
            summary.agent_count,
            summary.head_sequence,
            if summary.title.is_empty() {
                String::new()
            } else {
                summary.title.clone()
            }
        )
        .trim_end()
        .to_owned(),
    }
}

/// Renders a list of sessions, including the empty case.
#[must_use]
pub fn sessions(summaries: &[SessionSummary], format: Format) -> String {
    match format {
        Format::Json => {
            let rows: Vec<Value> = summaries
                .iter()
                .filter_map(|summary| serde_json::from_str(&session(summary, Format::Json)).ok())
                .collect();
            json!({ "sessions": rows }).to_string()
        }
        Format::Human => {
            if summaries.is_empty() {
                // Said rather than printed as a blank line: an empty result and
                // a swallowed error look identical otherwise.
                return "no sessions".to_owned();
            }
            session_rows(summaries, now_unix_ms()).join("\n")
        }
    }
}

/// The one row format `session list` and the picker share (ADR-0008):
/// name, short id, status, agent count, last-activity age. Choosing and
/// listing look identical because they are this same function.
///
/// Column widths come from the set, so the rows align without a fixed
/// budget; `now_unix_ms` is a parameter so a test's ages are facts.
#[must_use]
pub fn session_rows(summaries: &[SessionSummary], now_unix_ms: i64) -> Vec<String> {
    let ids: Vec<String> = summaries
        .iter()
        .map(|summary| {
            summary
                .session_id
                .as_ref()
                .map_or_else(|| "-".to_owned(), ToString::to_string)
        })
        .collect();
    let names: Vec<String> = summaries
        .iter()
        .map(|summary| {
            let name = crate::resolve::name_of(summary);
            if name.is_empty() {
                "-".to_owned()
            } else {
                name
            }
        })
        .collect();
    let shorts: Vec<String> = ids.iter().map(|one| short_id(one, &ids)).collect();
    let name_width = names.iter().map(String::len).max().unwrap_or(0);
    let short_width = shorts.iter().map(String::len).max().unwrap_or(0);

    summaries
        .iter()
        .zip(names.iter().zip(shorts))
        .map(|(summary, (name, short))| {
            format!(
                "{name:<name_width$}  {short:<short_width$}  {:<8}  agents={}  {}",
                session_state(summary.state),
                summary.agent_count,
                summary
                    .last_activity_at
                    .as_ref()
                    .map_or_else(|| "-".to_owned(), |at| age(now_unix_ms, at)),
            )
        })
        .collect()
}

/// The shortest prefix of `id` that no sibling shares, floored at twelve
/// characters so a row's short id never degenerates to the `session-` stem.
/// An id that is itself a prefix of a sibling stays full — a short id must
/// always resolve back to its own session, or it is a trap in a monospace
/// costume.
fn short_id(id: &str, all: &[String]) -> String {
    let floor = 12.min(id.len());
    for length in floor..id.len() {
        if !id.is_char_boundary(length) {
            continue;
        }
        let prefix = &id[..length];
        if all
            .iter()
            .all(|other| other == id || !other.starts_with(prefix))
        {
            return prefix.to_owned();
        }
    }
    id.to_owned()
}

/// A duration since `at`, in the coarsest single unit that is not zero —
/// ages are for scanning a list, not for arithmetic.
fn age(now_unix_ms: i64, at: &horde_proto::prost_types::Timestamp) -> String {
    let at_ms = at
        .seconds
        .saturating_mul(1_000)
        .saturating_add(i64::from(at.nanos) / 1_000_000);
    let seconds = (now_unix_ms.saturating_sub(at_ms)).max(0) / 1_000;
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3_600 => format!("{}m", seconds / 60),
        3_600..86_400 => format!("{}h", seconds / 3_600),
        _ => format!("{}d", seconds / 86_400),
    }
}

/// The wall clock, for the human list and the front door's rows; everything
/// assertable goes through [`session_rows`]'s parameter instead.
pub(crate) fn now_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        // A clock past what i64 milliseconds can hold saturates; a clock
        // before 1970 renders every age as fresh — the least wrong things a
        // display-only clock can do.
        Ok(elapsed) => i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// Renders one event from a sync stream.
#[must_use]
pub fn event(envelope: &EventEnvelope, format: Format) -> String {
    let sequence = envelope.sequence;
    let session_id = envelope
        .session_id
        .as_ref()
        .map_or_else(String::new, ToString::to_string);
    let inner = envelope.event.as_ref();
    let agent_id = inner
        .and_then(|event| event.agent_id.as_ref())
        .map(ToString::to_string);
    let kind_name = kind_name(inner.and_then(|event| event.kind.as_ref()));

    match format {
        Format::Json => json!({
            "sequence": sequence,
            "session_id": session_id,
            "agent_id": agent_id,
            "kind": kind_name,
            "detail": detail(inner.and_then(|event| event.kind.as_ref())),
        })
        .to_string(),
        Format::Human => {
            let detail = detail(inner.and_then(|event| event.kind.as_ref()));
            let rendered = detail.as_str().unwrap_or_default();
            format!(
                "{sequence:>6}  {kind_name:<24} {}{}",
                agent_id.unwrap_or_else(|| "-".to_owned()),
                if rendered.is_empty() {
                    String::new()
                } else {
                    format!("  {rendered}")
                }
            )
        }
    }
}

/// The event kind's wire name, straight from the compiled schema via
/// [`horde_proto::kind_field_name`] — no hand-kept match to drift or outgrow
/// the complexity gate. An event with no kind (or one this build's descriptor
/// cannot name) is still a position in the log, and hiding it would make the
/// printed sequence look gapped, so it renders as `unknown`.
fn kind_name(kind: Option<&Kind>) -> &'static str {
    kind.and_then(horde_proto::kind_field_name)
        .unwrap_or("unknown")
}

/// The one piece of an event worth showing beside its kind.
///
/// Deliberately shallow: a client that dumps whole payloads is unreadable, and
/// `--json` already carries everything a script needs.
fn detail(kind: Option<&Kind>) -> Value {
    match kind {
        Some(Kind::AgentOutputChunk(chunk)) => Value::String(chunk.text.clone()),
        Some(Kind::AssignmentSent(sent)) => Value::String(
            sent.assignment
                .as_ref()
                .map(|assignment| assignment.text.clone())
                .unwrap_or_default(),
        ),
        Some(Kind::NoteSent(sent)) => Value::String(
            sent.note
                .as_ref()
                .map(|note| note.text.clone())
                .unwrap_or_default(),
        ),
        Some(Kind::PlacementChanged(changed)) => Value::String(
            changed
                .placement
                .as_ref()
                .map(|placement| {
                    let state = placement_state(placement.state);
                    if placement.reason.is_empty() {
                        state.to_owned()
                    } else {
                        format!("{state}: {}", placement.reason)
                    }
                })
                .unwrap_or_default(),
        ),
        Some(Kind::TurnInterrupted(interrupted)) => Value::String(interrupted.reason.clone()),
        // Structured, not a prose line: a script following a session needs
        // the outcome it can branch on, and burying it in a sentence would
        // make every consumer a parser (W7b finding 4).
        Some(Kind::ResultReported(reported)) => reported.result.as_ref().map_or_else(
            || Value::String(String::new()),
            |result| {
                json!({
                    "status": result_status(result.status),
                    "summary": result.summary,
                    "git_refs": result
                        .payload
                        .as_ref()
                        .map(|payload| payload
                            .git_refs
                            .iter()
                            .map(|git_ref| json!({
                                "repo": git_ref.repo_name,
                                "commit": git_ref.commit_sha,
                                "branch": git_ref.branch,
                            }))
                            .collect::<Vec<_>>())
                        .unwrap_or_default(),
                })
            },
        ),
        // Structured for the same W7b reason as the result: the id is what
        // `horde approve` needs, and a follower that cannot learn it from
        // the stream cannot answer at all.
        Some(Kind::PermissionRequested(requested)) => requested.request.as_ref().map_or_else(
            || Value::String(String::new()),
            |request| {
                json!({
                    "request_id": request
                        .request_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    "tool": request
                        .tool_call
                        .as_ref()
                        .map(|call| call.tool_name.clone())
                        .unwrap_or_default(),
                })
            },
        ),
        Some(Kind::PermissionDecided(decided)) => decided.decision.as_ref().map_or_else(
            || Value::String(String::new()),
            |decision| {
                json!({
                    "outcome": permission_outcome(decision.outcome),
                    "reason": decision.reason,
                    "remembered": decision.remember_for_session,
                    "updated_input": decision
                        .updated_input
                        .as_ref()
                        .map(horde_wire::struct_to_json),
                })
            },
        ),
        _ => Value::String(String::new()),
    }
}

/// The wire result status, in the vocabulary `horde-wire` owns for it.
fn result_status(status: i32) -> &'static str {
    horde_wire::result_status_name(status)
}

/// The wire outcome, in the words `approve` reports.
fn permission_outcome(outcome: i32) -> &'static str {
    match horde_proto::v1::PermissionOutcome::try_from(outcome) {
        Ok(horde_proto::v1::PermissionOutcome::Allow) => "allowed",
        Ok(horde_proto::v1::PermissionOutcome::Deny) => "denied",
        Ok(horde_proto::v1::PermissionOutcome::Unspecified) | Err(_) => "unknown",
    }
}

fn session_state(state: i32) -> &'static str {
    match SessionLifecycleState::try_from(state) {
        Ok(SessionLifecycleState::Active) => "active",
        Ok(SessionLifecycleState::Archived) => "archived",
        _ => "unspecified",
    }
}

/// Delegated to the TUI crate's vocabulary: two frontends, one set of words
/// for the same wire values, and one place for a new state to be named.
fn placement_state(state: i32) -> &'static str {
    horde_tui::state::placement_state_name(state)
}

fn id(value: Option<String>) -> Value {
    value.map_or(Value::Null, Value::String)
}

/// Renders the server handshake.
#[must_use]
pub fn hello(server: &horde_proto::v1::ServerHello, format: Format) -> String {
    match format {
        Format::Json => json!({
            "semver": server.semver,
            "min_supported_semver": server.min_supported_semver,
            "capability_flags": server.capability_flags,
            "principal": server.principal,
        })
        .to_string(),
        Format::Human => {
            let flags = if server.capability_flags.is_empty() {
                "none".to_owned()
            } else {
                server.capability_flags.join(",")
            };
            format!(
                "horde-server {} (min supported client {})\ncapabilities: {flags}",
                server.semver, server.min_supported_semver
            )
        }
    }
}

/// Renders a freshly created session and the root agent that came with it.
#[must_use]
pub fn created(
    summary: &SessionSummary,
    root_agent_id: &horde_proto::v1::AgentId,
    format: Format,
) -> String {
    match format {
        Format::Json => {
            let mut value: Value =
                serde_json::from_str(&session(summary, Format::Json)).unwrap_or_else(|_| json!({}));
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "root_agent_id".to_owned(),
                    Value::String(root_agent_id.to_string()),
                );
            }
            value.to_string()
        }
        // The root agent id is what the next command needs, so it goes on the
        // line the user will copy from.
        Format::Human => format!(
            "{}\nroot agent: {root_agent_id}",
            session(summary, Format::Human)
        ),
    }
}

/// Renders the outcome of a send.
#[must_use]
pub fn sent(response: &horde_proto::v1::SendMessageResponse, format: Format) -> String {
    let assignment_id = response.assignment_id.as_ref().map(ToString::to_string);

    match format {
        Format::Json => json!({
            "sequence": response.sequence,
            "session_id": id(response.session_id.as_ref().map(ToString::to_string)),
            "assignment_id": id(assignment_id),
        })
        .to_string(),
        Format::Human => match assignment_id {
            // Only assignments get an id, because only they owe a result.
            Some(id) => format!("sent at {} (assignment {id})", response.sequence),
            None => format!("sent at {}", response.sequence),
        },
    }
}

/// Renders a spawn, placed or queued.
#[must_use]
pub fn spawned(response: &horde_proto::v1::SpawnAgentResponse, format: Format) -> String {
    let agent_id = response
        .agent_id
        .as_ref()
        .map_or_else(|| "-".to_owned(), ToString::to_string);
    let placement = response.placement.as_ref();
    let state = placement.map_or("unspecified", |placement| placement_state(placement.state));
    let reason = placement.map_or("", |placement| placement.reason.as_str());
    let worker =
        placement.and_then(|placement| placement.worker_id.as_ref().map(ToString::to_string));

    match format {
        Format::Json => json!({
            "agent_id": agent_id,
            "placement": state,
            "worker_id": id(worker),
            "reason": reason,
        })
        .to_string(),
        // A queued spawn without its reason is not actionable, so the reason is
        // never dropped from the human form.
        Format::Human => {
            let mut line = format!("{agent_id}  {state}");
            if let Some(worker) = worker {
                line.push_str(&format!(" on {worker}"));
            }
            if !reason.is_empty() {
                line.push_str(&format!("  ({reason})"));
            }
            line
        }
    }
}

/// Renders a minted enrollment code (DESIGN §3, packaged per ADR-0007).
///
/// The token line is the artifact the human carries to the joining machine;
/// everything else is context. `token` is `None` only when the server named
/// no enrollment port — an older server — where the bare code is all there
/// honestly is to print.
#[must_use]
pub fn minted(
    response: &horde_proto::v1::MintEnrollmentCodeResponse,
    token: Option<&horde_identity::JoinToken>,
    format: Format,
) -> String {
    let expires_at_unix_ms = response.expires_at.as_ref().map_or(0, |at| {
        at.seconds.saturating_mul(1_000) + i64::from(at.nanos) / 1_000_000
    });
    match format {
        Format::Json => json!({
            "token": token.map(ToString::to_string),
            "code": response.code,
            "server_cert_fingerprint": response.server_cert_fingerprint,
            "expires_at_unix_ms": expires_at_unix_ms,
        })
        .to_string(),
        Format::Human => match token {
            Some(token) => format!("token {token}\nexpires-at-unix-ms {expires_at_unix_ms}"),
            None => format!(
                "code {}\nexpires-at-unix-ms {expires_at_unix_ms}",
                response.code
            ),
        },
    }
}

/// Renders a revocation: who, and which serials stopped working.
#[must_use]
pub fn revoked(name: &str, serials: &str, format: Format) -> String {
    match format {
        Format::Json => json!({
            "revoked": name,
            "serials": serials.split(',').filter(|s| !s.is_empty()).collect::<Vec<_>>(),
        })
        .to_string(),
        Format::Human if serials.is_empty() => {
            // Nothing active is not an error, but saying "revoked" for a
            // no-op would hide that the name held no certificate at all.
            format!("{name} held no active certificate")
        }
        Format::Human => format!("revoked {name}: {serials}"),
    }
}

/// One MCP server config as JSON — the same fields both `mcp` renders show,
/// so the human and JSON forms cannot disagree about what a server is.
fn mcp_server_json(config: &horde_proto::v1::McpServerConfig) -> serde_json::Value {
    let transport = match &config.transport {
        Some(horde_proto::v1::mcp_server_config::Transport::Stdio(stdio)) => json!({
            "stdio": { "command": stdio.command, "args": stdio.args },
        }),
        Some(horde_proto::v1::mcp_server_config::Transport::Http(http)) => {
            json!({ "http": { "url": http.url } })
        }
        None => serde_json::Value::Null,
    };
    json!({
        "name": config.name,
        "transport": transport,
        "local": config.local,
        "allowed_agent_types": config.allowed_agent_types,
        "etag": config.etag,
    })
}

/// One human line per server: name, placement, transport, scope, etag.
fn mcp_server_line(config: &horde_proto::v1::McpServerConfig) -> String {
    let placement = if config.local { "local" } else { "hub" };
    let transport = match &config.transport {
        Some(horde_proto::v1::mcp_server_config::Transport::Stdio(stdio)) => {
            format!("stdio {}", stdio.command)
        }
        Some(horde_proto::v1::mcp_server_config::Transport::Http(http)) => {
            format!("http {}", http.url)
        }
        None => "no transport".to_owned(),
    };
    let scope = if config.allowed_agent_types.is_empty() {
        "all agents".to_owned()
    } else {
        config.allowed_agent_types.join(",")
    };
    format!(
        "{} [{placement}] {transport} ({scope}) etag {}",
        config.name, config.etag
    )
}

/// Renders the config a `mcp add` produced, etag included — the token the
/// next update or removal must quote.
#[must_use]
pub fn mcp_configured(config: &horde_proto::v1::McpServerConfig, format: Format) -> String {
    match format {
        Format::Json => mcp_server_json(config).to_string(),
        Format::Human => format!("configured {}", mcp_server_line(config)),
    }
}

/// Renders the catalog. An empty catalog says so rather than printing
/// nothing a script or a human could misread as an error.
#[must_use]
pub fn mcp_servers(servers: &[horde_proto::v1::McpServerConfig], format: Format) -> String {
    match format {
        Format::Json => json!({
            "servers": servers.iter().map(mcp_server_json).collect::<Vec<_>>(),
        })
        .to_string(),
        Format::Human if servers.is_empty() => "no MCP servers configured".to_owned(),
        Format::Human => servers
            .iter()
            .map(mcp_server_line)
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn workspace_json(definition: &horde_proto::v1::WorkspaceDefinition) -> serde_json::Value {
    json!({
        "workspace_definition_id": id(definition
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)),
        "name": definition.name,
        "etag": definition.etag,
        "repos": definition
            .repos
            .iter()
            .map(|repo| json!({
                "name": repo.name,
                "remote_url": repo.remote_url,
                "default_branch": repo.default_branch,
                "layout_path": repo.layout_path,
            }))
            .collect::<Vec<_>>(),
        "unmanaged_paths": definition.unmanaged_paths,
    })
}

/// One human line per definition: id, name, repo count, etag.
fn workspace_line(definition: &horde_proto::v1::WorkspaceDefinition) -> String {
    format!(
        "{}  {}  {} repo(s)  etag {}",
        definition
            .workspace_definition_id
            .as_ref()
            .map_or_else(|| "-".to_owned(), ToString::to_string),
        definition.name,
        definition.repos.len(),
        definition.etag
    )
}

/// Renders one definition — what `create` and `add-repo` answer with.
#[must_use]
pub fn workspace(definition: &horde_proto::v1::WorkspaceDefinition, format: Format) -> String {
    match format {
        Format::Json => workspace_json(definition).to_string(),
        Format::Human => workspace_line(definition),
    }
}

/// Renders the definition list.
#[must_use]
pub fn workspaces(definitions: &[horde_proto::v1::WorkspaceDefinition], format: Format) -> String {
    match format {
        Format::Json => json!({
            "workspaces": definitions.iter().map(workspace_json).collect::<Vec<_>>(),
        })
        .to_string(),
        Format::Human if definitions.is_empty() => "no workspaces defined".to_owned(),
        Format::Human => definitions
            .iter()
            .map(workspace_line)
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

/// Renders one definition and its known instances — what `get` answers with.
#[must_use]
pub fn workspace_with_instances(
    response: &horde_proto::v1::GetWorkspaceDefinitionResponse,
    format: Format,
) -> String {
    let instances: Vec<serde_json::Value> = response
        .instances
        .iter()
        .map(|report| {
            json!({
                "worker_id": id(report.worker_id.as_ref().map(ToString::to_string)),
                "state": instance_state(report.state),
                "root_path": report.root_path,
                "detail": report.detail,
            })
        })
        .collect();
    match format {
        Format::Json => {
            let mut rendered = response
                .definition
                .as_ref()
                .map_or_else(|| json!({}), workspace_json);
            rendered["instances"] = serde_json::Value::Array(instances);
            rendered.to_string()
        }
        Format::Human => {
            let mut lines = vec![
                response
                    .definition
                    .as_ref()
                    .map_or_else(|| "no such workspace".to_owned(), workspace_line),
            ];
            for report in &response.instances {
                lines.push(format!(
                    "  instance on {}: {}",
                    report
                        .worker_id
                        .as_ref()
                        .map_or_else(|| "-".to_owned(), ToString::to_string),
                    instance_state(report.state)
                ));
            }
            lines.join("\n")
        }
    }
}

/// Renders a deletion.
#[must_use]
pub fn workspace_deleted(workspace_definition_id: &str, format: Format) -> String {
    match format {
        Format::Json => json!({ "deleted": workspace_definition_id }).to_string(),
        Format::Human => format!("deleted {workspace_definition_id}"),
    }
}

/// Renders a prewarm's acceptance report.
#[must_use]
pub fn prewarmed(
    report: Option<&horde_proto::v1::WorkspaceInstanceReport>,
    format: Format,
) -> String {
    match (format, report) {
        (Format::Json, Some(report)) => json!({
            "worker_id": id(report.worker_id.as_ref().map(ToString::to_string)),
            "state": instance_state(report.state),
            "detail": report.detail,
        })
        .to_string(),
        (Format::Json, None) => json!({ "state": "accepted" }).to_string(),
        (Format::Human, Some(report)) => format!(
            "prewarm accepted on {}: {}",
            report
                .worker_id
                .as_ref()
                .map_or_else(|| "-".to_owned(), ToString::to_string),
            instance_state(report.state)
        ),
        (Format::Human, None) => "prewarm accepted".to_owned(),
    }
}

/// Renders the connected-worker list.
#[must_use]
pub fn workers(workers: &[horde_proto::v1::WorkerSummary], format: Format) -> String {
    match format {
        Format::Json => json!({
            "workers": workers
                .iter()
                .map(|worker| json!({
                    "worker_id": id(worker.worker_id.as_ref().map(ToString::to_string)),
                    "name": worker.name,
                    "state": worker_state(worker.state),
                    "semver": worker.semver,
                }))
                .collect::<Vec<_>>(),
        })
        .to_string(),
        Format::Human if workers.is_empty() => "no workers connected".to_owned(),
        Format::Human => workers
            .iter()
            .map(|worker| {
                format!(
                    "{}  {}",
                    worker
                        .worker_id
                        .as_ref()
                        .map_or_else(|| "-".to_owned(), ToString::to_string),
                    worker_state(worker.state)
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn instance_state(state: i32) -> &'static str {
    match horde_proto::v1::WorkspaceInstanceState::try_from(state) {
        Ok(horde_proto::v1::WorkspaceInstanceState::Materializing) => "materializing",
        Ok(horde_proto::v1::WorkspaceInstanceState::Ready) => "ready",
        Ok(horde_proto::v1::WorkspaceInstanceState::Degraded) => "degraded",
        Ok(horde_proto::v1::WorkspaceInstanceState::Missing) => "missing",
        _ => "unspecified",
    }
}

fn worker_state(state: i32) -> &'static str {
    match horde_proto::v1::WorkerConnectionState::try_from(state) {
        Ok(horde_proto::v1::WorkerConnectionState::Online) => "online",
        Ok(horde_proto::v1::WorkerConnectionState::Offline) => "offline",
        _ => "unspecified",
    }
}

/// Renders a removal.
#[must_use]
pub fn mcp_removed(name: &str, format: Format) -> String {
    match format {
        Format::Json => json!({ "removed": name }).to_string(),
        Format::Human => format!("removed {name}"),
    }
}

/// Renders a completed join: who this machine now is, and where that lives.
#[must_use]
pub fn joined(principal: &str, directory: &std::path::Path, format: Format) -> String {
    match format {
        Format::Json => json!({
            "principal": principal,
            "identity_dir": directory.display().to_string(),
        })
        .to_string(),
        Format::Human => format!(
            "enrolled as {principal}; identity in {}",
            directory.display()
        ),
    }
}

/// Renders a stashed blob: the address is the point — it is what an
/// attachment (or a later fetch) carries.
#[must_use]
pub fn stashed(stored: &horde_proto::v1::UploadBlobResponse, format: Format) -> String {
    match format {
        Format::Json => json!({
            "blob_digest": stored.blob_digest,
            "size_bytes": stored.size_bytes,
        })
        .to_string(),
        Format::Human => format!(
            "stashed {} bytes at {}",
            stored.size_bytes, stored.blob_digest
        ),
    }
}

/// One `forward ls` answer: names, directions, ports, and the realization
/// overlay only the server's registry can vouch for.
#[must_use]
pub fn forwards(statuses: &[horde_proto::v1::ForwardStatus], format: Format) -> String {
    match format {
        Format::Json => {
            let rows: Vec<_> = statuses.iter().map(forward_json).collect();
            json!(rows).to_string()
        }
        Format::Human => {
            if statuses.is_empty() {
                return "no live forwards".to_owned();
            }
            statuses
                .iter()
                .map(forward_line)
                .collect::<Vec<_>>()
                .join("\n")
        }
    }
}

fn forward_json(status: &horde_proto::v1::ForwardStatus) -> serde_json::Value {
    let intent = status.intent.clone().unwrap_or_default();
    let spec = intent.spec.unwrap_or_default();
    json!({
        "forward_id": intent.forward_id.map(|id| id.to_string()).unwrap_or_default(),
        "session_id": intent.session_id.map(|id| id.to_string()).unwrap_or_default(),
        "agent_id": intent.agent_id.map(|id| id.to_string()).unwrap_or_default(),
        "name": spec.name,
        "direction": direction_word(spec.direction),
        "worker_port": spec.worker_port,
        "client_port": spec.client_port,
        "backer": intent.backer_client_id.map(|id| id.to_string()),
        "worker_reachable": status.worker_reachable,
        "backed": status.backed,
    })
}

fn forward_line(status: &horde_proto::v1::ForwardStatus) -> String {
    let intent = status.intent.clone().unwrap_or_default();
    let spec = intent.spec.unwrap_or_default();
    let mut line = format!(
        "{}  {} {}  session {}  {}",
        spec.name,
        direction_word(spec.direction),
        spec.worker_port,
        intent
            .session_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        if status.worker_reachable {
            "worker ok"
        } else {
            "worker unreachable"
        },
    );
    if direction_word(spec.direction) == "inject" {
        let backer = intent
            .backer_client_id
            .map(|id| id.to_string())
            .unwrap_or_default();
        if status.backed {
            line.push_str(&format!("  backed by {backer}"));
        } else {
            line.push_str(&format!("  UNBACKED ({backer} detached)"));
        }
    }
    line
}

use horde_proto::forward::direction_name as direction_word;

/// A realized expose: the mapping the ADR promises, fallback named when the
/// requested port was taken.
#[must_use]
pub fn forward_realized(
    name: &str,
    forward_id: &str,
    local_addr: std::net::SocketAddr,
    fell_back: bool,
    format: Format,
) -> String {
    match format {
        Format::Json => json!({
            "forward_id": forward_id,
            "name": name,
            "local_addr": local_addr.to_string(),
            "fell_back": fell_back,
        })
        .to_string(),
        Format::Human => {
            let mut line = format!("{name} → {local_addr}  ({forward_id})");
            if fell_back {
                line.push_str("  [requested port was taken; ephemeral fallback]");
            }
            line
        }
    }
}

/// A backed inject: what this client serves, and where it lands worker-side.
#[must_use]
pub fn forward_backing(
    name: &str,
    forward_id: &str,
    local_port: u16,
    worker_port: u16,
    format: Format,
) -> String {
    match format {
        Format::Json => json!({
            "forward_id": forward_id,
            "name": name,
            "local_port": local_port,
            "worker_port": worker_port,
        })
        .to_string(),
        Format::Human => format!(
            "backing {name}: 127.0.0.1:{local_port} here → worker localhost{}  ({forward_id})",
            if worker_port == 0 {
                String::new()
            } else {
                format!(":{worker_port}")
            }
        ),
    }
}

/// Renders a fetched blob: where the verified bytes landed.
#[must_use]
pub fn fetched(address: &str, out: &std::path::Path, size_bytes: u64, format: Format) -> String {
    match format {
        Format::Json => json!({
            "blob_digest": address,
            "out": out.display().to_string(),
            "size_bytes": size_bytes,
        })
        .to_string(),
        Format::Human => format!(
            "fetched {address} ({size_bytes} bytes, verified) into {}",
            out.display()
        ),
    }
}

/// Renders an acknowledgement that carries only a log position.
#[must_use]
pub fn sequence(verb: &str, sequence: u64, format: Format) -> String {
    match format {
        Format::Json => json!({ "result": verb, "sequence": sequence }).to_string(),
        // The sequence is the point: a caller can sync from exactly here
        // instead of racing its own write.
        Format::Human => format!("{verb} at {sequence}"),
    }
}

/// How a usage answer's rows are grouped, in this renderer's own words.
///
/// Render-owned rather than imported from `cli` (which would close a module
/// cycle) and richer than the bare wire enum: the wire key cannot say which
/// grouping produced it — an empty model and an empty account are both empty
/// strings — so labelling a row needs the grouping stated alongside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageGrouping {
    Agent,
    Session,
    Tree,
    Account,
    Model,
}

impl UsageGrouping {
    /// The heading word: "usage by {word}".
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Session => "session",
            Self::Tree => "tree path",
            Self::Account => "provider account",
            Self::Model => "model",
        }
    }
}

/// Renders a usage answer: aggregate rows (measured from journaled events)
/// and per-account standing (measured window and sums beside the derived
/// headroom, labelled with its basis so the two are never mistaken).
#[must_use]
pub fn usage(
    response: &horde_proto::v1::QueryUsageResponse,
    grouping: UsageGrouping,
    format: Format,
) -> String {
    match format {
        Format::Json => json!({
            "grouping": grouping.word(),
            "aggregates": response
                .aggregates
                .iter()
                .map(|aggregate| aggregate_json(aggregate, grouping))
                .collect::<Vec<Value>>(),
            "accounts": response
                .accounts
                .iter()
                .map(account_json)
                .collect::<Vec<Value>>(),
        })
        .to_string(),
        Format::Human => {
            let mut lines = vec![format!("usage by {} (measured)", grouping.word())];
            if response.aggregates.is_empty() {
                // Said rather than blank: an empty result and a swallowed
                // error look identical otherwise.
                lines.push("  no usage recorded".to_owned());
            }
            for aggregate in &response.aggregates {
                lines.push(aggregate_row(aggregate, grouping));
            }
            if !response.accounts.is_empty() {
                lines.push("provider accounts".to_owned());
                for account in &response.accounts {
                    lines.push(account_row(account));
                }
            }
            lines.join("\n")
        }
    }
}

fn aggregate_row(aggregate: &horde_proto::v1::UsageAggregate, grouping: UsageGrouping) -> String {
    let tokens = aggregate.tokens.unwrap_or_default();
    format!(
        "  {}  {}  agents {}",
        group_label(grouping, &aggregate.key.clone().unwrap_or_default()),
        tokens_human(&tokens),
        aggregate.agent_count,
    )
}

/// One account's standing on one line: name, derived headroom, and the basis
/// naming where the derivation came from — "(derived …)" versus the plainly
/// measured token columns is the measured/derived distinction, spelled out.
fn account_row(account: &horde_proto::v1::AccountHeadroom) -> String {
    let name = pair(&account.provider, &account.account, "(none)");
    let standing = account_standing(account.exhausted, account.remaining_fraction);
    let tokens = account.tokens.unwrap_or_default();
    format!(
        "  {name}  {standing} ({})  spent {}",
        account.basis,
        tokens_human(&tokens)
    )
}

/// The derived half of an account row, in words.
fn account_standing(exhausted: bool, remaining_fraction: Option<f64>) -> String {
    match (exhausted, remaining_fraction) {
        (true, _) => "EXHAUSTED".to_owned(),
        (false, Some(remaining)) => format!("{:.0}% left", remaining * 100.0),
        (false, None) => "headroom unknown".to_owned(),
    }
}

/// The measured token columns, in one fixed order.
fn tokens_human(tokens: &horde_proto::v1::TokenUsage) -> String {
    format!(
        "in {}  out {}  cache r/w {}/{}  reasoning {}",
        tokens.input_tokens,
        tokens.output_tokens,
        tokens.cache_read_input_tokens,
        tokens.cache_write_input_tokens,
        tokens.reasoning_output_tokens,
    )
}

/// The row label a group key renders as, under the grouping that made it.
fn group_label(grouping: UsageGrouping, key: &horde_proto::v1::UsageGroupKey) -> String {
    let name = |value: Option<String>| value.unwrap_or_else(|| "-".to_owned());
    match grouping {
        UsageGrouping::Agent => format!(
            "{} {}",
            name(key.session_id.as_ref().map(ToString::to_string)),
            name(key.agent_id.as_ref().map(ToString::to_string)),
        ),
        UsageGrouping::Session => name(key.session_id.as_ref().map(ToString::to_string)),
        UsageGrouping::Tree => {
            if key.tree_path.is_empty() {
                "(no tree path)".to_owned()
            } else {
                key.tree_path.join(" > ")
            }
        }
        UsageGrouping::Account => pair(&key.provider, &key.provider_account, "(none)"),
        UsageGrouping::Model => pair(&key.provider, &key.model, "(default)"),
    }
}

/// `provider/name`, with the empties named rather than rendered as bare
/// slashes: a record journaled with no binding is "(unattributed)", and an
/// empty second half gets the word the grouping gives it — "(default)" for a
/// model (the harness's own default model is a real state), "(none)" for an
/// account.
fn pair(provider: &str, name: &str, empty_name: &str) -> String {
    let unattributed = provider.is_empty() && name.is_empty();
    if unattributed {
        return "(unattributed)".to_owned();
    }
    let name = if name.is_empty() { empty_name } else { name };
    format!("{provider}/{name}")
}

fn aggregate_json(aggregate: &horde_proto::v1::UsageAggregate, grouping: UsageGrouping) -> Value {
    let key = aggregate.key.clone().unwrap_or_default();
    json!({
        "label": group_label(grouping, &key),
        "key": {
            "session_id": id(key.session_id.as_ref().map(ToString::to_string)),
            "agent_id": id(key.agent_id.as_ref().map(ToString::to_string)),
            "tree_path": key.tree_path,
            "provider": key.provider,
            "provider_account": key.provider_account,
            "model": key.model,
        },
        "measured": {
            "tokens": tokens_json(&aggregate.tokens.unwrap_or_default()),
            "agent_count": aggregate.agent_count,
        },
    })
}

/// Measured facts and derived arithmetic are separate objects in the JSON,
/// so a script cannot read Horde's arithmetic as the provider's number.
fn account_json(account: &horde_proto::v1::AccountHeadroom) -> Value {
    json!({
        "provider": account.provider,
        "account": account.account,
        "measured": {
            "tokens": tokens_json(&account.tokens.unwrap_or_default()),
            "window": account.window.as_ref().map_or(Value::Null, window_json),
        },
        "derived": {
            "remaining_fraction": account.remaining_fraction,
            "exhausted": account.exhausted,
            "basis": account.basis,
        },
    })
}

fn window_json(window: &horde_proto::v1::RateWindow) -> Value {
    let source = match horde_proto::v1::HeadroomSource::try_from(window.source) {
        Ok(horde_proto::v1::HeadroomSource::ProviderReported) => "provider-reported",
        Ok(horde_proto::v1::HeadroomSource::HordeMetered) => "horde-metered",
        _ => "unspecified",
    };
    json!({
        "source": source,
        "used": window.used,
        "limit": window.limit,
        "used_fraction": window.used_fraction,
        "allowed": window.allowed,
    })
}

fn tokens_json(tokens: &horde_proto::v1::TokenUsage) -> Value {
    json!({
        "input_tokens": tokens.input_tokens,
        "output_tokens": tokens.output_tokens,
        "cache_read_input_tokens": tokens.cache_read_input_tokens,
        "cache_write_input_tokens": tokens.cache_write_input_tokens,
        "reasoning_output_tokens": tokens.reasoning_output_tokens,
    })
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, AgentOutputChannel, AgentOutputChunk, Event, EventEnvelope, Placement,
        PlacementChanged, PlacementState, SessionId, SessionLifecycleState, SessionSummary,
        event::Kind,
    };

    use super::{
        Format, created, event, hello, instance_state, prewarmed, sent, sequence, session,
        sessions, spawned, worker_state, workers, workspace, workspace_deleted,
        workspace_with_instances, workspaces,
    };

    /// The `mcp` renders: the empty catalog says so in words (a script or a
    /// human reading nothing cannot tell it from a swallowed error), a
    /// populated one carries the fields a follow-up command needs, and a
    /// removal names what was removed.
    #[test]
    fn the_mcp_renders_say_what_happened_in_both_formats() {
        let config = horde_proto::v1::McpServerConfig {
            name: "linear".to_owned(),
            transport: Some(horde_proto::v1::mcp_server_config::Transport::Stdio(
                horde_proto::v1::McpStdioTransport {
                    command: "linear-mcp".to_owned(),
                    ..horde_proto::v1::McpStdioTransport::default()
                },
            )),
            local: true,
            etag: "3".to_owned(),
            ..horde_proto::v1::McpServerConfig::default()
        };

        assert_eq!(
            super::mcp_servers(&[], Format::Human),
            "no MCP servers configured"
        );
        let listed = super::mcp_servers(std::slice::from_ref(&config), Format::Human);
        assert_eq!(
            listed, "linear [local] stdio linear-mcp (all agents) etag 3",
            "the one line carries name, placement, transport, scope, and the \
             etag the next mutation must quote"
        );

        let json: serde_json::Value = serde_json::from_str(&super::mcp_servers(
            std::slice::from_ref(&config),
            Format::Json,
        ))
        .expect("valid JSON");
        assert_eq!(json["servers"][0]["name"], "linear");
        assert_eq!(json["servers"][0]["etag"], "3");

        assert_eq!(
            super::mcp_configured(&config, Format::Human),
            "configured linear [local] stdio linear-mcp (all agents) etag 3"
        );
        assert_eq!(
            super::mcp_removed("linear", Format::Human),
            "removed linear"
        );
        let removed: serde_json::Value =
            serde_json::from_str(&super::mcp_removed("linear", Format::Json)).expect("valid JSON");
        assert_eq!(removed["removed"], "linear");
    }

    fn summary() -> SessionSummary {
        SessionSummary {
            session_id: Some(SessionId::new("s1")),
            title: "ci-fix".to_owned(),
            root_agent_id: Some(AgentId::new("root")),
            workspace_definition_id: None,
            state: SessionLifecycleState::Active as i32,
            created_at: None,
            head_sequence: 12,
            agent_count: 3,
            last_activity_at: None,
        }
    }

    fn envelope(kind: Option<Kind>) -> EventEnvelope {
        EventEnvelope {
            session_id: Some(SessionId::new("s1")),
            sequence: 7,
            recorded_at: None,
            event: Some(Event {
                agent_id: Some(AgentId::new("agent-1")),
                occurred_at: None,
                idempotency_key: String::new(),
                kind,
            }),
        }
    }

    #[test]
    fn json_session_output_parses_and_carries_every_field_a_script_needs() {
        let rendered = session(&summary(), Format::Json);
        let parsed: serde_json::Value =
            serde_json::from_str(&rendered).expect("--json must emit valid JSON");

        assert_eq!(parsed["session_id"], "s1");
        assert_eq!(parsed["state"], "active");
        assert_eq!(parsed["head_sequence"], 12);
        assert_eq!(parsed["agent_count"], 3);
        assert_eq!(
            parsed["name"], "ci-fix",
            "JSON carries both id and name so scripts never re-resolve (ADR-0008)"
        );
    }

    /// A multi-word title's JSON name is its slug — the handle every verb
    /// resolves — not the raw title, which rides in its own field.
    #[test]
    fn the_json_name_is_the_slugified_title() {
        let mut titled = summary();
        titled.title = "Fix Auth Timeout".to_owned();
        let parsed: serde_json::Value =
            serde_json::from_str(&session(&titled, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["name"], "fix-auth-timeout");
        assert_eq!(parsed["title"], "Fix Auth Timeout");
    }

    fn listed(id: &str, title: &str, agents: u32, last_activity_ms: i64) -> SessionSummary {
        SessionSummary {
            session_id: Some(SessionId::new(id)),
            title: title.to_owned(),
            state: SessionLifecycleState::Active as i32,
            agent_count: agents,
            last_activity_at: Some(horde_proto::prost_types::Timestamp {
                seconds: last_activity_ms.div_euclid(1_000),
                nanos: i32::try_from(last_activity_ms.rem_euclid(1_000)).unwrap_or(0) * 1_000_000,
            }),
            ..SessionSummary::default()
        }
    }

    /// The one row format (ADR-0008): name, short id, status, agent count,
    /// last-activity age — with the short id an actual unique prefix, and
    /// the untitled session shown honestly rather than blank.
    #[test]
    fn session_rows_carry_name_short_id_status_agents_and_age() {
        let now = 3_600_000_i64;
        let rows = super::session_rows(
            &[
                listed("session-a1f0aa-0", "Fix Auth Timeout", 3, now - 90_000),
                listed("session-b2e1bb-0", "brisk-warband", 1, now - 5_000),
                listed("session-b2e1bb-1", "", 2, now - 7_200_000),
            ],
            now,
        );
        assert_eq!(rows.len(), 3);
        assert!(
            rows[0].contains("fix-auth-timeout"),
            "the name is the slug: {}",
            rows[0]
        );
        assert!(
            rows[0].contains("session-a1f0") && !rows[0].contains("session-a1f0aa-0"),
            "the short id is a shortened unique prefix: {}",
            rows[0]
        );
        assert!(rows[0].contains("active"));
        assert!(rows[0].contains("agents=3"));
        assert!(rows[0].contains("1m"), "90s ago is 1m: {}", rows[0]);
        assert!(rows[1].contains("5s"), "{}", rows[1]);
        assert!(
            rows[2].contains("session-b2e1bb-1"),
            "an id that only differs at its tail keeps enough to stay unique: {}",
            rows[2]
        );
        assert!(
            rows[2].starts_with("-"),
            "untitled shows a dash: {}",
            rows[2]
        );
    }

    /// The age vocabulary, one boundary per unit — a list is scanned, not
    /// computed with.
    #[test]
    fn ages_use_the_coarsest_nonzero_unit() {
        let at = |ms: i64| horde_proto::prost_types::Timestamp {
            seconds: ms / 1_000,
            nanos: 0,
        };
        assert_eq!(super::age(59_000, &at(0)), "59s");
        assert_eq!(super::age(60_000, &at(0)), "1m");
        assert_eq!(super::age(3_599_000, &at(0)), "59m");
        assert_eq!(super::age(3_600_000, &at(0)), "1h");
        assert_eq!(super::age(86_400_000, &at(0)), "1d");
        assert_eq!(
            super::age(0, &at(5_000)),
            "0s",
            "clock skew must not print a negative age"
        );

        // Sub-second nanos participate as whole milliseconds — divided into
        // the stamp, not multiplied through it and not reduced modulo it.
        let with_nanos =
            |seconds: i64, nanos: i32| horde_proto::prost_types::Timestamp { seconds, nanos };
        assert_eq!(
            super::age(60_500, &with_nanos(0, 500_000_000)),
            "1m",
            "half a second of nanos is 500ms of stamp"
        );
        assert_eq!(
            super::age(60_998, &with_nanos(0, 999_000_000)),
            "59s",
            "999ms of nanos holds the age just under the minute"
        );
    }

    /// The human list's wall clock is a real clock: after this test was
    /// written, never before it — a frozen or negative clock would render
    /// every session as decades old or freshly born.
    #[test]
    fn the_wall_clock_reads_as_the_present() {
        // 2024-01-01T00:00:00Z, safely before any run of this suite.
        assert!(super::now_unix_ms() > 1_704_067_200_000);
    }

    /// The short id never lies: an id that is a full prefix of a sibling
    /// stays whole, because a shorter form would resolve to the wrong one.
    #[test]
    fn a_short_id_that_would_be_anothers_prefix_stays_full() {
        let ids = vec!["session-key-1".to_owned(), "session-key-12".to_owned()];
        assert_eq!(super::short_id("session-key-1", &ids), "session-key-1");
        assert_eq!(super::short_id("session-key-12", &ids), "session-key-12");

        let lone = vec!["session-18f3a2b4c5d6-0".to_owned()];
        assert_eq!(
            super::short_id("session-18f3a2b4c5d6-0", &lone),
            "session-18f3",
            "the floor keeps the short id past the shared session- stem"
        );
    }

    #[test]
    fn human_session_output_leads_with_the_id_so_it_can_be_copied() {
        let rendered = session(&summary(), Format::Human);
        assert!(rendered.starts_with("s1"), "got {rendered:?}");
        assert!(rendered.contains("active"));
    }

    /// An empty list and a swallowed error look identical if the empty case
    /// prints nothing.
    #[test]
    fn an_empty_session_list_says_so_rather_than_printing_nothing() {
        assert_eq!(sessions(&[], Format::Human), "no sessions");

        let parsed: serde_json::Value =
            serde_json::from_str(&sessions(&[], Format::Json)).expect("valid JSON");
        assert_eq!(parsed["sessions"].as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn an_event_renders_its_kind_and_sequence() {
        let chunk = envelope(Some(Kind::AgentOutputChunk(AgentOutputChunk {
            channel: AgentOutputChannel::Assistant as i32,
            text: "hello".to_owned(),
            turn_id: "t1".to_owned(),
            chunk_index: 0,
        })));

        let human = event(&chunk, Format::Human);
        assert!(human.contains('7') && human.contains("agent_output_chunk"));
        assert!(human.contains("hello"), "the text is the point: {human:?}");

        let parsed: serde_json::Value =
            serde_json::from_str(&event(&chunk, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["sequence"], 7);
        assert_eq!(parsed["kind"], "agent_output_chunk");
        assert_eq!(parsed["detail"], "hello");
    }

    /// A result's outcome is machine-readable (W7b finding 4): a script
    /// following a session must be able to branch on status and read the
    /// payload's refs without parsing prose.
    #[test]
    fn a_result_reported_carries_status_summary_and_refs_in_json() {
        let reported = envelope(Some(Kind::ResultReported(
            horde_proto::v1::ResultReported {
                result: Some(horde_proto::v1::AgentResult {
                    assignment_id: None,
                    status: horde_proto::v1::ResultStatus::Partial as i32,
                    summary: "got halfway".to_owned(),
                    payload: Some(horde_proto::v1::Payload {
                        git_refs: vec![horde_proto::v1::GitRef {
                            repo_name: "atlas".to_owned(),
                            commit_sha: "abc123".to_owned(),
                            branch: "horde/agent-1".to_owned(),
                        }],
                        attachments: Vec::new(),
                    }),
                }),
            },
        )));

        let parsed: serde_json::Value =
            serde_json::from_str(&event(&reported, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["detail"]["status"], "partial");
        assert_eq!(parsed["detail"]["summary"], "got halfway");
        assert_eq!(parsed["detail"]["git_refs"][0]["repo"], "atlas");
        assert_eq!(parsed["detail"]["git_refs"][0]["commit"], "abc123");

        // An unreadable status must not become a success a script acts on.
        let unreadable = envelope(Some(Kind::ResultReported(
            horde_proto::v1::ResultReported {
                result: Some(horde_proto::v1::AgentResult {
                    status: 9_999,
                    ..horde_proto::v1::AgentResult::default()
                }),
            },
        )));
        let parsed: serde_json::Value =
            serde_json::from_str(&event(&unreadable, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["detail"]["status"], "unknown");
    }

    fn sample_definition() -> horde_proto::v1::WorkspaceDefinition {
        horde_proto::v1::WorkspaceDefinition {
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-1")),
            name: "atlas-ws".to_owned(),
            etag: "3".to_owned(),
            repos: vec![horde_proto::v1::WorkspaceRepo {
                name: "atlas".to_owned(),
                remote_url: "https://git.example/atlas.git".to_owned(),
                default_branch: "main".to_owned(),
                layout_path: "src".to_owned(),
            }],
            ..horde_proto::v1::WorkspaceDefinition::default()
        }
    }

    /// The workspace renders, both formats: what `create`/`list`/`get`
    /// print is the operator's whole view of a definition, and the etag in
    /// particular is the token every later edit must quote.
    #[test]
    fn workspace_renders_carry_the_id_repos_and_etag() {
        let definition = sample_definition();

        let parsed: serde_json::Value =
            serde_json::from_str(&workspace(&definition, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["workspace_definition_id"], "ws-1");
        assert_eq!(parsed["etag"], "3");
        assert_eq!(parsed["repos"][0]["name"], "atlas");
        assert_eq!(parsed["repos"][0]["layout_path"], "src");

        let human = workspace(&definition, Format::Human);
        assert!(
            human.contains("ws-1") && human.contains("etag 3"),
            "{human}"
        );

        let listed: serde_json::Value =
            serde_json::from_str(&workspaces(std::slice::from_ref(&definition), Format::Json))
                .expect("valid JSON");
        assert_eq!(listed["workspaces"][0]["name"], "atlas-ws");
        assert_eq!(
            workspaces(&[], Format::Human),
            "no workspaces defined",
            "an empty list says so rather than printing nothing"
        );
        assert!(workspaces(&[definition], Format::Human).contains("atlas-ws"));

        let deleted: serde_json::Value =
            serde_json::from_str(&workspace_deleted("ws-1", Format::Json)).expect("valid JSON");
        assert_eq!(deleted["deleted"], "ws-1");
        assert!(workspace_deleted("ws-1", Format::Human).contains("ws-1"));
    }

    /// `get` shows the definition and its known instances, with the state
    /// vocabulary spelled for humans in both formats.
    #[test]
    fn a_workspace_get_renders_its_instances_and_their_states() {
        let response = horde_proto::v1::GetWorkspaceDefinitionResponse {
            definition: Some(sample_definition()),
            instances: vec![horde_proto::v1::WorkspaceInstanceReport {
                worker_id: Some(horde_proto::v1::WorkerId::new("w-1")),
                state: horde_proto::v1::WorkspaceInstanceState::Ready as i32,
                root_path: "/w/ws-1".to_owned(),
                detail: String::new(),
                ..horde_proto::v1::WorkspaceInstanceReport::default()
            }],
        };

        let parsed: serde_json::Value =
            serde_json::from_str(&workspace_with_instances(&response, Format::Json))
                .expect("valid JSON");
        assert_eq!(parsed["name"], "atlas-ws");
        assert_eq!(parsed["instances"][0]["worker_id"], "w-1");
        assert_eq!(parsed["instances"][0]["state"], "ready");

        let human = workspace_with_instances(&response, Format::Human);
        assert!(
            human.contains("instance on w-1: ready"),
            "the instances render under the definition: {human}"
        );
    }

    /// The prewarm render, all four corners: with and without an acceptance
    /// report, in both formats.
    #[test]
    fn a_prewarm_renders_its_acceptance_or_a_bare_accepted() {
        let report = horde_proto::v1::WorkspaceInstanceReport {
            worker_id: Some(horde_proto::v1::WorkerId::new("w-1")),
            state: horde_proto::v1::WorkspaceInstanceState::Materializing as i32,
            ..horde_proto::v1::WorkspaceInstanceReport::default()
        };

        let parsed: serde_json::Value =
            serde_json::from_str(&prewarmed(Some(&report), Format::Json)).expect("valid JSON");
        assert_eq!(parsed["state"], "materializing");
        assert!(prewarmed(Some(&report), Format::Human).contains("w-1"));

        let bare: serde_json::Value =
            serde_json::from_str(&prewarmed(None, Format::Json)).expect("valid JSON");
        assert_eq!(bare["state"], "accepted");
        assert_eq!(prewarmed(None, Format::Human), "prewarm accepted");
    }

    /// The worker listing, and the two state tables' full vocabularies — a
    /// state that rendered under a neighbour's name would send an operator
    /// chasing the wrong worker.
    #[test]
    fn workers_render_with_their_connection_state() {
        let worker = horde_proto::v1::WorkerSummary {
            worker_id: Some(horde_proto::v1::WorkerId::new("w-1")),
            name: "w-1".to_owned(),
            state: horde_proto::v1::WorkerConnectionState::Online as i32,
            ..horde_proto::v1::WorkerSummary::default()
        };
        let parsed: serde_json::Value =
            serde_json::from_str(&workers(std::slice::from_ref(&worker), Format::Json))
                .expect("valid JSON");
        assert_eq!(parsed["workers"][0]["state"], "online");
        assert!(workers(&[worker], Format::Human).contains("w-1  online"));
        assert_eq!(workers(&[], Format::Human), "no workers connected");

        for (state, name) in [
            (
                horde_proto::v1::WorkspaceInstanceState::Materializing as i32,
                "materializing",
            ),
            (
                horde_proto::v1::WorkspaceInstanceState::Ready as i32,
                "ready",
            ),
            (
                horde_proto::v1::WorkspaceInstanceState::Degraded as i32,
                "degraded",
            ),
            (
                horde_proto::v1::WorkspaceInstanceState::Missing as i32,
                "missing",
            ),
            (0, "unspecified"),
            (9_999, "unspecified"),
        ] {
            assert_eq!(instance_state(state), name);
        }
        for (state, name) in [
            (
                horde_proto::v1::WorkerConnectionState::Online as i32,
                "online",
            ),
            (
                horde_proto::v1::WorkerConnectionState::Offline as i32,
                "offline",
            ),
            (0, "unspecified"),
            (9_999, "unspecified"),
        ] {
            assert_eq!(worker_state(state), name);
        }
    }

    #[test]
    fn a_permission_decided_carries_outcome_reason_and_narrowing_in_json() {
        // Built from a collected map rather than inserts: one field is all
        // the render needs, and the terser form keeps this fixture from
        // twinning the fold suite's.
        let narrowed = horde_proto::prost_types::Struct {
            fields: [(
                "path".to_owned(),
                horde_proto::prost_types::Value {
                    kind: Some(horde_proto::prost_types::value::Kind::StringValue(
                        "/tmp/safe".to_owned(),
                    )),
                },
            )]
            .into_iter()
            .collect(),
        };
        let decided = envelope(Some(Kind::PermissionDecided(
            horde_proto::v1::PermissionDecided {
                decision: Some(horde_proto::v1::PermissionDecision {
                    request_id: Some(horde_proto::v1::PermissionRequestId::new("call-1")),
                    outcome: horde_proto::v1::PermissionOutcome::Allow as i32,
                    reason: "clamped to /tmp".to_owned(),
                    remember_for_session: true,
                    updated_input: Some(narrowed),
                    ..horde_proto::v1::PermissionDecision::default()
                }),
            },
        )));

        let parsed: serde_json::Value =
            serde_json::from_str(&event(&decided, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["detail"]["outcome"], "allowed");
        assert_eq!(parsed["detail"]["reason"], "clamped to /tmp");
        assert_eq!(parsed["detail"]["remembered"], true);
        assert_eq!(parsed["detail"]["updated_input"]["path"], "/tmp/safe");

        let denied = envelope(Some(Kind::PermissionDecided(
            horde_proto::v1::PermissionDecided {
                decision: Some(horde_proto::v1::PermissionDecision {
                    outcome: horde_proto::v1::PermissionOutcome::Deny as i32,
                    ..horde_proto::v1::PermissionDecision::default()
                }),
            },
        )));
        let parsed: serde_json::Value =
            serde_json::from_str(&event(&denied, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["detail"]["outcome"], "denied");
        assert_eq!(
            parsed["detail"]["updated_input"],
            serde_json::Value::Null,
            "no narrowing renders as null, not as an empty object"
        );
    }

    /// Hiding an event this build cannot pretty-print would make the printed
    /// sequence look gapped, which is worse than an ugly line.
    #[test]
    fn an_event_with_no_kind_still_prints_its_position() {
        let unknown = envelope(None);

        let human = event(&unknown, Format::Human);
        assert!(human.contains('7'));
        assert!(human.contains("unknown"));

        let parsed: serde_json::Value =
            serde_json::from_str(&event(&unknown, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["kind"], "unknown");
        assert_eq!(parsed["sequence"], 7);
    }

    /// `kind_name` is a 13-arm match, so a partially covered one scores badly
    /// however simple each arm is. Naming every kind here also makes an added
    /// event kind fail this test rather than silently print as `unknown`.
    #[test]
    fn every_event_kind_has_a_wire_name() {
        use horde_proto::v1::{
            AgentLifecycleChanged, AgentSpawned, AssignmentSent, EnrollmentCodeMinted,
            EnrollmentRevoked, IdentityEnrolled, NoteSent, PermissionDecided, PermissionRequested,
            ResultReported, SessionLifecycleChanged, TurnInterrupted, UsageRecorded,
            WorkspaceDefinitionChanged, WorkspaceInstanceChanged, WorkspacePrewarmRequested,
        };

        let cases: Vec<(Kind, &str)> = vec![
            (
                Kind::AgentOutputChunk(AgentOutputChunk::default()),
                "agent_output_chunk",
            ),
            (Kind::AgentSpawned(AgentSpawned::default()), "agent_spawned"),
            (
                Kind::AssignmentSent(AssignmentSent::default()),
                "assignment_sent",
            ),
            (Kind::NoteSent(NoteSent::default()), "note_sent"),
            (
                Kind::ResultReported(ResultReported::default()),
                "result_reported",
            ),
            (
                Kind::PermissionRequested(PermissionRequested::default()),
                "permission_requested",
            ),
            (
                Kind::PermissionDecided(PermissionDecided::default()),
                "permission_decided",
            ),
            (
                Kind::UsageRecorded(UsageRecorded::default()),
                "usage_recorded",
            ),
            (
                Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged::default()),
                "workspace_instance_changed",
            ),
            (
                Kind::AgentLifecycleChanged(AgentLifecycleChanged::default()),
                "agent_lifecycle_changed",
            ),
            (
                Kind::SessionLifecycleChanged(SessionLifecycleChanged::default()),
                "session_lifecycle_changed",
            ),
            (
                Kind::PlacementChanged(PlacementChanged::default()),
                "placement_changed",
            ),
            (
                Kind::TurnInterrupted(TurnInterrupted::default()),
                "turn_interrupted",
            ),
            (
                Kind::WorkspaceDefinitionChanged(WorkspaceDefinitionChanged::default()),
                "workspace_definition_changed",
            ),
            (
                Kind::WorkspacePrewarmRequested(WorkspacePrewarmRequested::default()),
                "workspace_prewarm_requested",
            ),
            (
                Kind::EnrollmentCodeMinted(EnrollmentCodeMinted::default()),
                "enrollment_code_minted",
            ),
            (
                Kind::IdentityEnrolled(IdentityEnrolled::default()),
                "identity_enrolled",
            ),
            (
                Kind::EnrollmentRevoked(EnrollmentRevoked::default()),
                "enrollment_revoked",
            ),
            (
                Kind::McpServerConfigChanged(horde_proto::v1::McpServerConfigChanged::default()),
                "mcp_server_config_changed",
            ),
        ];

        for (kind, expected) in cases {
            let rendered = event(&envelope(Some(kind)), Format::Human);
            assert!(
                rendered.contains(expected),
                "expected {expected:?} in {rendered:?}"
            );
        }
    }

    #[test]
    fn the_handshake_names_the_server_version_in_both_formats() {
        let server = horde_proto::v1::ServerHello {
            semver: "0.1.0".to_owned(),
            capability_flags: vec!["payload.attachments".to_owned()],
            min_supported_semver: "0.1.0".to_owned(),
            server_time: None,
            principal: String::new(),
        };

        let human = hello(&server, Format::Human);
        assert!(human.contains("0.1.0") && human.contains("payload.attachments"));

        let parsed: serde_json::Value =
            serde_json::from_str(&hello(&server, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["semver"], "0.1.0");
    }

    /// A server advertising nothing must not render an empty field that reads
    /// like a truncated line.
    #[test]
    fn a_server_with_no_capability_flags_says_none() {
        let server = horde_proto::v1::ServerHello {
            semver: "0.1.0".to_owned(),
            min_supported_semver: "0.1.0".to_owned(),
            ..horde_proto::v1::ServerHello::default()
        };
        assert!(hello(&server, Format::Human).contains("none"));
    }

    #[test]
    fn a_created_session_carries_the_root_agent_the_next_command_needs() {
        let root = AgentId::new("root-1");

        let human = created(&summary(), &root, Format::Human);
        assert!(human.contains("root-1"), "got {human:?}");

        let parsed: serde_json::Value =
            serde_json::from_str(&created(&summary(), &root, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["root_agent_id"], "root-1");
        assert_eq!(parsed["session_id"], "s1", "the session fields survive too");
    }

    #[test]
    fn an_assignment_reports_its_id_and_a_note_does_not() {
        let assignment = horde_proto::v1::SendMessageResponse {
            assignment_id: Some(horde_proto::v1::AssignmentId::new("a-1")),
            session_id: Some(SessionId::new("s1")),
            sequence: 4,
        };
        let human = sent(&assignment, Format::Human);
        assert!(
            human.contains('4') && human.contains("a-1"),
            "got {human:?}"
        );

        // Only assignments owe a result, so only they get an id.
        let note = horde_proto::v1::SendMessageResponse {
            assignment_id: None,
            session_id: Some(SessionId::new("s1")),
            sequence: 5,
        };
        let human = sent(&note, Format::Human);
        assert!(
            human.contains('5') && !human.contains("assignment"),
            "got {human:?}"
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&sent(&note, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["assignment_id"], serde_json::Value::Null);
    }

    /// A spawn response for `agent-1`, landed however the caller says.
    fn spawn_response(
        state: PlacementState,
        worker: Option<&str>,
        reason: &str,
    ) -> horde_proto::v1::SpawnAgentResponse {
        horde_proto::v1::SpawnAgentResponse {
            agent_id: Some(AgentId::new("agent-1")),
            placement: Some(Placement {
                agent_id: Some(AgentId::new("agent-1")),
                state: state as i32,
                worker_id: worker.map(horde_proto::v1::WorkerId::new),
                reason: reason.to_owned(),
            }),
        }
    }

    #[test]
    fn a_placed_spawn_names_its_worker() {
        let response = spawn_response(PlacementState::Placed, Some("worker-a"), "");

        let human = spawned(&response, Format::Human);
        assert!(
            human.contains("agent-1") && human.contains("placed") && human.contains("worker-a")
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&spawned(&response, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["placement"], "placed");
        assert_eq!(parsed["worker_id"], "worker-a");
    }

    /// A queued spawn without its reason is not actionable.
    #[test]
    fn a_queued_spawn_keeps_its_reason_in_both_formats() {
        let response = spawn_response(PlacementState::Queued, None, "no worker is connected");

        let human = spawned(&response, Format::Human);
        assert!(human.contains("queued") && human.contains("no worker is connected"));

        let parsed: serde_json::Value =
            serde_json::from_str(&spawned(&response, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["reason"], "no worker is connected");
        assert_eq!(parsed["worker_id"], serde_json::Value::Null);
    }

    #[test]
    fn a_spawn_with_no_placement_still_renders() {
        let response = horde_proto::v1::SpawnAgentResponse {
            agent_id: None,
            placement: None,
        };
        assert!(spawned(&response, Format::Human).contains("unspecified"));
    }

    #[test]
    fn an_acknowledgement_carries_the_position_a_caller_syncs_from() {
        assert_eq!(
            sequence("interrupted", 9, Format::Human),
            "interrupted at 9"
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&sequence("denied", 9, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["result"], "denied");
        assert_eq!(parsed["sequence"], 9);
    }

    #[test]
    fn every_placement_and_session_state_has_a_name() {
        for (state, expected) in [
            (PlacementState::Placed, "placed"),
            (PlacementState::Queued, "queued"),
            (PlacementState::Expired, "expired"),
            (PlacementState::Cancelled, "cancelled"),
            (PlacementState::Unspecified, "unspecified"),
        ] {
            assert_eq!(super::placement_state(state as i32), expected);
        }
        assert_eq!(super::placement_state(9999), "unspecified");

        assert_eq!(
            super::session_state(SessionLifecycleState::Archived as i32),
            "archived"
        );
        assert_eq!(super::session_state(9999), "unspecified");
    }

    #[test]
    fn a_session_with_no_title_does_not_render_a_trailing_gap() {
        let untitled = SessionSummary {
            title: String::new(),
            ..summary()
        };
        let human = session(&untitled, Format::Human);
        assert_eq!(human, human.trim_end(), "got {human:?}");
    }

    /// A rendered escalation carries the id `horde approve` needs and the
    /// tool the approver is judging — a follower that cannot learn either
    /// from the stream cannot answer at all (the deployed-stack hub e2e
    /// approves by exactly this field).
    #[test]
    fn a_permission_request_renders_its_id_and_tool() {
        use horde_proto::v1::{
            PermissionRequest, PermissionRequestId, PermissionRequested, ProposedToolCall,
        };

        let requested = envelope(Some(Kind::PermissionRequested(PermissionRequested {
            request: Some(PermissionRequest {
                request_id: Some(PermissionRequestId::new("call-9")),
                agent_id: None,
                tool_call: Some(ProposedToolCall {
                    tool_name: "linear__create".to_owned(),
                    input: None,
                    rendered: String::new(),
                }),
                tree_path: Vec::new(),
                permission_mode: String::new(),
                escalated_from: 0,
                expires_at: None,
                forward: None,
            }),
        })));
        let parsed: serde_json::Value =
            serde_json::from_str(&event(&requested, Format::Json)).expect("json");
        assert_eq!(parsed["detail"]["request_id"], "call-9");
        assert_eq!(parsed["detail"]["tool"], "linear__create");

        let bare = envelope(Some(Kind::PermissionRequested(PermissionRequested {
            request: None,
        })));
        let parsed: serde_json::Value =
            serde_json::from_str(&event(&bare, Format::Json)).expect("json");
        assert_eq!(
            parsed["detail"], "",
            "a malformed request renders empty rather than panicking"
        );
    }

    /// Every arm of `detail` must be pinned by an assertion on the text it
    /// extracts. Mutation testing deletes arms one at a time, and a fall-through
    /// to the empty default is invisible unless the text itself is checked —
    /// the AssignmentSent and NoteSent arms survived until this existed.
    #[test]
    fn detail_extracts_the_text_of_every_kind_that_carries_one() {
        use horde_proto::v1::{Assignment, AssignmentSent, Note, NoteSent, TurnInterrupted};

        let assignment = envelope(Some(Kind::AssignmentSent(AssignmentSent {
            from: None,
            assignment: Some(Assignment {
                assignment_id: None,
                text: "fix the build".to_owned(),
                payload: None,
            }),
            delivery: 0,
        })));
        assert!(
            event(&assignment, Format::Human).contains("fix the build"),
            "an assignment's text is the only part a reader cares about"
        );

        let note = envelope(Some(Kind::NoteSent(NoteSent {
            from: None,
            note: Some(Note {
                text: "just so you know".to_owned(),
                payload: None,
            }),
            delivery: 0,
        })));
        assert!(event(&note, Format::Human).contains("just so you know"));

        let interrupted = envelope(Some(Kind::TurnInterrupted(TurnInterrupted {
            reason: "changed my mind".to_owned(),
            ..TurnInterrupted::default()
        })));
        assert!(event(&interrupted, Format::Human).contains("changed my mind"));
    }

    /// A message whose body failed to arrive must render as empty rather than
    /// panicking or inventing text.
    #[test]
    fn detail_tolerates_a_message_with_no_body() {
        use horde_proto::v1::{AssignmentSent, NoteSent};

        for kind in [
            Kind::AssignmentSent(AssignmentSent::default()),
            Kind::NoteSent(NoteSent::default()),
        ] {
            let parsed: serde_json::Value =
                serde_json::from_str(&event(&envelope(Some(kind)), Format::Json))
                    .expect("valid JSON");
            assert_eq!(parsed["detail"], "");
        }
    }

    #[test]
    fn a_queued_placement_shows_the_reason_the_spawner_needs() {
        let queued = envelope(Some(Kind::PlacementChanged(PlacementChanged {
            adapter_capabilities: Vec::new(),
            placement: Some(Placement {
                agent_id: Some(AgentId::new("agent-1")),
                state: PlacementState::Queued as i32,
                worker_id: None,
                reason: "no worker is connected".to_owned(),
            }),
        })));

        let human = event(&queued, Format::Human);
        assert!(
            human.contains("queued") && human.contains("no worker is connected"),
            "a queued spawn is useless without its reason: {human:?}"
        );
    }
}

#[cfg(test)]
mod enrollment_render_tests {
    use super::{Format, joined, minted, revoked};

    fn response(code: &str) -> horde_proto::v1::MintEnrollmentCodeResponse {
        horde_proto::v1::MintEnrollmentCodeResponse {
            code: code.to_owned(),
            server_cert_fingerprint: "ab".repeat(32),
            expires_at: Some(horde_proto::prost_types::Timestamp {
                seconds: 1_755,
                nanos: 500_000_000,
            }),
            enrollment_port: 7778,
        }
    }

    #[test]
    fn a_minted_code_prints_the_token_and_its_deadline_in_both_formats() {
        let token = horde_identity::JoinToken::new(
            "localhost:7778",
            horde_identity::JoinCode {
                secret: "00ff".to_owned(),
                server_fingerprint: "a".repeat(64),
            },
        );
        let human = minted(&response("s.fp"), Some(&token), Format::Human);
        assert_eq!(human, format!("token {token}\nexpires-at-unix-ms 1755500"));

        let json: serde_json::Value =
            serde_json::from_str(&minted(&response("s.fp"), Some(&token), Format::Json))
                .expect("json mode emits json");
        assert_eq!(json["token"], token.to_string());
        assert_eq!(json["code"], "s.fp");
        assert_eq!(json["expires_at_unix_ms"], 1_755_500);
        assert_eq!(json["server_cert_fingerprint"], "ab".repeat(32));

        // A portless mint (an older server): the bare code is all there
        // honestly is — a `token` line here would name a listener nobody
        // declared.
        let bare = minted(&response("s.fp"), None, Format::Human);
        assert_eq!(bare, "code s.fp\nexpires-at-unix-ms 1755500");
        let bare: serde_json::Value =
            serde_json::from_str(&minted(&response("s.fp"), None, Format::Json))
                .expect("json mode emits json");
        assert!(bare["token"].is_null());
    }

    #[test]
    fn a_revocation_names_who_and_what_and_admits_a_no_op() {
        assert_eq!(
            revoked("gpu-box", "0a,0b", Format::Human),
            "revoked gpu-box: 0a,0b"
        );
        assert_eq!(
            revoked("gpu-box", "", Format::Human),
            "gpu-box held no active certificate",
            "a no-op revocation must not read as a revocation"
        );

        let json: serde_json::Value =
            serde_json::from_str(&revoked("gpu-box", "0a,0b", Format::Json))
                .expect("json mode emits json");
        assert_eq!(json["revoked"], "gpu-box");
        assert_eq!(json["serials"], serde_json::json!(["0a", "0b"]));
        let empty: serde_json::Value =
            serde_json::from_str(&revoked("g", "", Format::Json)).expect("json mode emits json");
        assert_eq!(empty["serials"], serde_json::json!([]));
    }

    #[test]
    fn a_join_reports_the_principal_and_where_the_identity_lives() {
        let dir = std::path::Path::new("/tmp/id");
        assert_eq!(
            joined("client:laptop", dir, Format::Human),
            "enrolled as client:laptop; identity in /tmp/id"
        );
        let json: serde_json::Value =
            serde_json::from_str(&joined("client:laptop", dir, Format::Json))
                .expect("json mode emits json");
        assert_eq!(json["principal"], "client:laptop");
        assert_eq!(json["identity_dir"], "/tmp/id");
    }
}

#[cfg(test)]
mod blob_render_tests {
    use super::{Format, fetched, stashed};

    /// The address is what a script pipes into the next command, so both
    /// formats must carry it — and the size, which is the only sanity check
    /// a caller has before fetching.
    #[test]
    fn a_stash_reports_the_address_and_size_in_both_formats() {
        let stored = horde_proto::v1::UploadBlobResponse {
            blob_digest: "sha256:abc123".to_owned(),
            size_bytes: 4096,
        };
        assert_eq!(
            stashed(&stored, Format::Human),
            "stashed 4096 bytes at sha256:abc123"
        );
        let json: serde_json::Value =
            serde_json::from_str(&stashed(&stored, Format::Json)).expect("json mode emits json");
        assert_eq!(json["blob_digest"], "sha256:abc123");
        assert_eq!(json["size_bytes"], 4096);
    }

    #[test]
    fn a_fetch_reports_where_the_verified_bytes_landed() {
        let out = std::path::Path::new("/tmp/shot.png");
        assert_eq!(
            fetched("sha256:abc123", out, 512, Format::Human),
            "fetched sha256:abc123 (512 bytes, verified) into /tmp/shot.png"
        );
        let json: serde_json::Value =
            serde_json::from_str(&fetched("sha256:abc123", out, 512, Format::Json))
                .expect("json mode emits json");
        assert_eq!(json["blob_digest"], "sha256:abc123");
        assert_eq!(json["out"], "/tmp/shot.png");
        assert_eq!(json["size_bytes"], 512);
    }
}

#[cfg(test)]
mod usage_tests {
    use horde_proto::v1::{
        AccountHeadroom, AgentId, HeadroomSource, QueryUsageResponse, RateWindow, SessionId,
        TokenUsage, UsageAggregate, UsageGroupKey,
    };

    use super::{Format, UsageGrouping, account_standing, group_label, pair, usage};

    fn tokens(input: u64, output: u64) -> TokenUsage {
        TokenUsage {
            input_tokens: input,
            output_tokens: output,
            ..TokenUsage::default()
        }
    }

    fn account_aggregate() -> UsageAggregate {
        UsageAggregate {
            key: Some(UsageGroupKey {
                provider: "claude".to_owned(),
                provider_account: "personal".to_owned(),
                ..UsageGroupKey::default()
            }),
            tokens: Some(tokens(110, 45)),
            agent_count: 2,
        }
    }

    fn healthy_account() -> AccountHeadroom {
        AccountHeadroom {
            provider: "claude".to_owned(),
            account: "personal".to_owned(),
            window: Some(RateWindow {
                source: HeadroomSource::ProviderReported as i32,
                used: Some(75),
                limit: Some(100),
                ..RateWindow::default()
            }),
            remaining_fraction: Some(0.25),
            exhausted: false,
            tokens: Some(tokens(110, 45)),
            basis: "derived from provider-reported counts".to_owned(),
        }
    }

    #[test]
    fn the_human_answer_keeps_measured_and_derived_apart() {
        let response = QueryUsageResponse {
            aggregates: vec![account_aggregate()],
            accounts: vec![healthy_account()],
        };
        let rendered = usage(&response, UsageGrouping::Account, Format::Human);
        assert_eq!(
            rendered,
            "usage by provider account (measured)\n  \
             claude/personal  in 110  out 45  cache r/w 0/0  reasoning 0  agents 2\n\
             provider accounts\n  \
             claude/personal  25% left (derived from provider-reported counts)  \
             spent in 110  out 45  cache r/w 0/0  reasoning 0"
        );
    }

    #[test]
    fn an_empty_answer_says_so_rather_than_printing_nothing() {
        let empty = QueryUsageResponse::default();
        let rendered = usage(&empty, UsageGrouping::Session, Format::Human);
        assert_eq!(rendered, "usage by session (measured)\n  no usage recorded");
    }

    #[test]
    fn the_json_answer_separates_measured_from_derived() {
        let response = QueryUsageResponse {
            aggregates: vec![account_aggregate()],
            accounts: vec![healthy_account()],
        };
        let parsed: serde_json::Value =
            serde_json::from_str(&usage(&response, UsageGrouping::Account, Format::Json))
                .expect("--json must emit valid JSON");

        assert_eq!(parsed["grouping"], "provider account");
        let row = &parsed["aggregates"][0];
        assert_eq!(row["label"], "claude/personal");
        assert_eq!(row["measured"]["tokens"]["input_tokens"], 110);
        assert_eq!(row["measured"]["agent_count"], 2);

        let account = &parsed["accounts"][0];
        assert_eq!(account["measured"]["window"]["used"], 75);
        assert_eq!(account["measured"]["window"]["source"], "provider-reported");
        assert_eq!(account["measured"]["tokens"]["output_tokens"], 45);
        assert_eq!(account["derived"]["remaining_fraction"], 0.25);
        assert_eq!(account["derived"]["exhausted"], false);
        assert_eq!(
            account["derived"]["basis"],
            "derived from provider-reported counts"
        );
        assert!(
            account["measured"]["remaining_fraction"].is_null(),
            "the derived number must not also appear as a measurement"
        );
    }

    /// Every source the wire can carry keeps its own JSON word — the
    /// measured/derived split depends on a script being able to see *whose*
    /// number a window is, so "horde-metered" collapsing into "unspecified"
    /// would misattribute Horde's own metering to nobody.
    #[test]
    fn every_window_source_keeps_its_own_name_in_json() {
        let with_source = |source: i32| {
            let response = QueryUsageResponse {
                aggregates: Vec::new(),
                accounts: vec![AccountHeadroom {
                    window: Some(RateWindow {
                        source,
                        ..RateWindow::default()
                    }),
                    ..healthy_account()
                }],
            };
            let parsed: serde_json::Value =
                serde_json::from_str(&usage(&response, UsageGrouping::Account, Format::Json))
                    .expect("json");
            parsed["accounts"][0]["measured"]["window"]["source"].clone()
        };

        assert_eq!(
            with_source(HeadroomSource::ProviderReported as i32),
            "provider-reported"
        );
        assert_eq!(
            with_source(HeadroomSource::HordeMetered as i32),
            "horde-metered"
        );
        assert_eq!(
            with_source(HeadroomSource::Unspecified as i32),
            "unspecified"
        );
        assert_eq!(with_source(887), "unspecified");
    }

    #[test]
    fn an_account_with_no_window_renders_null_rather_than_a_fabricated_one() {
        let response = QueryUsageResponse {
            aggregates: Vec::new(),
            accounts: vec![AccountHeadroom {
                provider: "codex".to_owned(),
                account: "team".to_owned(),
                window: None,
                remaining_fraction: None,
                exhausted: false,
                tokens: Some(tokens(1, 0)),
                basis: "no rate window journaled".to_owned(),
            }],
        };
        let parsed: serde_json::Value =
            serde_json::from_str(&usage(&response, UsageGrouping::Account, Format::Json))
                .expect("json");
        assert!(parsed["accounts"][0]["measured"]["window"].is_null());
        assert!(parsed["accounts"][0]["derived"]["remaining_fraction"].is_null());
    }

    /// The three standings a derived account row can have. EXHAUSTED wins
    /// over any fraction, because it also folds in the reset clock.
    #[test]
    fn account_standing_words_cover_all_three_states() {
        assert_eq!(account_standing(true, Some(0.0)), "EXHAUSTED");
        assert_eq!(account_standing(true, None), "EXHAUSTED");
        assert_eq!(account_standing(false, Some(0.25)), "25% left");
        assert_eq!(account_standing(false, Some(1.0)), "100% left");
        assert_eq!(account_standing(false, None), "headroom unknown");
    }

    #[test]
    fn labels_follow_the_grouping_not_the_populated_fields() {
        let key = UsageGroupKey {
            session_id: Some(SessionId::new("s1")),
            agent_id: Some(AgentId::new("agent-9")),
            tree_path: vec!["ci-fix".to_owned(), "reviewer".to_owned()],
            provider: "claude".to_owned(),
            provider_account: "personal".to_owned(),
            model: "sonnet".to_owned(),
        };
        assert_eq!(group_label(UsageGrouping::Agent, &key), "s1 agent-9");
        assert_eq!(group_label(UsageGrouping::Session, &key), "s1");
        assert_eq!(group_label(UsageGrouping::Tree, &key), "ci-fix > reviewer");
        assert_eq!(group_label(UsageGrouping::Account, &key), "claude/personal");
        assert_eq!(group_label(UsageGrouping::Model, &key), "claude/sonnet");

        let empty = UsageGroupKey::default();
        assert_eq!(group_label(UsageGrouping::Agent, &empty), "- -");
        assert_eq!(group_label(UsageGrouping::Session, &empty), "-");
        assert_eq!(group_label(UsageGrouping::Tree, &empty), "(no tree path)");
        assert_eq!(
            group_label(UsageGrouping::Account, &empty),
            "(unattributed)"
        );
        assert_eq!(group_label(UsageGrouping::Model, &empty), "(unattributed)");
    }

    /// The empty halves are named, not rendered as bare slashes — and the
    /// word depends on which half of which grouping is empty.
    #[test]
    fn the_pair_label_names_its_empties() {
        assert_eq!(pair("claude", "personal", "(none)"), "claude/personal");
        assert_eq!(pair("codex", "", "(default)"), "codex/(default)");
        assert_eq!(pair("codex", "", "(none)"), "codex/(none)");
        assert_eq!(pair("", "", "(none)"), "(unattributed)");
        assert_eq!(
            pair("", "orphan", "(none)"),
            "/orphan",
            "a name with no provider still shows the name"
        );
    }

    #[test]
    fn every_grouping_has_its_heading_word() {
        assert_eq!(UsageGrouping::Agent.word(), "agent");
        assert_eq!(UsageGrouping::Session.word(), "session");
        assert_eq!(UsageGrouping::Tree.word(), "tree path");
        assert_eq!(UsageGrouping::Account.word(), "provider account");
        assert_eq!(UsageGrouping::Model.word(), "model");
    }

    // -- forwards (ADR-0006) -------------------------------------------------

    fn forward_status(
        name: &str,
        direction: horde_proto::v1::ForwardDirection,
        backer: Option<&str>,
        worker_reachable: bool,
        backed: bool,
    ) -> horde_proto::v1::ForwardStatus {
        horde_proto::v1::ForwardStatus {
            intent: Some(horde_proto::v1::ForwardIntent {
                forward_id: Some(horde_proto::v1::ForwardId::new(format!("fwd-{name}"))),
                session_id: Some(horde_proto::v1::SessionId::new("s1")),
                agent_id: Some(horde_proto::v1::AgentId::new("a1")),
                spec: Some(horde_proto::v1::ForwardSpec {
                    direction: direction as i32,
                    name: name.to_owned(),
                    worker_port: 5173,
                    client_port: 5432,
                }),
                backer_client_id: backer.map(horde_proto::v1::ClientId::new),
            }),
            worker_reachable,
            backed,
        }
    }

    /// The three row shapes an operator reads: a reachable expose, a backed
    /// inject, and an unbacked inject naming the detached backer — whose
    /// return is the remedy, which is why the name must survive the state.
    #[test]
    fn the_forward_list_says_reachability_and_backing_in_words() {
        use horde_proto::v1::ForwardDirection;

        let rendered = super::forwards(
            &[
                forward_status("vite", ForwardDirection::Expose, None, true, false),
                forward_status("db", ForwardDirection::Inject, Some("laptop"), true, true),
                forward_status(
                    "cache",
                    ForwardDirection::Inject,
                    Some("desk"),
                    false,
                    false,
                ),
            ],
            Format::Human,
        );
        assert!(rendered.contains("vite  expose 5173"), "{rendered}");
        assert!(rendered.contains("worker ok"), "{rendered}");
        assert!(rendered.contains("backed by laptop"), "{rendered}");
        assert!(rendered.contains("worker unreachable"), "{rendered}");
        assert!(rendered.contains("UNBACKED (desk detached)"), "{rendered}");
        assert!(
            !rendered.contains("vite")
                || !rendered.lines().next().unwrap_or("").contains("UNBACKED"),
            "an expose has no backing state to claim: {rendered}"
        );

        assert_eq!(super::forwards(&[], Format::Human), "no live forwards");
    }

    #[test]
    fn the_forward_list_renders_json_rows_scripts_can_read() {
        use horde_proto::v1::ForwardDirection;

        let rendered = super::forwards(
            &[forward_status(
                "db",
                ForwardDirection::Inject,
                Some("laptop"),
                true,
                false,
            )],
            Format::Json,
        );
        let rows: serde_json::Value = serde_json::from_str(&rendered).expect("one JSON value");
        assert_eq!(rows[0]["name"], "db");
        assert_eq!(rows[0]["direction"], "inject");
        assert_eq!(rows[0]["worker_port"], 5173);
        assert_eq!(rows[0]["client_port"], 5432);
        assert_eq!(rows[0]["backer"], "laptop");
        assert_eq!(rows[0]["worker_reachable"], true);
        assert_eq!(rows[0]["backed"], false);
        assert_eq!(rows[0]["session_id"], "s1");
    }

    /// The mapping line is the whole point of the fallback: it must say when
    /// the address is not the one asked for.
    #[test]
    fn a_realized_expose_prints_its_mapping_and_names_a_fallback() {
        let addr: std::net::SocketAddr = "127.0.0.1:49152".parse().expect("an address");

        let exact = super::forward_realized("vite", "fwd-1", addr, false, Format::Human);
        assert!(exact.contains("vite → 127.0.0.1:49152"), "{exact}");
        assert!(!exact.contains("fallback"), "{exact}");

        let fallback = super::forward_realized("vite", "fwd-1", addr, true, Format::Human);
        assert!(fallback.contains("ephemeral fallback"), "{fallback}");

        let json: serde_json::Value = serde_json::from_str(&super::forward_realized(
            "vite",
            "fwd-1",
            addr,
            true,
            Format::Json,
        ))
        .expect("one JSON value");
        assert_eq!(json["local_addr"], "127.0.0.1:49152");
        assert_eq!(json["fell_back"], true);
    }

    /// The backing line names both ends; an unknown worker port (serve mode)
    /// stays honest rather than printing `:0`.
    #[test]
    fn a_backing_line_names_both_ends_or_neither() {
        let both = super::forward_backing("db", "fwd-1", 5432, 15432, Format::Human);
        assert!(both.contains("127.0.0.1:5432 here"), "{both}");
        assert!(both.contains("worker localhost:15432"), "{both}");

        let unknown = super::forward_backing("db", "fwd-1", 5432, 0, Format::Human);
        assert!(!unknown.contains(":0"), "{unknown}");

        let json: serde_json::Value = serde_json::from_str(&super::forward_backing(
            "db",
            "fwd-1",
            5432,
            15432,
            Format::Json,
        ))
        .expect("one JSON value");
        assert_eq!(json["local_port"], 5432);
        assert_eq!(json["worker_port"], 15432);
    }
}
