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
            summaries
                .iter()
                .map(|summary| session(summary, Format::Human))
                .collect::<Vec<_>>()
                .join("\n")
        }
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

/// The wire result status, in the words `report_result` accepts.
fn result_status(status: i32) -> &'static str {
    match horde_proto::v1::ResultStatus::try_from(status) {
        Ok(horde_proto::v1::ResultStatus::Success) => "success",
        Ok(horde_proto::v1::ResultStatus::Partial) => "partial",
        Ok(horde_proto::v1::ResultStatus::Blocked) => "blocked",
        // Never rounded to success: a script acting on an invented outcome
        // is worse than one told the outcome is unreadable.
        Ok(horde_proto::v1::ResultStatus::Unspecified) | Err(_) => "unknown",
    }
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

/// Renders a minted enrollment code (DESIGN §3).
///
/// The code line is the artifact the human carries to the joining machine;
/// everything else is context.
#[must_use]
pub fn minted(response: &horde_proto::v1::MintEnrollmentCodeResponse, format: Format) -> String {
    let expires_at_unix_ms = response.expires_at.as_ref().map_or(0, |at| {
        at.seconds.saturating_mul(1_000) + i64::from(at.nanos) / 1_000_000
    });
    match format {
        Format::Json => json!({
            "code": response.code,
            "server_cert_fingerprint": response.server_cert_fingerprint,
            "expires_at_unix_ms": expires_at_unix_ms,
        })
        .to_string(),
        Format::Human => format!(
            "code {}\nexpires-at-unix-ms {expires_at_unix_ms}",
            response.code
        ),
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

    use super::{Format, created, event, hello, sent, sequence, session, sessions, spawned};

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
        let reported = envelope(Some(Kind::ResultReported(horde_proto::v1::ResultReported {
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
        })));

        let parsed: serde_json::Value =
            serde_json::from_str(&event(&reported, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["detail"]["status"], "partial");
        assert_eq!(parsed["detail"]["summary"], "got halfway");
        assert_eq!(parsed["detail"]["git_refs"][0]["repo"], "atlas");
        assert_eq!(parsed["detail"]["git_refs"][0]["commit"], "abc123");

        // An unreadable status must not become a success a script acts on.
        let unreadable = envelope(Some(Kind::ResultReported(horde_proto::v1::ResultReported {
            result: Some(horde_proto::v1::AgentResult {
                status: 9_999,
                ..horde_proto::v1::AgentResult::default()
            }),
        })));
        let parsed: serde_json::Value =
            serde_json::from_str(&event(&unreadable, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["detail"]["status"], "unknown");
    }

    /// A decision's outcome, reason, and narrowing are part of the record a
    /// follower reads — an `updated_input` that vanished from the render
    /// would hide exactly the narrowing the human imposed.
    #[test]
    fn a_permission_decided_carries_outcome_reason_and_narrowing_in_json() {
        let mut narrowed = horde_proto::prost_types::Struct::default();
        narrowed.fields.insert(
            "path".to_owned(),
            horde_proto::prost_types::Value {
                kind: Some(horde_proto::prost_types::value::Kind::StringValue(
                    "/tmp/safe".to_owned(),
                )),
            },
        );
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
        }
    }

    #[test]
    fn a_minted_code_prints_the_code_and_its_deadline_in_both_formats() {
        let human = minted(&response("s.fp"), Format::Human);
        assert_eq!(human, "code s.fp\nexpires-at-unix-ms 1755500");

        let json: serde_json::Value =
            serde_json::from_str(&minted(&response("s.fp"), Format::Json))
                .expect("json mode emits json");
        assert_eq!(json["code"], "s.fp");
        assert_eq!(json["expires_at_unix_ms"], 1_755_500);
        assert_eq!(json["server_cert_fingerprint"], "ab".repeat(32));
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
}
