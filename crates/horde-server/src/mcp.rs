//! The MCP Hub catalog: which user MCP servers exist, and who sees them.
//!
//! ADR-0004: user MCP servers are configured at the Server and proxied onto
//! every Agent's one endpoint, so each server is authenticated exactly once. A
//! config marked `local` runs Worker-side instead, one instance per Agent —
//! the pressure valve for machine-bound or stateful servers.
//!
//! Like the Workspace Definition catalog, this is server-owned state, and
//! ADR-0001 admits no store for server state but the event log — so the
//! catalog is a fold over `McpServerConfigChanged` events in a reserved
//! stream. Carrying the full post-change config makes the fold
//! last-writer-wins, so a replay from any offset converges.

use std::collections::BTreeMap;

use horde_proto::v1::{Event, McpServerConfig, event::Kind, mcp_server_config::Transport};
use prost::Message;

use crate::error::ServerError;
use crate::log::StoredEvent;

/// The reserved stream the MCP Hub catalog lives in.
///
/// Server-assigned session ids are `session-…`, so nothing user-created can
/// collide with it. A stream, not a Session:
/// [`is_mcp_catalog_stream`] keeps it out of every session listing.
pub const MCP_CATALOG_STREAM: &str = "horde-mcp-catalog";

/// Whether a log stream is the MCP Hub catalog rather than a session.
#[must_use]
pub fn is_mcp_catalog_stream(session_id: &str) -> bool {
    session_id == MCP_CATALOG_STREAM
}

/// One configured server as the catalog fold currently sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerRecord {
    /// The full post-change config, with `etag` already stamped from the
    /// catalog stream sequence of the change that produced it.
    pub config: McpServerConfig,
    pub deleted: bool,
}

/// The MCP Hub catalog, folded from the catalog stream.
///
/// Pure, like [`crate::workspace::WorkspaceCatalog`]: same events in, same
/// catalog out. Etags are the catalog stream sequence of each server's latest
/// change — deterministic across replays, so a restart cannot invalidate an
/// optimistic-concurrency token a client is holding.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct McpServerCatalog {
    records: BTreeMap<String, ServerRecord>,
}

impl McpServerCatalog {
    /// Folds catalog events into the catalog.
    ///
    /// Events that are not `McpServerConfigChanged`, or that carry no config
    /// or no name, are ignored rather than fatal: one malformed event must not
    /// take down every hub operation.
    ///
    /// # Errors
    ///
    /// [`ServerError::Decode`] when a stored payload is not an `Event` at all,
    /// which is log corruption rather than a bad catalog entry.
    pub fn project(events: &[StoredEvent]) -> Result<Self, ServerError> {
        events
            .iter()
            .try_fold(Self::default(), |mut catalog, stored| {
                if let Some(Kind::McpServerConfigChanged(changed)) =
                    Event::decode(stored.payload.as_slice())?.kind
                {
                    catalog.fold_change(stored.sequence, changed);
                }
                Ok(catalog)
            })
    }

    /// Folds one already-decoded catalog change.
    ///
    /// Public because the command projection folds the same events row by row
    /// — one fold, two drivers, so the catalog the spawn command resolves
    /// against cannot drift from the one the RPCs serve.
    pub fn fold_change(&mut self, sequence: u64, changed: horde_proto::v1::McpServerConfigChanged) {
        let Some(mut config) = changed.config else {
            return;
        };
        if config.name.is_empty() {
            return;
        }

        config.etag = sequence.to_string();
        self.records.insert(
            config.name.clone(),
            ServerRecord {
                config,
                deleted: changed.deleted,
            },
        );
    }

    /// A live server by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&McpServerConfig> {
        self.records
            .get(name)
            .filter(|record| !record.deleted)
            .map(|record| &record.config)
    }

    /// Every live server, in name order.
    pub fn live(&self) -> impl Iterator<Item = &McpServerConfig> {
        self.records
            .values()
            .filter(|record| !record.deleted)
            .map(|record| &record.config)
    }

    /// The `local` servers `agent_type` may see — the full configs a
    /// `SpawnAgentCommand` carries so the Worker can spawn them per-Agent
    /// without a round trip.
    #[must_use]
    pub fn local_servers_for(&self, agent_type: &str) -> Vec<McpServerConfig> {
        self.live()
            .filter(|config| config.local && visible_to(config, agent_type))
            .cloned()
            .collect()
    }

    /// The hub-hosted servers `agent_type` may see, by name — the namespace
    /// prefixes the Agent's endpoint admits from the hub's toolset.
    #[must_use]
    pub fn visible_hub_servers_for(&self, agent_type: &str) -> Vec<String> {
        self.live()
            .filter(|config| !config.local && visible_to(config, agent_type))
            .map(|config| config.name.clone())
            .collect()
    }
}

/// Whether `agent_type`'s Agents may see this server's tools.
///
/// An empty allowlist means every Agent. A non-empty one fails closed: an
/// Agent whose spawn declared no type (`agent_type` empty) sees only
/// unrestricted servers, because granting a scoped server to an unscoped
/// Agent would make "restrict this to reviewers" widen the moment someone
/// forgets to name a type.
#[must_use]
pub fn visible_to(config: &McpServerConfig, agent_type: &str) -> bool {
    config.allowed_agent_types.is_empty()
        || config
            .allowed_agent_types
            .iter()
            .any(|allowed| allowed == agent_type)
}

/// Refuses an ill-formed config, in words the caller can fix.
///
/// The name rule is load-bearing rather than cosmetic: the name is the
/// namespace prefix on every proxied tool (`{name}__{tool}`), and the split
/// back off the wire is on the first `__` — so the charset excludes `_`
/// entirely, which is what makes the prefix unambiguous.
///
/// # Errors
///
/// [`ServerError::Invalid`] naming the first shortfall.
pub fn validate_config(config: &McpServerConfig) -> Result<(), ServerError> {
    if config.name.is_empty() {
        return Err(ServerError::Invalid(
            "an MCP server needs a name; it is the namespace prefix agents see".to_owned(),
        ));
    }
    if !config
        .name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(ServerError::Invalid(format!(
            "MCP server name {:?} must be lowercase [a-z0-9-]: the name prefixes \
             every proxied tool as `{{name}}__{{tool}}`, and the split back off \
             the wire is only unambiguous if the name cannot contain `_`",
            config.name
        )));
    }
    match &config.transport {
        None => Err(ServerError::Invalid(format!(
            "MCP server {} names no transport; give it a stdio command or an http url",
            config.name
        ))),
        Some(Transport::Stdio(stdio)) if stdio.command.is_empty() => {
            Err(ServerError::Invalid(format!(
                "MCP server {} has a stdio transport with no command",
                config.name
            )))
        }
        Some(Transport::Http(http)) if http.url.is_empty() => Err(ServerError::Invalid(format!(
            "MCP server {} has an http transport with no url",
            config.name
        ))),
        Some(Transport::Stdio(_) | Transport::Http(_)) => Ok(()),
    }
}

/// Rules on one configure attempt: create or update, checked against what
/// the catalog holds.
///
/// The etag is what splits the two intents — empty means "I believe this
/// name is new", non-empty means "I read this version and am changing it" —
/// and both beliefs are checked rather than trusted, so a concurrent writer
/// surfaces as a refusal instead of a silent last-writer-wins overwrite
/// through the RPC surface. A named decision rather than logic in the
/// handler, for the QUALITY_GATES reason: all four corners of
/// (exists, etag-empty) must be assertable without arranging an RPC.
///
/// # Errors
///
/// [`ServerError::McpServerNameTaken`] for a create over a live name — the
/// caller's mistake is the name, not the token. [`ServerError::NoSuchMcpServer`]
/// for an update naming a server that is not there — the etag the caller
/// holds is for something already deleted. [`ServerError::StaleMcpServerEtag`]
/// for an update whose token is not current.
pub fn check_configure(
    existing: Option<&McpServerConfig>,
    attempted: &McpServerConfig,
) -> Result<(), ServerError> {
    match (existing, attempted.etag.is_empty()) {
        (Some(_), true) => Err(ServerError::McpServerNameTaken {
            name: attempted.name.clone(),
        }),
        (Some(current), false) => check_etag(current, &attempted.etag),
        (None, true) => Ok(()),
        (None, false) => Err(ServerError::NoSuchMcpServer {
            name: attempted.name.clone(),
        }),
    }
}

/// Enforces the optimistic-concurrency token on a mutation.
///
/// # Errors
///
/// [`ServerError::StaleMcpServerEtag`] on a mismatch — ABORTED per the proto,
/// so the caller re-reads and retries rather than blindly re-sending.
pub fn check_etag(config: &McpServerConfig, supplied: &str) -> Result<(), ServerError> {
    if config.etag == supplied {
        return Ok(());
    }
    Err(ServerError::StaleMcpServerEtag {
        name: config.name.clone(),
        current: config.etag.clone(),
        supplied: supplied.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        Event, McpHttpTransport, McpServerConfig, McpServerConfigChanged, McpStdioTransport,
        event::Kind, mcp_server_config::Transport,
    };
    use prost::Message;

    use super::{
        MCP_CATALOG_STREAM, McpServerCatalog, is_mcp_catalog_stream, validate_config, visible_to,
    };
    use crate::error::ServerError;
    use crate::log::StoredEvent;

    fn stdio(name: &str) -> McpServerConfig {
        let transport = Transport::Stdio(McpStdioTransport {
            command: "server-bin".to_owned(),
            ..McpStdioTransport::default()
        });
        McpServerConfig {
            name: name.to_owned(),
            transport: Some(transport),
            ..McpServerConfig::default()
        }
    }

    fn changed_at(sequence: u64, config: McpServerConfig, deleted: bool) -> StoredEvent {
        let event = Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::McpServerConfigChanged(McpServerConfigChanged {
                config: Some(config),
                deleted,
            })),
        };
        StoredEvent {
            session_id: MCP_CATALOG_STREAM.to_owned(),
            sequence,
            recorded_at_unix_ms: 0,
            payload: event.encode_to_vec(),
        }
    }

    fn project(events: &[StoredEvent]) -> McpServerCatalog {
        McpServerCatalog::project(events).unwrap_or_else(|error| {
            panic!("the catalog must fold these events: {error}");
        })
    }

    #[test]
    fn the_catalog_stream_is_reserved_and_sessions_are_not() {
        assert!(is_mcp_catalog_stream(MCP_CATALOG_STREAM));
        assert!(!is_mcp_catalog_stream("session-1"));
        assert!(!is_mcp_catalog_stream(""));
        assert!(
            crate::enrollment::is_reserved_stream(MCP_CATALOG_STREAM),
            "session listings and placement walk is_reserved_stream, so a \
             catalog stream missing from it becomes a phantom session"
        );
    }

    #[test]
    fn the_fold_is_last_writer_wins_and_stamps_etags_from_sequence() {
        let mut second = stdio("linear");
        second.local = true;

        let catalog = project(&[
            changed_at(1, stdio("linear"), false),
            changed_at(2, second, false),
        ]);

        let live = catalog
            .get("linear")
            .unwrap_or_else(|| panic!("linear must be live"));
        assert!(live.local, "the later write must win");
        assert_eq!(live.etag, "2", "the etag is the winning change's sequence");
    }

    #[test]
    fn a_deleted_server_is_gone_but_the_log_still_records_it() {
        let catalog = project(&[
            changed_at(1, stdio("linear"), false),
            changed_at(2, stdio("linear"), true),
        ]);

        assert_eq!(catalog.get("linear"), None);
        assert_eq!(catalog.live().count(), 0);
    }

    #[test]
    fn a_recreated_server_after_deletion_is_live_again() {
        let catalog = project(&[
            changed_at(1, stdio("linear"), false),
            changed_at(2, stdio("linear"), true),
            changed_at(3, stdio("linear"), false),
        ]);
        assert!(catalog.get("linear").is_some());
    }

    #[test]
    fn malformed_catalog_events_are_ignored_rather_than_fatal() {
        let no_config = StoredEvent {
            session_id: MCP_CATALOG_STREAM.to_owned(),
            sequence: 1,
            recorded_at_unix_ms: 0,
            payload: Event {
                agent_id: None,
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(Kind::McpServerConfigChanged(McpServerConfigChanged {
                    config: None,
                    deleted: false,
                })),
            }
            .encode_to_vec(),
        };
        let unnamed = changed_at(2, stdio(""), false);

        let catalog = project(&[no_config, unnamed, changed_at(3, stdio("grafana"), false)]);
        assert_eq!(
            catalog.live().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["grafana"],
            "the well-formed entry must survive its malformed neighbours"
        );
    }

    /// The visibility truth table — extracted as a named predicate per
    /// QUALITY_GATES, because a compound condition inside the fold could not
    /// have its cases exercised.
    #[test]
    fn visibility_is_open_by_default_and_fails_closed_when_scoped() {
        let open = stdio("linear");
        assert!(visible_to(&open, "reviewer"));
        assert!(visible_to(&open, ""), "an untyped agent sees open servers");

        let mut scoped = stdio("linear");
        scoped.allowed_agent_types = vec!["reviewer".to_owned()];
        assert!(visible_to(&scoped, "reviewer"));
        assert!(!visible_to(&scoped, "test-runner"));
        assert!(
            !visible_to(&scoped, ""),
            "an untyped agent must not see a scoped server: non-empty fails closed"
        );
    }

    #[test]
    fn spawn_resolution_splits_local_configs_from_hub_names() {
        let mut local = stdio("browser");
        local.local = true;
        let mut scoped_hub = stdio("linear");
        scoped_hub.allowed_agent_types = vec!["reviewer".to_owned()];

        let catalog = project(&[
            changed_at(1, local, false),
            changed_at(2, scoped_hub, false),
            changed_at(3, stdio("grafana"), false),
        ]);

        let locals = catalog.local_servers_for("reviewer");
        assert_eq!(
            locals.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["browser"]
        );
        assert_eq!(
            catalog.visible_hub_servers_for("reviewer"),
            vec!["grafana".to_owned(), "linear".to_owned()]
        );
        assert_eq!(
            catalog.visible_hub_servers_for(""),
            vec!["grafana".to_owned()],
            "the scoped hub server must not reach an untyped agent"
        );
    }

    #[test]
    fn validation_refuses_the_names_that_would_break_the_namespace() {
        for bad in ["", "Linear", "my_server", "a__b", "spaced name", "ütf"] {
            let error = validate_config(&stdio(bad));
            assert!(
                matches!(error, Err(ServerError::Invalid(_))),
                "{bad:?} must be refused"
            );
        }
        assert!(validate_config(&stdio("linear-2")).is_ok());
    }

    #[test]
    fn validation_requires_a_usable_transport() {
        let mut no_transport = stdio("linear");
        no_transport.transport = None;
        assert!(matches!(
            validate_config(&no_transport),
            Err(ServerError::Invalid(_))
        ));

        let mut empty_command = stdio("linear");
        empty_command.transport = Some(Transport::Stdio(McpStdioTransport::default()));
        assert!(matches!(
            validate_config(&empty_command),
            Err(ServerError::Invalid(_))
        ));

        let mut empty_url = stdio("linear");
        empty_url.transport = Some(Transport::Http(McpHttpTransport::default()));
        assert!(matches!(
            validate_config(&empty_url),
            Err(ServerError::Invalid(_))
        ));

        let mut http = stdio("linear");
        http.transport = Some(Transport::Http(McpHttpTransport {
            url: "http://127.0.0.1:8931/mcp".to_owned(),
        }));
        assert!(validate_config(&http).is_ok());
    }

    /// All four corners of (exists, etag-empty) — the decision the configure
    /// RPC hangs on, exercised without an RPC.
    #[test]
    fn a_configure_is_ruled_by_what_exists_and_what_the_etag_claims() {
        let mut current = stdio("linear");
        current.etag = "3".to_owned();

        let create = stdio("linear");
        assert!(
            super::check_configure(None, &create).is_ok(),
            "an empty etag over an absent name creates"
        );
        assert!(
            matches!(
                super::check_configure(Some(&current), &create),
                Err(ServerError::McpServerNameTaken { name }) if name == "linear"
            ),
            "a create over a live name is AlreadyExists, not a stale etag"
        );

        let mut update = stdio("linear");
        update.etag = "3".to_owned();
        assert!(
            super::check_configure(Some(&current), &update).is_ok(),
            "the current token updates"
        );
        assert!(
            matches!(
                super::check_configure(None, &update),
                Err(ServerError::NoSuchMcpServer { name }) if name == "linear"
            ),
            "an update naming an absent server is NotFound"
        );

        let mut stale = stdio("linear");
        stale.etag = "2".to_owned();
        assert!(
            matches!(
                super::check_configure(Some(&current), &stale),
                Err(ServerError::StaleMcpServerEtag { .. })
            ),
            "a stale token aborts"
        );
    }

    #[test]
    fn the_etag_check_accepts_the_current_token_and_refuses_the_stale_one() {
        let mut config = stdio("linear");
        config.etag = "3".to_owned();

        assert!(super::check_etag(&config, "3").is_ok());
        let refused = super::check_etag(&config, "2");
        assert!(
            matches!(
                &refused,
                Err(ServerError::StaleMcpServerEtag { name, current, supplied })
                    if name == "linear" && current == "3" && supplied == "2"
            ),
            "got {refused:?}"
        );
    }
}
