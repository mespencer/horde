//! `horde agent status`: one agent's lifecycle, read off the log.
//!
//! The wire has no agent read model to ask, and needs none: an agent's
//! state, the worker it runs on and its last change are all facts the
//! session log already carries (`AgentLifecycleChanged`, `PlacementChanged`
//! — ADR-0011's `LOST` among them), so the verb replays the session and
//! folds them. The fold is a pure function over envelopes, so it is tested
//! without a server.

use horde_proto::v1::{EventEnvelope, PlacementState, event::Kind};
use serde_json::json;

use crate::client::SyncOptions;
use crate::{Client, ClientError, Format};

/// The `horde agent` verbs.
#[derive(clap::Subcommand, Debug)]
pub enum AgentCommand {
    /// Show an agent's lifecycle state, the worker it is placed on, and its
    /// last change — `failed` and `lost` (its worker's lease lapsed,
    /// ADR-0011) included, with the reason the log recorded.
    Status(StatusArgs),
}

/// `horde agent status <AGENT>`.
#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    /// The agent, by id — or a session name, which addresses that
    /// session's root agent.
    pub agent: String,
    /// The session the agent belongs to, by name, id or prefix. Omitted,
    /// the live sessions are searched (all of them with `--all`).
    #[arg(long)]
    pub session: Option<String>,
    /// Search archived sessions too.
    #[arg(long, alias = "include-archived")]
    pub all: bool,
}

/// What the log says about one agent.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentStatus {
    pub agent_id: String,
    pub session_id: String,
    /// The wire `AgentLifecycleState`; unspecified until the first change.
    pub state: i32,
    /// The wire `PlacementState` of the latest placement, if any.
    pub placement: Option<i32>,
    /// The worker the latest placement named.
    pub worker_id: Option<String>,
    /// The last lifecycle change: its sequence, when the server recorded
    /// it (unix ms), and why.
    pub last_change: Option<LastChange>,
}

/// One lifecycle change, as `agent status` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LastChange {
    pub sequence: u64,
    pub recorded_at_unix_ms: i64,
    pub reason: String,
}

impl AgentStatus {
    /// Whether the agent's worker was declared lost under it (ADR-0011):
    /// the context died with the worker, and nothing will revive it.
    #[must_use]
    pub fn lost(&self) -> bool {
        self.placement == Some(PlacementState::Lost as i32)
    }
}

/// Folds `events` into `agent_id`'s status — `None` when the log never
/// mentions it.
#[must_use]
pub fn fold(agent_id: &str, session_id: &str, events: &[EventEnvelope]) -> Option<AgentStatus> {
    let mut status: Option<AgentStatus> = None;
    for envelope in events {
        let Some(event) = envelope.event.as_ref() else {
            continue;
        };
        if event.agent_id.as_ref().map(ToString::to_string).as_deref() != Some(agent_id) {
            continue;
        }
        let current = status.get_or_insert_with(|| AgentStatus {
            agent_id: agent_id.to_owned(),
            session_id: session_id.to_owned(),
            ..AgentStatus::default()
        });
        apply(current, envelope, event.kind.as_ref());
    }
    status
}

fn apply(status: &mut AgentStatus, envelope: &EventEnvelope, kind: Option<&Kind>) {
    match kind {
        Some(Kind::AgentLifecycleChanged(changed)) => {
            status.state = changed.state;
            status.last_change = Some(LastChange {
                sequence: envelope.sequence,
                recorded_at_unix_ms: envelope.recorded_at.as_ref().map_or(0, |at| {
                    at.seconds.saturating_mul(1_000) + i64::from(at.nanos) / 1_000_000
                }),
                reason: changed.reason.clone(),
            });
        }
        Some(Kind::PlacementChanged(changed)) => {
            if let Some(placement) = changed.placement.as_ref() {
                status.placement = Some(placement.state);
                if let Some(worker) = placement.worker_id.as_ref() {
                    status.worker_id = Some(worker.to_string());
                }
            }
        }
        _ => {}
    }
}

/// The state word: the lifecycle's, with `lost` named when the placement
/// says the worker went with it.
fn state_word(status: &AgentStatus) -> String {
    let lifecycle = horde_tui::state::agent_lifecycle_name(status.state);
    if status.lost() {
        format!("{lifecycle} (lost with its worker)")
    } else {
        lifecycle.to_owned()
    }
}

/// Renders one agent's status in either format; `now_unix_ms` is a
/// parameter so the age of the last change is a fact under test.
#[must_use]
pub fn render(status: &AgentStatus, format: Format, now_unix_ms: i64) -> String {
    let worker = status.worker_id.clone();
    match format {
        Format::Json => json!({
            "agent_id": status.agent_id,
            "session_id": status.session_id,
            "state": horde_tui::state::agent_lifecycle_name(status.state),
            "lost": status.lost(),
            "placement": status.placement.map(horde_tui::state::placement_state_name),
            "worker_id": worker,
            "last_change": status.last_change.as_ref().map(|change| json!({
                "sequence": change.sequence,
                "recorded_at_unix_ms": change.recorded_at_unix_ms,
                "reason": change.reason,
            })),
        })
        .to_string(),
        Format::Human => {
            let change = status.last_change.as_ref().map_or_else(
                || "no lifecycle change recorded yet".to_owned(),
                |change| {
                    let age = (now_unix_ms - change.recorded_at_unix_ms).max(0) / 1_000;
                    let reason = if change.reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {}", change.reason)
                    };
                    format!("at #{} ({age}s ago){reason}", change.sequence)
                },
            );
            format!(
                "agent {}\nsession {}\nstate {}\nworker {}\nlast change {change}",
                status.agent_id,
                status.session_id,
                state_word(status),
                worker.as_deref().unwrap_or("none")
            )
        }
    }
}

/// Runs `horde agent …`.
///
/// # Errors
///
/// Resolution or replay failing, or the agent appearing in no searched
/// session.
pub async fn run(
    client: &mut Client,
    command: &AgentCommand,
    format: Format,
) -> Result<(), ClientError> {
    let AgentCommand::Status(args) = command;
    let status = find(client, args).await?;
    println!("{}", render(&status, format, crate::render::now_unix_ms()));
    Ok(())
}

/// The status of the agent `args` names, from the session that holds it.
async fn find(client: &mut Client, args: &StatusArgs) -> Result<AgentStatus, ClientError> {
    let agent_id = crate::resolve::agent_id(client, &args.agent).await?;
    let sessions = match &args.session {
        Some(reference) => vec![crate::resolve::session_id(client, reference, args.all).await?],
        None => client
            .list_sessions(args.all)
            .await?
            .iter()
            .filter_map(|summary| summary.session_id.as_ref().map(ToString::to_string))
            .collect(),
    };
    for session_id in sessions {
        let mut events = Vec::new();
        client
            .sync(
                &SyncOptions::new(session_id.clone()),
                |envelope| events.push(envelope.clone()),
                |_| {},
            )
            .await?;
        if let Some(status) = fold(&agent_id, &session_id, &events) {
            return Ok(status);
        }
    }
    Err(ClientError::UnknownAgent {
        reference: args.agent.clone(),
    })
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, AgentLifecycleChanged, AgentLifecycleState, Event, EventEnvelope, Placement,
        PlacementChanged, PlacementState, WorkerId, event::Kind,
    };

    use super::{AgentStatus, LastChange, fold, render};
    use crate::Format;

    fn envelope(sequence: u64, agent: &str, kind: Kind) -> EventEnvelope {
        EventEnvelope {
            sequence,
            recorded_at: Some(horde_proto::prost_types::Timestamp {
                seconds: 100,
                nanos: 5_000_000,
            }),
            event: Some(Event {
                agent_id: Some(AgentId::new(agent)),
                kind: Some(kind),
                ..Event::default()
            }),
            ..EventEnvelope::default()
        }
    }

    fn placed(state: PlacementState, worker: Option<&str>) -> Kind {
        Kind::PlacementChanged(PlacementChanged {
            placement: Some(Placement {
                state: state as i32,
                worker_id: worker.map(WorkerId::new),
                ..Placement::default()
            }),
            ..PlacementChanged::default()
        })
    }

    fn changed(previous: AgentLifecycleState, state: AgentLifecycleState, reason: &str) -> Kind {
        Kind::AgentLifecycleChanged(AgentLifecycleChanged {
            previous_state: previous as i32,
            state: state as i32,
            reason: reason.to_owned(),
        })
    }

    /// A lost agent, as ADR-0011 journals one: placed, running, then the
    /// placement lost (keeping the worker it was lost from) and the
    /// lifecycle failed — and another agent's events change nothing.
    #[test]
    fn the_fold_follows_placement_and_lifecycle_to_a_loss() {
        let events = vec![
            envelope(1, "agent-a", placed(PlacementState::Placed, Some("w1"))),
            envelope(
                2,
                "agent-a",
                changed(
                    AgentLifecycleState::Starting,
                    AgentLifecycleState::Running,
                    "",
                ),
            ),
            envelope(3, "agent-b", placed(PlacementState::Placed, Some("w2"))),
            EventEnvelope::default(),
            envelope(4, "agent-a", placed(PlacementState::Lost, None)),
            envelope(
                5,
                "agent-a",
                changed(
                    AgentLifecycleState::Running,
                    AgentLifecycleState::Failed,
                    "worker w1 lost",
                ),
            ),
        ];
        let status = fold("agent-a", "s1", &events).expect("the log mentions agent-a");
        assert_eq!(
            status,
            AgentStatus {
                agent_id: "agent-a".to_owned(),
                session_id: "s1".to_owned(),
                state: AgentLifecycleState::Failed as i32,
                placement: Some(PlacementState::Lost as i32),
                worker_id: Some("w1".to_owned()),
                last_change: Some(LastChange {
                    sequence: 5,
                    recorded_at_unix_ms: 100_005,
                    reason: "worker w1 lost".to_owned(),
                }),
            }
        );
        assert!(status.lost());
        assert_eq!(fold("agent-z", "s1", &events), None);
    }

    /// Both renderings carry the state, the loss, the worker and the last
    /// change; a status with no change yet says so.
    #[test]
    fn a_status_renders_its_state_worker_and_last_change() {
        let events = vec![
            envelope(4, "agent-a", placed(PlacementState::Lost, Some("w1"))),
            envelope(
                5,
                "agent-a",
                changed(
                    AgentLifecycleState::Running,
                    AgentLifecycleState::Failed,
                    "worker w1 lost",
                ),
            ),
        ];
        let status = fold("agent-a", "s1", &events).expect("mentioned");
        let human = render(&status, Format::Human, 160_005);
        assert_eq!(
            human,
            "agent agent-a\nsession s1\nstate failed (lost with its worker)\nworker w1\n\
             last change at #5 (60s ago): worker w1 lost"
        );
        let json: serde_json::Value =
            serde_json::from_str(&render(&status, Format::Json, 0)).expect("json");
        assert_eq!(json["state"], "failed");
        assert_eq!(json["lost"], true);
        assert_eq!(json["placement"], "lost");
        assert_eq!(json["worker_id"], "w1");
        assert_eq!(json["last_change"]["sequence"], 5);
        assert_eq!(json["last_change"]["recorded_at_unix_ms"], 100_005);
        assert_eq!(json["last_change"]["reason"], "worker w1 lost");

        let quiet = fold(
            "agent-q",
            "s1",
            &[envelope(1, "agent-q", placed(PlacementState::Queued, None))],
        )
        .expect("mentioned");
        let human = render(&quiet, Format::Human, 0);
        assert!(human.contains("state unspecified\n"), "{human}");
        assert!(human.contains("worker none"), "{human}");
        assert!(
            human.contains("no lifecycle change recorded yet"),
            "{human}"
        );
        let running = fold(
            "agent-r",
            "s1",
            &[envelope(
                2,
                "agent-r",
                changed(
                    AgentLifecycleState::Starting,
                    AgentLifecycleState::Running,
                    "",
                ),
            )],
        )
        .expect("mentioned");
        assert!(
            render(&running, Format::Human, 100_005).ends_with("at #2 (0s ago)"),
            "{}",
            render(&running, Format::Human, 100_005)
        );
    }
}
