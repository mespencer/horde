//! The chain's driver: a fold over the log that answers what it can.
//!
//! The entry is the log itself. A `PermissionRequested` reaches it from either
//! raiser — the Worker journaling a harness escalation, or the MCP endpoint's
//! prompt tool — and this driver sees both the same way, so the chain has one
//! implementation rather than one per entry path. The exit is also the log:
//! every decision the chain makes is an ordinary `PermissionDecided`, which
//! the command projection already turns into a `DeliverPermissionDecisionCommand`
//! for the blocked agent's worker. Nothing between entry and exit is new
//! machinery; the chain is a reader and a writer of events.
//!
//! # What escalation looks like
//!
//! Escalation is *not* an event of its own — the request is already in the log
//! and stays pending, which is what makes it answerable from any client. What
//! the driver does add is context: a request that reaches the human layer is
//! re-raised once, enriched with the agent's tree path, the mode that was in
//! force, and which layer gave up (`PermissionRequest.escalated_from`), so an
//! approver on a phone reads "session ci-fix → reviewer wants `docker rm`"
//! instead of a bare request id. The re-raise is idempotent (a keyed append)
//! and self-limiting: an already-enriched request is never processed again,
//! and the projection refuses to re-open a request that was decided while the
//! re-raise was in flight.
//!
//! # Restarts and redelivery
//!
//! The driver holds no state but a cursor, and every append is keyed, so a
//! restart that re-reads the whole log re-arrives at the same decisions and
//! deduplicates against the ones already written. A request the chain already
//! settled projects as decided and is skipped; one a human already answered
//! likewise. The same property makes the chain safe to run beside a human
//! answering concurrently: whichever decision lands second is a duplicate
//! settle of an already-settled request, which every reader ignores.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use horde_proto::prost_types::Struct;
use horde_proto::v1::{
    AgentId, Event, PermissionDecided, PermissionDecision, PermissionLayer, PermissionOutcome,
    PermissionRequest, PermissionRequested, ProposedToolCall, event::Kind,
};
use prost::Message;
use tokio::sync::broadcast;

use super::chain::{ChainRuling, Consultation, JudgeRuling, PolicyJudge, evaluate};
use super::modes::PermissionModes;
use crate::error::ServerError;
use crate::log::{EventLog, GlobalEvent, Notification};
use crate::service::shared::append_server_event;
use crate::state::{AgentState, PendingPermission, SessionState};

/// How many log rows one fold step reads.
const PAGE: u32 = 512;

/// What the deterministic layers plus (optionally) the judge concluded.
#[derive(Debug, Clone, PartialEq)]
enum Settled {
    Decided {
        outcome: PermissionOutcome,
        decided_by: PermissionLayer,
        reason: String,
        updated_input: Option<Struct>,
    },
    Escalate {
        from: PermissionLayer,
    },
}

/// The permission chain, driving itself off the log.
pub struct ChainDriver {
    log: EventLog,
    modes: Arc<PermissionModes>,
    judge: Option<Arc<dyn PolicyJudge>>,
    /// Append position read up to. Owned here for the same reason
    /// `CommandStream` owns its cursor: the catch-up loop terminates on what
    /// it read, never on what it did with it.
    cursor: u64,
    notifications: broadcast::Receiver<Notification>,
    parks: Arc<AtomicU64>,
}

impl std::fmt::Debug for ChainDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChainDriver")
            .field("row", &self.cursor)
            .field("parks", &self.parks.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Starts the chain on its own task. It ends when the log is dropped.
pub fn spawn(
    log: &EventLog,
    modes: Arc<PermissionModes>,
    judge: Option<Arc<dyn PolicyJudge>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(ChainDriver::open(log, modes, judge).run())
}

impl ChainDriver {
    /// Opens a driver over the log.
    ///
    /// Subscribes before the first read, so a request appended during catch-up
    /// is either in the read or in the channel.
    #[must_use]
    pub fn open(
        log: &EventLog,
        modes: Arc<PermissionModes>,
        judge: Option<Arc<dyn PolicyJudge>>,
    ) -> Self {
        Self {
            notifications: log.subscribe(),
            log: log.clone(),
            modes,
            judge,
            cursor: 0,
            parks: Arc::new(AtomicU64::new(0)),
        }
    }

    /// How many times this driver has waited for the log to move.
    ///
    /// The same liveness instrument as `CommandStream::parks`: a driver that
    /// spins instead of parking answers every request correctly while burning
    /// a core, and only a number a test can read tells the two apart.
    #[must_use]
    pub fn parks(&self) -> u64 {
        self.parks.load(Ordering::Relaxed)
    }

    /// Runs until the log is dropped.
    pub async fn run(mut self) {
        while self.step().await {}
    }

    /// One catch-up plus one park. `false` means the log is gone.
    async fn step(&mut self) -> bool {
        if let Err(error) = self.catch_up().await {
            tracing::error!(%error, "the permission chain could not read or write the log");
        }
        self.parks.fetch_add(1, Ordering::Relaxed);
        woken(self.notifications.recv().await)
    }

    /// Folds every log row appended since the last call, answering what the
    /// chain can answer. Returns how many events it appended.
    async fn catch_up(&mut self) -> Result<u64, ServerError> {
        let mut appended = 0;
        loop {
            let page = self.log.read_global_from(self.cursor, PAGE).await?;
            // Terminates on the page it read: the rows this loop itself
            // appends are decisions and enriched re-raises, and `consider`
            // appends nothing for either, so the tail it chases is finite.
            let Some(last) = page.last() else {
                return Ok(appended);
            };
            self.cursor = last.row;

            for global in &page {
                appended += self.consider(global).await?;
            }
        }
    }

    /// Acts on one log row, returning how many events it appended.
    async fn consider(&self, global: &GlobalEvent) -> Result<u64, ServerError> {
        let Some(request) = raised_in(global) else {
            return Ok(0);
        };
        // The chain's own re-raise: already enriched, already evaluated once.
        // Processing it again would consult the judge twice and re-raise
        // forever.
        if already_escalated(&request) {
            return Ok(0);
        }
        let Some(request_id) = request.request_id.as_ref().map(ToString::to_string) else {
            // No decision can ever quote this request; there is nothing the
            // chain can do that the raiser's own docs do not already cover.
            return Ok(0);
        };

        let session_id = global.event.session_id.clone();
        let events = self.log.read_all(&session_id).await?;
        let state = SessionState::project(&session_id, &events)?;
        let Some(pending) = state.pending_permissions.get(&request_id) else {
            // Already decided — by a human, or by this chain on an earlier
            // pass through the log.
            return Ok(0);
        };

        self.settle(&session_id, &request_id, &request, pending, &state)
            .await
    }

    /// Runs the chain for one pending request and journals what it concluded.
    async fn settle(
        &self,
        session_id: &str,
        request_id: &str,
        request: &PermissionRequest,
        pending: &PendingPermission,
        state: &SessionState,
    ) -> Result<u64, ServerError> {
        let agent = state.agents.get(&pending.agent_id);
        let mode_name = mode_in_force(
            &request.permission_mode,
            agent.map_or("", |agent| agent.permission_mode.as_str()),
        );
        let ancestry = ancestry(state, &pending.agent_id);

        let settled = self
            .ruling_for(mode_name, pending.tool_call.as_ref(), &ancestry, state)
            .await;

        let (key, event) = match settled {
            Settled::Decided {
                outcome,
                decided_by,
                reason,
                updated_input,
            } => (
                format!("chain:{request_id}"),
                decided_event(
                    &pending.agent_id,
                    request_id,
                    outcome,
                    decided_by,
                    reason,
                    updated_input,
                ),
            ),
            Settled::Escalate { from } => (
                format!("escalated:{request_id}"),
                escalated_event(&pending.agent_id, request, ancestry, mode_name, from),
            ),
        };
        append_server_event(&self.log, session_id, &key, event).await?;
        Ok(1)
    }

    /// The deterministic layers, then the judge if the mode wants one.
    async fn ruling_for(
        &self,
        mode_name: &str,
        call: Option<&ProposedToolCall>,
        ancestry: &[String],
        state: &SessionState,
    ) -> Settled {
        let Some(mode) = self.modes.resolve(mode_name) else {
            // A name someone wrote, applied as rules nobody chose, would be a
            // silent misconfiguration. Escalating surfaces it to a human.
            tracing::warn!(mode_name, "unknown permission mode; escalating");
            return Settled::Escalate {
                from: PermissionLayer::DeterministicAllow,
            };
        };
        let Some(call) = call else {
            // A request without its proposed call cannot be rule-matched, and
            // guessing either way is the failure the chain exists to prevent.
            return Settled::Escalate {
                from: PermissionLayer::DeterministicAllow,
            };
        };

        match evaluate(mode, call, ancestry, &state.remembered_allows) {
            ChainRuling::Deny { reason } => Settled::Decided {
                outcome: PermissionOutcome::Deny,
                decided_by: PermissionLayer::DeterministicDeny,
                reason,
                updated_input: None,
            },
            ChainRuling::Allow { updated_input } => Settled::Decided {
                outcome: PermissionOutcome::Allow,
                decided_by: PermissionLayer::DeterministicAllow,
                reason: String::new(),
                updated_input,
            },
            ChainRuling::Consult { instructions } => {
                self.consult(&instructions, call, ancestry).await
            }
            ChainRuling::Escalate { from } => Settled::Escalate { from },
        }
    }

    /// Asks the judge, if one is configured. Abstention — including "no judge
    /// to ask" — escalates rather than resolving to a guessed outcome.
    async fn consult(
        &self,
        instructions: &str,
        call: &ProposedToolCall,
        ancestry: &[String],
    ) -> Settled {
        let Some(judge) = &self.judge else {
            tracing::warn!("the mode wants a policy judge but none is configured; escalating");
            return Settled::Escalate {
                from: PermissionLayer::PolicyJudge,
            };
        };
        match judge
            .rule(Consultation {
                instructions,
                call,
                ancestry,
            })
            .await
        {
            JudgeRuling::Allow { rationale } => Settled::Decided {
                outcome: PermissionOutcome::Allow,
                decided_by: PermissionLayer::PolicyJudge,
                reason: rationale,
                updated_input: None,
            },
            JudgeRuling::Deny { rationale } => Settled::Decided {
                outcome: PermissionOutcome::Deny,
                decided_by: PermissionLayer::PolicyJudge,
                reason: rationale,
                updated_input: None,
            },
            JudgeRuling::Abstain { rationale } => {
                tracing::info!(rationale, "the policy judge abstained; escalating");
                Settled::Escalate {
                    from: PermissionLayer::PolicyJudge,
                }
            }
        }
    }
}

/// Whether a park's wake-up means there is more log to read (`true`), or the
/// log is gone (`false`).
///
/// Lagging is survivable for the same reason it is everywhere else: the
/// channel is a hint and the log is the truth, so a driver that fell behind
/// simply re-reads from its cursor.
fn woken(outcome: Result<Notification, broadcast::error::RecvError>) -> bool {
    !matches!(outcome, Err(broadcast::error::RecvError::Closed))
}

/// The permission request this row raises, if it raises one.
fn raised_in(global: &GlobalEvent) -> Option<PermissionRequest> {
    let event = Event::decode(global.event.payload.as_slice()).ok()?;
    match event.kind? {
        Kind::PermissionRequested(requested) => requested.request,
        _ => None,
    }
}

/// Whether this request is the chain's own enriched re-raise.
fn already_escalated(request: &PermissionRequest) -> bool {
    request.escalated_from != PermissionLayer::Unspecified as i32
}

/// Which mode governs a request: the one stamped on it, else the agent's own.
fn mode_in_force<'a>(request_mode: &'a str, agent_mode: &'a str) -> &'a str {
    if request_mode.is_empty() {
        agent_mode
    } else {
        request_mode
    }
}

/// An agent's ancestry as display names, root first.
///
/// The spawn-time `tree_path` is authoritative where the log carries one;
/// agents spawned without it are walked up their parent chain, splicing onto
/// the nearest ancestor that has a recorded path. The walk is bounded by the
/// visited set, not by a clock: malformed history cannot repeat a state, so it
/// cannot spin.
fn ancestry(state: &SessionState, agent_id: &str) -> Vec<String> {
    let mut below = Vec::new();
    let mut visited = BTreeSet::new();
    let mut current = Some(agent_id.to_owned());

    while let Some(id) = current {
        if !visited.insert(id.clone()) {
            break;
        }
        let Some(agent) = state.agents.get(&id) else {
            break;
        };
        if !agent.tree_path.is_empty() {
            let mut path = agent.tree_path.clone();
            below.reverse();
            path.append(&mut below);
            return path;
        }
        below.push(display_name(agent));
        current = agent.parent_agent_id.clone();
    }

    below.reverse();
    below
}

/// What an agent is called in an approver-facing path.
fn display_name(agent: &AgentState) -> String {
    if agent.agent_type.is_empty() {
        agent.agent_id.clone()
    } else {
        agent.agent_type.clone()
    }
}

/// The chain's decision, as the event every reader already understands.
fn decided_event(
    agent_id: &str,
    request_id: &str,
    outcome: PermissionOutcome,
    decided_by: PermissionLayer,
    reason: String,
    updated_input: Option<Struct>,
) -> Event {
    Event {
        agent_id: Some(AgentId::new(agent_id)),
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::PermissionDecided(PermissionDecided {
            decision: Some(PermissionDecision {
                request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
                outcome: outcome as i32,
                decided_by: decided_by as i32,
                decided_by_actor: None,
                reason,
                remember_for_session: false,
                updated_input,
            }),
        })),
    }
}

/// The enriched re-raise: the same request, now carrying what an approver
/// needs — ancestry, the mode in force, and which layer gave up.
fn escalated_event(
    agent_id: &str,
    request: &PermissionRequest,
    ancestry: Vec<String>,
    mode_name: &str,
    from: PermissionLayer,
) -> Event {
    Event {
        agent_id: Some(AgentId::new(agent_id)),
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::PermissionRequested(PermissionRequested {
            request: Some(PermissionRequest {
                tree_path: ancestry,
                permission_mode: mode_name.to_owned(),
                escalated_from: from as i32,
                ..request.clone()
            }),
        })),
    }
}
