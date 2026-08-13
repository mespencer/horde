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

/// Starts the chain on its own task.
///
/// Stopped by aborting the returned handle. The driver holds its own clone of
/// the log — it has to, to append decisions — so it can never observe "every
/// other handle is gone" from inside its own wait; an explicit abort is the
/// honest shutdown. Aborting mid-pass loses nothing: every append is keyed,
/// and a restarted driver re-reads the log and deduplicates into the same
/// decisions.
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

    /// Runs until aborted, or until the notification channel closes — which a
    /// spawned driver never sees from inside (it holds a log clone itself),
    /// but a driver built over a receiver it does not feed still honors.
    ///
    /// The explicit yield is load-bearing, not politeness. `step` is where
    /// this loop awaits, and a `step` that stops awaiting — a bug, or its
    /// mutant — turns `run` into a poll that never returns, which nothing in
    /// tokio can cancel: abort cannot preempt it and runtime shutdown blocks
    /// on it. One yield per iteration keeps the loop cancellable no matter
    /// what `step` degenerates into, which is what turns that mutant from a
    /// suite-wide hang into an ordinary fast failure (QUALITY_GATES: a clock
    /// cannot bound a loop that never yields).
    pub async fn run(mut self) {
        while self.step().await {
            tokio::task::yield_now().await;
        }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use horde_proto::prost_types::{Struct, Value, value::Kind as ValueKind};
    use horde_proto::v1::{
        AgentSpawned, AgentSpec, PermissionRequestId, ProposedToolCall, event::Kind,
    };

    use super::*;
    use crate::log::PendingEvent;
    use crate::permission::modes::{PermissionMode, PermissionModes};
    use crate::permission::rules::Rule;

    // -- arranging the log ---------------------------------------------------

    async fn append(log: &EventLog, session: &str, key: &str, event: Event) {
        log.append(PendingEvent {
            session_id: session.to_owned(),
            idempotency_key: key.to_owned(),
            payload: event.encode_to_vec(),
            ..PendingEvent::default()
        })
        .await
        .expect("append");
    }

    fn agent_event(agent: &str, kind: Kind) -> Event {
        Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(kind),
        }
    }

    fn spawned(agent: &str, parent: Option<&str>, mode: &str, tree_path: &[&str]) -> Event {
        agent_event(
            agent,
            Kind::AgentSpawned(AgentSpawned {
                parent_agent_id: parent.map(AgentId::new),
                spawned_by: None,
                spec: Some(AgentSpec {
                    agent_type: format!("{agent}-type"),
                    permission_mode: mode.to_owned(),
                    ..AgentSpec::default()
                }),
                tree_path: tree_path.iter().map(|s| (*s).to_owned()).collect(),
            }),
        )
    }

    fn call(tool: &str, path: &str) -> ProposedToolCall {
        let file_path = Value {
            kind: Some(ValueKind::StringValue(path.to_owned())),
        };
        ProposedToolCall {
            tool_name: tool.to_owned(),
            input: Some(Struct {
                fields: [("file_path".to_owned(), file_path)].into(),
            }),
            rendered: format!("{tool} {path}"),
        }
    }

    fn requested(agent: &str, request_id: &str, tool_call: Option<ProposedToolCall>) -> Event {
        agent_event(
            agent,
            Kind::PermissionRequested(PermissionRequested {
                request: Some(PermissionRequest {
                    request_id: Some(PermissionRequestId::new(request_id)),
                    agent_id: Some(AgentId::new(agent)),
                    tool_call,
                    ..PermissionRequest::default()
                }),
            }),
        )
    }

    fn human_decided(agent: &str, request_id: &str, remember: bool) -> Event {
        agent_event(
            agent,
            Kind::PermissionDecided(PermissionDecided {
                decision: Some(PermissionDecision {
                    request_id: Some(PermissionRequestId::new(request_id)),
                    outcome: PermissionOutcome::Allow as i32,
                    decided_by: PermissionLayer::Human as i32,
                    remember_for_session: remember,
                    ..PermissionDecision::default()
                }),
            }),
        )
    }

    // -- reading back what the chain wrote ----------------------------------

    async fn decisions_in(log: &EventLog, session: &str) -> Vec<PermissionDecision> {
        events_in(log, session)
            .await
            .into_iter()
            .filter_map(|event| match event.kind {
                Some(Kind::PermissionDecided(decided)) => decided.decision,
                _ => None,
            })
            .collect()
    }

    async fn requests_in(log: &EventLog, session: &str) -> Vec<PermissionRequest> {
        events_in(log, session)
            .await
            .into_iter()
            .filter_map(|event| match event.kind {
                Some(Kind::PermissionRequested(requested)) => requested.request,
                _ => None,
            })
            .collect()
    }

    async fn events_in(log: &EventLog, session: &str) -> Vec<Event> {
        log.read_all(session)
            .await
            .expect("read")
            .iter()
            .map(|stored| Event::decode(stored.payload.as_slice()).expect("decode"))
            .collect()
    }

    fn driver_over(
        log: &EventLog,
        modes: PermissionModes,
        judge: Option<Arc<dyn PolicyJudge>>,
    ) -> ChainDriver {
        ChainDriver::open(log, Arc::new(modes), judge)
    }

    /// One full pass of the chain over the log, returning how many events it
    /// appended.
    async fn run_chain(
        log: &EventLog,
        modes: PermissionModes,
        judge: Option<Arc<dyn PolicyJudge>>,
    ) -> u64 {
        driver_over(log, modes, judge)
            .catch_up()
            .await
            .expect("the chain must be able to read and write the log")
    }

    /// A registry whose *default* mode allows everything — the arrangement the
    /// escalate-anyway tests use to prove the default was never consulted.
    fn allow_all_by_default() -> PermissionModes {
        PermissionModes::default()
            .with_default(PermissionMode::escalate_everything().allow(Rule::any_tool()))
    }

    /// A mode registry whose one named mode allows `Read` and denies `Bash`.
    fn cautious() -> PermissionModes {
        PermissionModes::default().with_mode(
            "cautious",
            PermissionMode::escalate_everything()
                .deny(Rule::tool("Bash"), "no shell in cautious mode")
                .allow(Rule::tool("Read")),
        )
    }

    /// One session: a root agent under `cautious`, and one pending request.
    async fn one_request(log: &EventLog, tool_call: Option<ProposedToolCall>) {
        append(
            log,
            "s1",
            "spawn-root",
            spawned("root", None, "cautious", &["ci-fix"]),
        )
        .await;
        append(log, "s1", "req-1", requested("root", "req-1", tool_call)).await;
    }

    // -- the deterministic layers, through the whole driver ------------------

    #[tokio::test]
    async fn an_allow_rule_settles_the_request_and_names_its_layer() {
        let log = EventLog::in_memory().expect("log");
        one_request(&log, Some(call("Read", "/workspace/a"))).await;

        let appended = run_chain(&log, cautious(), None).await;
        assert_eq!(appended, 1, "one request, one decision");

        let decisions = decisions_in(&log, "s1").await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(
            decisions[0].request_id.as_ref().map(ToString::to_string),
            Some("req-1".to_owned())
        );
        assert_eq!(decisions[0].outcome, PermissionOutcome::Allow as i32);
        assert_eq!(
            decisions[0].decided_by,
            PermissionLayer::DeterministicAllow as i32,
            "the journal must say which layer decided"
        );
    }

    #[tokio::test]
    async fn a_deny_rule_settles_the_request_with_its_reason() {
        let log = EventLog::in_memory().expect("log");
        one_request(&log, Some(call("Bash", "/x"))).await;

        run_chain(&log, cautious(), None).await;

        let decisions = decisions_in(&log, "s1").await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].outcome, PermissionOutcome::Deny as i32);
        assert_eq!(
            decisions[0].decided_by,
            PermissionLayer::DeterministicDeny as i32
        );
        assert_eq!(
            decisions[0].reason, "no shell in cautious mode",
            "a denial without its reason teaches the agent nothing"
        );
    }

    // -- escalation ----------------------------------------------------------

    #[tokio::test]
    async fn an_unsettled_request_is_re_raised_with_context_for_the_approver() {
        let log = EventLog::in_memory().expect("log");
        one_request(&log, Some(call("Write", "/etc/hosts"))).await;

        run_chain(&log, cautious(), None).await;

        assert!(
            decisions_in(&log, "s1").await.is_empty(),
            "escalation is a human's question, never the chain's answer"
        );
        let requests = requests_in(&log, "s1").await;
        assert_eq!(
            requests.len(),
            2,
            "the original, then one enriched re-raise"
        );

        let enriched = &requests[1];
        assert_eq!(
            enriched.tree_path,
            vec!["ci-fix".to_owned()],
            "the approver reads the tree path"
        );
        assert_eq!(enriched.permission_mode, "cautious");
        assert_eq!(
            enriched.escalated_from,
            PermissionLayer::DeterministicAllow as i32,
            "the layer that gave up is named"
        );
        assert_eq!(
            enriched.tool_call.as_ref().map(|c| c.tool_name.clone()),
            Some("Write".to_owned()),
            "the proposal itself must survive the enrichment"
        );
    }

    /// The structural loop-guard: the re-raise is itself a `PermissionRequested`
    /// row, and processing it again would re-raise forever. Two full passes by
    /// two fresh drivers must leave exactly one enrichment.
    #[tokio::test]
    async fn an_escalation_is_enriched_exactly_once_across_restarts() {
        let log = EventLog::in_memory().expect("log");
        one_request(&log, Some(call("Write", "/etc/hosts"))).await;

        run_chain(&log, cautious(), None).await;
        // A restart re-reads the whole log through a fresh driver.
        run_chain(&log, cautious(), None).await;

        assert_eq!(
            requests_in(&log, "s1").await.len(),
            2,
            "re-processing the log must not stack enrichments"
        );
    }

    #[tokio::test]
    async fn a_request_already_decided_is_left_alone() {
        let log = EventLog::in_memory().expect("log");
        one_request(&log, Some(call("Read", "/x"))).await;
        append(&log, "s1", "human-1", human_decided("root", "req-1", false)).await;

        let appended = run_chain(&log, cautious(), None).await;

        assert_eq!(appended, 0, "a settled request is nobody's to re-answer");
        assert_eq!(decisions_in(&log, "s1").await.len(), 1);
    }

    /// A chain decision must also be final across a restart: the driver's own
    /// `PermissionDecided` projects the request as settled, so a fresh pass
    /// appends nothing more.
    #[tokio::test]
    async fn a_chain_decision_is_not_re_made_on_a_restart() {
        let log = EventLog::in_memory().expect("log");
        one_request(&log, Some(call("Read", "/x"))).await;

        run_chain(&log, cautious(), None).await;
        let second = run_chain(&log, cautious(), None).await;

        assert_eq!(second, 0);
        assert_eq!(decisions_in(&log, "s1").await.len(), 1);
    }

    #[tokio::test]
    async fn an_unknown_mode_escalates_rather_than_guessing() {
        let log = EventLog::in_memory().expect("log");
        append(
            &log,
            "s1",
            "spawn-root",
            spawned("root", None, "typo-mode", &["ci-fix"]),
        )
        .await;
        append(
            &log,
            "s1",
            "req-1",
            requested("root", "req-1", Some(call("Read", "/x"))),
        )
        .await;

        // The registry has no "typo-mode" — resolve must not fall back to the
        // default, whose rules nobody chose for this agent.
        run_chain(&log, allow_all_by_default(), None).await;

        assert!(decisions_in(&log, "s1").await.is_empty());
        assert_eq!(requests_in(&log, "s1").await.len(), 2, "escalated instead");
    }

    #[tokio::test]
    async fn a_request_without_its_proposed_call_escalates() {
        let log = EventLog::in_memory().expect("log");
        append(
            &log,
            "s1",
            "spawn-root",
            spawned("root", None, "", &["ci-fix"]),
        )
        .await;
        append(&log, "s1", "req-1", requested("root", "req-1", None)).await;

        run_chain(&log, allow_all_by_default(), None).await;

        assert!(
            decisions_in(&log, "s1").await.is_empty(),
            "a call the rules never saw must not be allowed by them"
        );
    }

    // -- the judge -----------------------------------------------------------

    /// A scripted judge: a real implementation of the seam that answers from a
    /// script and records what it was asked, in the FakeAdapter sense.
    #[derive(Debug)]
    struct ScriptedJudge {
        ruling: JudgeRuling,
        asked: Mutex<Vec<(String, String, Vec<String>)>>,
    }

    impl ScriptedJudge {
        fn ruling(ruling: JudgeRuling) -> Arc<Self> {
            Arc::new(Self {
                ruling,
                asked: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl PolicyJudge for ScriptedJudge {
        async fn rule(&self, consultation: Consultation<'_>) -> JudgeRuling {
            if let Ok(mut asked) = self.asked.lock() {
                asked.push((
                    consultation.instructions.to_owned(),
                    consultation.call.tool_name.clone(),
                    consultation.ancestry.to_vec(),
                ));
            }
            self.ruling.clone()
        }
    }

    fn judged() -> PermissionModes {
        PermissionModes::default().with_mode(
            "judged",
            PermissionMode::escalate_everything()
                .deny(Rule::tool("Bash"), "never a shell")
                .judged_by("allow reads under /workspace"),
        )
    }

    async fn one_judged_request(log: &EventLog, tool_call: ProposedToolCall) {
        append(
            log,
            "s1",
            "spawn-root",
            spawned("root", None, "judged", &["ci-fix"]),
        )
        .await;
        append(
            log,
            "s1",
            "req-1",
            requested("root", "req-1", Some(tool_call)),
        )
        .await;
    }

    /// One judged request driven through the chain: builds the log, scripts
    /// the judge, runs one pass, and hands both back for assertions.
    async fn judged_run(
        tool_call: ProposedToolCall,
        ruling: JudgeRuling,
    ) -> (EventLog, Arc<ScriptedJudge>) {
        let log = EventLog::in_memory().expect("log");
        one_judged_request(&log, tool_call).await;
        let judge = ScriptedJudge::ruling(ruling);
        run_chain(&log, judged(), Some(judge.clone())).await;
        (log, judge)
    }

    #[tokio::test]
    async fn the_judge_rules_what_the_deterministic_layers_left_unsettled() {
        let (log, judge) = judged_run(
            call("Read", "/workspace/a"),
            JudgeRuling::Allow {
                rationale: "a read under the workspace".to_owned(),
            },
        )
        .await;

        let decisions = decisions_in(&log, "s1").await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].outcome, PermissionOutcome::Allow as i32);
        assert_eq!(decisions[0].decided_by, PermissionLayer::PolicyJudge as i32);
        assert_eq!(
            decisions[0].reason, "a read under the workspace",
            "the judge's rationale is the decision's reason"
        );

        let asked = judge.asked.lock().expect("lock");
        assert_eq!(
            asked.as_slice(),
            &[(
                "allow reads under /workspace".to_owned(),
                "Read".to_owned(),
                vec!["ci-fix".to_owned()],
            )],
            "the judge reads the mode's instructions, the call, and the ancestry"
        );
    }

    #[tokio::test]
    async fn a_judge_denial_reaches_the_journal_as_the_judges() {
        let (log, _judge) = judged_run(
            call("WebFetch", "https://example.com"),
            JudgeRuling::Deny {
                rationale: "the policy says never the network".to_owned(),
            },
        )
        .await;

        let decisions = decisions_in(&log, "s1").await;
        assert_eq!(decisions[0].outcome, PermissionOutcome::Deny as i32);
        assert_eq!(decisions[0].decided_by, PermissionLayer::PolicyJudge as i32);
        assert_eq!(decisions[0].reason, "the policy says never the network");
    }

    #[tokio::test]
    async fn a_deny_rule_runs_before_the_judge_is_ever_asked() {
        // A judge that would allow, behind a rule that denies: the rule wins.
        let never_reached = JudgeRuling::Allow {
            rationale: "would have allowed".to_owned(),
        };
        let (log, judge) = judged_run(call("Bash", "/x"), never_reached).await;

        let sole = decisions_in(&log, "s1").await.remove(0);
        assert_eq!(sole.outcome, PermissionOutcome::Deny as i32);
        assert_eq!(sole.decided_by, PermissionLayer::DeterministicDeny as i32);
        assert!(
            judge.asked.lock().expect("lock").is_empty(),
            "layer order is the security property: deny is final"
        );
    }

    #[tokio::test]
    async fn an_abstaining_judge_escalates_to_the_human() {
        let (log, judge) = judged_run(
            call("Write", "/etc/hosts"),
            JudgeRuling::Abstain {
                rationale: "the instructions do not cover writes".to_owned(),
            },
        )
        .await;

        assert!(decisions_in(&log, "s1").await.is_empty());
        let requests = requests_in(&log, "s1").await;
        assert_eq!(
            requests[1].escalated_from,
            PermissionLayer::PolicyJudge as i32,
            "the approver is told the judge already looked and passed"
        );
        assert_eq!(
            judge.asked.lock().expect("lock").len(),
            1,
            "the enriched re-raise is skipped, so the judge is asked exactly \
             once — twice means the chain is re-evaluating its own escalation"
        );
    }

    #[tokio::test]
    async fn a_mode_that_wants_a_judge_nobody_configured_escalates() {
        let log = EventLog::in_memory().expect("log");
        one_judged_request(&log, call("Write", "/etc/hosts")).await;

        run_chain(&log, judged(), None).await;

        assert!(
            decisions_in(&log, "s1").await.is_empty(),
            "an unrunnable judge stage must not resolve to either outcome"
        );
        assert_eq!(
            requests_in(&log, "s1").await[1].escalated_from,
            PermissionLayer::PolicyJudge as i32
        );
    }

    // -- remember-for-session ------------------------------------------------

    /// The remember arrangement: a `Write /tmp/notes` a human allowed with
    /// remember-for-session, then `second` raised as `req-2`, then one chain
    /// pass over all of it.
    async fn remembered_write_then(second: ProposedToolCall) -> EventLog {
        let log = EventLog::in_memory().expect("log");
        one_request(&log, Some(call("Write", "/tmp/notes"))).await;
        append(&log, "s1", "human-1", human_decided("root", "req-1", true)).await;
        append(
            &log,
            "s1",
            "req-2",
            requested("root", "req-2", Some(second)),
        )
        .await;
        run_chain(&log, cautious(), None).await;
        log
    }

    /// The full remember loop: escalate, a human allows with remember, and the
    /// identical proposal is then settled by the widened allow layer.
    #[tokio::test]
    async fn a_remembered_allow_settles_the_repeat_without_a_human() {
        let log = remembered_write_then(call("Write", "/tmp/notes")).await;

        let decisions = decisions_in(&log, "s1").await;
        let repeat: Vec<_> = decisions
            .iter()
            .filter(|decision| {
                decision.request_id.as_ref().map(ToString::to_string) == Some("req-2".to_owned())
            })
            .collect();
        assert_eq!(repeat.len(), 1, "the repeat is settled by the chain");
        assert_eq!(repeat[0].outcome, PermissionOutcome::Allow as i32);
        assert_eq!(
            repeat[0].decided_by,
            PermissionLayer::DeterministicAllow as i32,
            "remember-for-session widens the deterministic-allow layer"
        );
    }

    #[tokio::test]
    async fn a_different_call_is_not_covered_by_what_was_remembered() {
        let log = remembered_write_then(call("Write", "/etc/hosts")).await;

        let decisions = decisions_in(&log, "s1").await;
        assert!(
            decisions
                .iter()
                .all(|decision| decision.decided_by == PermissionLayer::Human as i32),
            "a different argument re-escalates; only req-1's human decision exists"
        );
    }

    // -- liveness ------------------------------------------------------------

    /// The driver must park, not spin — over a log that *has* rows, none of
    /// which need it. Same instrument as `CommandStream`, and the non-empty
    /// log matters for a second reason: a catch-up whose cursor stops moving
    /// re-reads one page forever, and a driver that never finishes catching up
    /// never parks at all. Zero here is either failure; one is the only pass.
    #[tokio::test]
    async fn an_idle_driver_parks_instead_of_spinning() {
        let log = EventLog::in_memory().expect("log");
        append(&log, "s1", "spawn", spawned("root", None, "", &["ci-fix"])).await;
        append(
            &log,
            "s1",
            "req",
            requested("root", "req-1", Some(call("Read", "/x"))),
        )
        .await;
        append(&log, "s1", "human", human_decided("root", "req-1", false)).await;

        let mut driver = driver_over(&log, PermissionModes::default(), None);
        assert_eq!(driver.parks(), 0);

        let idle = tokio::time::timeout(std::time::Duration::from_millis(200), driver.step()).await;
        assert!(
            idle.is_err(),
            "everything is settled, so nothing may arrive"
        );
        assert_eq!(
            driver.parks(),
            1,
            "0 means it never waited (or never finished catching up); a spin \
             drives this into the thousands"
        );
    }

    /// `Debug` on a driver is what an operator reads when the chain looks
    /// stuck, and the two facts that diagnose it are how far it has folded and
    /// whether it is parking.
    #[tokio::test]
    async fn the_debug_view_shows_how_far_the_chain_has_read() {
        let log = EventLog::in_memory().expect("log");
        let mut driver = driver_over(&log, PermissionModes::default(), None);

        let fresh = format!("{driver:?}");
        assert!(fresh.contains("ChainDriver"), "got {fresh}");
        assert!(
            fresh.contains("row: 0") && fresh.contains("parks: 0"),
            "a driver that has done nothing must say so: {fresh}"
        );

        one_request(&log, Some(call("Read", "/x"))).await;
        driver.catch_up().await.expect("catch up");
        let read = format!("{driver:?}");
        assert!(
            !read.contains("row: 0"),
            "the cursor is the whole diagnosis of a stuck chain: {read}"
        );
    }

    /// The whole loop, live: a request appended after the driver started must
    /// come back decided, and dropping the log must end the task.
    ///
    /// Multi-threaded on purpose. A mutant that hollows `step` out to `true`
    /// turns `run` into a loop that never awaits, and on the current-thread
    /// runtime that starves the very timer bounding this test — the mutant
    /// records a timeout instead of a kill (QUALITY_GATES: a clock cannot
    /// bound a loop that never yields). On a worker thread the spin keeps its
    /// core, the timer fires, and the same mutant fails in seconds.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_running_driver_answers_a_request_appended_later() {
        let log = EventLog::in_memory().expect("log");
        let task = spawn(&log, Arc::new(cautious()), None);

        one_request(&log, Some(call("Read", "/workspace/a"))).await;

        // History and tail both matter: the chain may decide before or after
        // the tail opens, and only the race decides which side carries it.
        let (history, mut tail) = log.tail("s1", 1).await.expect("tail");
        let decision_in = |batch: Vec<crate::log::StoredEvent>| {
            batch.into_iter().find_map(|stored| {
                match Event::decode(stored.payload.as_slice()).ok()?.kind {
                    Some(Kind::PermissionDecided(decided)) => decided.decision,
                    _ => None,
                }
            })
        };
        let decided = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            if let Some(found) = decision_in(history) {
                return Some(found);
            }
            loop {
                let batch = match tail.next_batch().await {
                    Some(Ok(batch)) => batch,
                    _ => return None,
                };
                if let Some(found) = decision_in(batch) {
                    return Some(found);
                }
            }
        })
        .await
        .expect("the chain must decide while the suite watches")
        .expect("a decision, not a closed stream");

        assert_eq!(decided.outcome, PermissionOutcome::Allow as i32);
        assert_eq!(
            decided.decided_by,
            PermissionLayer::DeterministicAllow as i32
        );

        // Abort is the shutdown contract: the driver holds its own log clone,
        // so no drop from outside can close its channel.
        task.abort();
        let ended = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("an aborted driver must end");
        assert!(
            ended.is_err_and(|error| error.is_cancelled()),
            "the driver ends by cancellation, not by panic"
        );
    }

    // -- the pure pieces -----------------------------------------------------

    #[test]
    fn a_wake_up_is_only_fatal_when_the_channel_closed() {
        let notification = Notification {
            session_id: "s1".to_owned(),
            sequence: 1,
        };
        assert!(woken(Ok(notification)));
        assert!(
            woken(Err(broadcast::error::RecvError::Lagged(7))),
            "lagging re-reads from the cursor; the log is the truth"
        );
        assert!(!woken(Err(broadcast::error::RecvError::Closed)));
    }

    #[test]
    fn the_request_mode_outranks_the_agents_only_when_stamped() {
        assert_eq!(mode_in_force("stamped", "spec"), "stamped");
        assert_eq!(mode_in_force("", "spec"), "spec");
        assert_eq!(mode_in_force("stamped", ""), "stamped");
        assert_eq!(mode_in_force("", ""), "");
    }

    #[test]
    fn only_an_enriched_request_counts_as_already_escalated() {
        let bare = PermissionRequest::default();
        assert!(!already_escalated(&bare));

        let enriched = PermissionRequest {
            escalated_from: PermissionLayer::PolicyJudge as i32,
            ..PermissionRequest::default()
        };
        assert!(already_escalated(&enriched));
    }

    fn projected(events: &[Event]) -> SessionState {
        let stored: Vec<crate::log::StoredEvent> = events
            .iter()
            .enumerate()
            .map(|(index, event)| crate::log::StoredEvent {
                session_id: "s1".to_owned(),
                sequence: index as u64 + 1,
                recorded_at_unix_ms: 0,
                payload: event.encode_to_vec(),
            })
            .collect();
        SessionState::project("s1", &stored).expect("project")
    }

    #[test]
    fn ancestry_prefers_the_recorded_tree_path() {
        let state = projected(&[spawned("root", None, "", &["ci-fix"])]);
        assert_eq!(ancestry(&state, "root"), vec!["ci-fix".to_owned()]);
    }

    /// A child spawned without a recorded path splices onto the nearest
    /// ancestor that has one — the shape `spawn_agent` writes today.
    #[test]
    fn ancestry_walks_parents_and_splices_onto_a_recorded_path() {
        let state = projected(&[
            spawned("root", None, "", &["ci-fix"]),
            spawned("child", Some("root"), "", &[]),
            spawned("grandchild", Some("child"), "", &[]),
        ]);
        assert_eq!(
            ancestry(&state, "grandchild"),
            vec![
                "ci-fix".to_owned(),
                "child-type".to_owned(),
                "grandchild-type".to_owned(),
            ],
            "root first, then the walked names in tree order"
        );
    }

    #[test]
    fn ancestry_of_an_unknown_agent_is_empty_not_invented() {
        let state = projected(&[]);
        assert!(ancestry(&state, "ghost").is_empty());
    }

    /// Malformed history can make an agent its own ancestor. The walk is
    /// bounded by the visited set — by structure, not by a clock — so it must
    /// terminate and keep what it saw on the way in.
    #[test]
    fn a_parent_cycle_cannot_spin_the_ancestry_walk() {
        let state = projected(&[
            spawned("a", Some("b"), "", &[]),
            spawned("b", Some("a"), "", &[]),
        ]);
        assert_eq!(
            ancestry(&state, "a"),
            vec!["b-type".to_owned(), "a-type".to_owned()],
            "each agent contributes once, and the walk ends"
        );
    }

    #[test]
    fn an_agent_with_no_type_is_named_by_its_id() {
        let typed = AgentState {
            agent_id: "a1".to_owned(),
            parent_agent_id: None,
            lifecycle: horde_proto::v1::AgentLifecycleState::Queued,
            tree_path: Vec::new(),
            agent_type: "reviewer".to_owned(),
            permission_mode: String::new(),
            worker_id: None,
            spec: None,
            placement_state: horde_proto::v1::PlacementState::Unspecified,
            queued_at_unix_ms: None,
        };
        assert_eq!(display_name(&typed), "reviewer");

        let untyped = AgentState {
            agent_type: String::new(),
            ..typed
        };
        assert_eq!(display_name(&untyped), "a1");
    }
}
