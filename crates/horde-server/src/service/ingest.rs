//! Journal ingestion: where a worker's events become log rows — or don't.
//!
//! This is where the two sequence spaces meet — per-worker journal positions
//! coming in, per-session log positions going out — and, since ADR-0009,
//! where the server decides what a worker is allowed to say at all.
//!
//! # What a worker may journal
//!
//! A worker *observes*; it does not decide. So the kinds it may journal are
//! the observations — agent output, lifecycle, permission requests, results,
//! interrupt acknowledgements, usage, workspace instance changes — and only
//! about agents the log has placed on it ([`OriginView::judge`]). Placement,
//! permission decisions, the catalogs, enrollment, forwards, logins, spawns
//! and messages are server decisions, and a journal carrying one is refused
//! however well-formed it is: before this, a worker could decide its own
//! permission requests or rewrite the MCP catalog by journaling the event.
//!
//! # Why a refusal is a row, not an error
//!
//! A refused event that failed the batch wedged the worker: it reconnects,
//! resumes from the same journal position, and offers the same event again,
//! forever. So a refusal is appended as a [`JournalEventRefused`] dead letter
//! in [`DEAD_LETTER_STREAM`], under the worker's origin and the refused
//! event's journal sequence. The worker's watermark is derived from stored
//! rows, so it advances past the refusal; the `(origin, journal_sequence)`
//! dedupe index turns a replay of the same poison event into a no-op; and the
//! refusal is an auditable event like everything else. Only a storage fault —
//! the server failing, not the worker — still fails the batch.
//!
//! # Late journals from a former placement (ADR-0011)
//!
//! A worker partitioned away keeps journaling, and by the time it returns
//! its agent may have been re-placed or declared lost. Such an entry is not
//! a stranger's and is not dead-lettered: output and usage are history and
//! land as themselves; anything that would speak for the agent *now* is
//! recorded whole as a `StaleJournalEvent` in the agent's own session —
//! deliberate, visible to every attached client, never silent.

use std::collections::{BTreeMap, BTreeSet};

use horde_proto::v1::{
    Event, JournalEventRefused, JournaledEvent, PlacementState, SessionId, StaleJournalEvent,
    WorkerId, WorkspaceInstanceChanged, event::Kind,
};
use prost::Message;

use super::shared::ServerCore;
use crate::error::ServerError;
use crate::log::{EventLog, GlobalEvent, PendingEvent};

/// The reserved stream refused journal entries are recorded in.
pub const DEAD_LETTER_STREAM: &str = "horde-dead-letters";

/// Whether a log stream is the dead-letter stream.
#[must_use]
pub fn is_dead_letter_stream(session_id: &str) -> bool {
    session_id == DEAD_LETTER_STREAM
}

/// How many log rows one catch-up step reads.
const PAGE: u32 = 512;

/// Where the log says one agent lives.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Residence {
    session_id: String,
    /// The worker it is placed on; `None` while queued or never placed.
    worker_id: Option<String>,
}

/// The facts a journal entry is judged against, folded incrementally from
/// the whole log: which session each agent belongs to and which worker it is
/// placed on, and which workers were asked to materialize into which stream.
///
/// One per journal stream, caught up before each batch. Incremental because
/// the alternative — projecting a session per event — is a whole-session read
/// for every chunk of agent output.
#[derive(Debug, Default)]
pub struct OriginView {
    agents: BTreeMap<String, Residence>,
    /// `(agent, worker)` pairs where the worker once held the agent and no
    /// longer does — the placements a late journal can come from (ADR-0011).
    former: BTreeSet<(String, String)>,
    /// Each worker's journal floor, from its latest reset (ADR-0011).
    floors: BTreeMap<String, u64>,
    /// `(stream, worker)` pairs a prewarm named: the only way a worker's
    /// agent-less instance change has a reason to exist.
    prewarmed: BTreeSet<(String, String)>,
    /// `(request id, worker)` pairs a gc request targeted: the only answers
    /// a worker's `WorkspaceGcCompleted` may give (ADR-0014).
    gc_requests: BTreeSet<(String, String)>,
    cursor: u64,
    /// A plaintext channel's journal: no certificate names its worker, so
    /// there is no origin to judge against (see [`Self::ambient`]).
    ambient: bool,
}

impl OriginView {
    /// The view a certified worker's journal is judged against.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The view for a plaintext journal stream, which judges nothing.
    ///
    /// A plaintext channel exists only on loopback test servers — the
    /// production listener is mTLS-only — and its worker id is whatever the
    /// Start message claimed. Judging origin against a claimed origin would
    /// be theatre; this is the same ambient bootstrap trust the forward and
    /// login planes extend to an unnamed loopback caller. Malformed entries
    /// still dead-letter: that rule is about the entry, not its author.
    #[must_use]
    pub fn ambient() -> Self {
        Self {
            ambient: true,
            ..Self::default()
        }
    }

    /// Reads every log row appended since the last catch-up.
    ///
    /// # Errors
    ///
    /// The log failing to read.
    pub async fn catch_up(&mut self, log: &EventLog) -> Result<(), ServerError> {
        // Even an ambient view folds: it judges nothing, but a journal floor
        // is a fact about storage, not about trust.
        loop {
            let page = log.read_global_from(self.cursor, PAGE).await?;
            let Some(last) = page.last() else {
                return Ok(());
            };
            self.cursor = last.row;
            for global in &page {
                self.fold(global);
            }
        }
    }

    /// Folds one log row. Pure, so the judge's inputs are a function of a
    /// sequence of events.
    pub fn fold(&mut self, global: &GlobalEvent) {
        let Ok(event) = Event::decode(global.event.payload.as_slice()) else {
            return;
        };
        let agent_id = event.agent_id.as_ref().map(ToString::to_string);
        match (event.kind, agent_id) {
            (Some(Kind::AgentSpawned(_)), Some(agent_id)) => {
                self.agents.insert(
                    agent_id,
                    Residence {
                        session_id: global.event.session_id.clone(),
                        worker_id: None,
                    },
                );
            }
            (Some(Kind::PlacementChanged(changed)), Some(agent_id)) => {
                if let Some(residence) = self.agents.get_mut(&agent_id) {
                    let next = changed.placement.as_ref().and_then(placed_worker);
                    if let Some(previous) = residence.worker_id.take()
                        && next.as_deref() != Some(previous.as_str())
                    {
                        self.former.insert((agent_id.clone(), previous));
                    }
                    if let Some(worker_id) = next.as_ref() {
                        // Held again: whatever it journals now is current.
                        self.former.remove(&(agent_id, worker_id.clone()));
                    }
                    residence.worker_id = next;
                }
            }
            (Some(Kind::WorkerJournalReset(reset)), _) => {
                if let Some(worker_id) = reset.worker_id.as_ref() {
                    self.floors
                        .insert(worker_id.to_string(), reset.journal_floor);
                }
            }
            (Some(Kind::WorkspacePrewarmRequested(prewarm)), _) => {
                if let Some(worker_id) = prewarm.worker_id.as_ref() {
                    self.prewarmed
                        .insert((global.event.session_id.clone(), worker_id.to_string()));
                }
            }
            // A gc request names its workers: each may answer it, and a
            // worker removing an Instance reports it MISSING into the
            // stream the request rode.
            (Some(Kind::WorkspaceGcRequested(requested)), _) => {
                for worker in &requested.worker_ids {
                    self.gc_requests
                        .insert((requested.request_id.clone(), worker.to_string()));
                    self.prewarmed
                        .insert((global.event.session_id.clone(), worker.to_string()));
                }
            }
            _ => {}
        }
    }

    /// Whether `worker_id` may journal `event` into `session_id`; the refusal
    /// reason otherwise.
    ///
    /// # Errors
    ///
    /// The reason, in words for the dead letter.
    pub fn judge(&self, worker_id: &str, session_id: &str, event: &Event) -> Result<(), String> {
        if self.ambient {
            return Ok(());
        }
        let agent_id = event
            .agent_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        match event.kind.as_ref() {
            None => Err("the event carries no kind".to_owned()),
            Some(Kind::WorkspaceInstanceChanged(changed)) => {
                self.judge_instance(worker_id, session_id, agent_id.as_deref(), changed)
            }
            Some(Kind::PermissionRequested(raised)) => {
                let named = raised
                    .request
                    .as_ref()
                    .and_then(|request| request.agent_id.as_ref())
                    .map(ToString::to_string)
                    .filter(|id| !id.is_empty());
                if named.is_some() && named != agent_id {
                    return Err(format!(
                        "a permission request raised for agent {} cannot name agent {}",
                        agent_id.unwrap_or_default(),
                        named.unwrap_or_default()
                    ));
                }
                self.judge_agent(worker_id, session_id, agent_id.as_deref())
            }
            Some(
                Kind::AgentOutputChunk(_)
                | Kind::AgentLifecycleChanged(_)
                | Kind::ResultReported(_)
                | Kind::TurnInterrupted(_)
                | Kind::UsageRecorded(_)
                | Kind::CheckoutPrepared(_),
            ) => self.judge_agent(worker_id, session_id, agent_id.as_deref()),
            Some(Kind::WorkspaceCleanup(cleanup)) => {
                judge_housekeeping(worker_id, session_id, cleanup.worker_id.as_ref())
            }
            Some(Kind::WorkspaceDiskPressure(pressure)) => {
                judge_housekeeping(worker_id, session_id, pressure.worker_id.as_ref())
            }
            Some(Kind::WorkspaceGcCompleted(completed)) => {
                judge_housekeeping(worker_id, session_id, completed.worker_id.as_ref())?;
                if self
                    .gc_requests
                    .contains(&(completed.request_id.clone(), worker_id.to_owned()))
                {
                    Ok(())
                } else {
                    Err(format!(
                        "no gc request {} asked worker {worker_id} to clean up",
                        completed.request_id
                    ))
                }
            }
            Some(kind) => Err(format!(
                "{} is written only by the server",
                horde_proto::kind_field_name(kind).unwrap_or("this kind")
            )),
        }
    }

    /// The worker's journal floor (ADR-0011): its journal positions are
    /// stored this far above what it numbers them.
    #[must_use]
    pub fn floor(&self, worker_id: &str) -> u64 {
        self.floors.get(worker_id).copied().unwrap_or(0)
    }

    /// Whether `event` is a late journal from a placement `worker_id` used to
    /// hold for this session's agent (ADR-0011), and if so how it is handled.
    /// `None` for anything else — current placements and strangers alike are
    /// [`Self::judge`]'s.
    #[must_use]
    pub fn lateness(&self, worker_id: &str, session_id: &str, event: &Event) -> Option<Late> {
        if self.ambient {
            return None;
        }
        let agent_id = event.agent_id.as_ref().map(ToString::to_string)?;
        let residence = self.agents.get(&agent_id)?;
        let formerly = residence.session_id == session_id
            && self.former.contains(&(agent_id, worker_id.to_owned()));
        if !formerly {
            return None;
        }
        late_handling(event.kind.as_ref()?, residence.worker_id.clone())
    }

    /// An agent event: the agent must belong to the session it targets and
    /// be placed on the journaling worker.
    fn judge_agent(
        &self,
        worker_id: &str,
        session_id: &str,
        agent_id: Option<&str>,
    ) -> Result<(), String> {
        let agent_id = agent_id.ok_or_else(|| "an agent event must name its agent".to_owned())?;
        let held = self.agents.get(agent_id).is_some_and(|residence| {
            residence.session_id == session_id
                && crate::trust::holds_agent(residence.worker_id.as_deref(), worker_id)
        });
        if held {
            Ok(())
        } else {
            Err(format!(
                "agent {agent_id} in {session_id:?} is not placed on worker {worker_id}"
            ))
        }
    }

    /// An instance change: its report may name only the journaling worker,
    /// and it must answer either a placed agent or a prewarm that named this
    /// worker in this stream.
    fn judge_instance(
        &self,
        worker_id: &str,
        session_id: &str,
        agent_id: Option<&str>,
        changed: &WorkspaceInstanceChanged,
    ) -> Result<(), String> {
        let named = changed
            .report
            .as_ref()
            .and_then(|report| report.worker_id.as_ref())
            .map(ToString::to_string)
            .unwrap_or_default();
        if !named.is_empty() && named != worker_id {
            return Err(format!(
                "worker {worker_id} may not report an instance for worker {named}"
            ));
        }
        if agent_id.is_some() {
            return self.judge_agent(worker_id, session_id, agent_id);
        }
        if self
            .prewarmed
            .contains(&(session_id.to_owned(), worker_id.to_owned()))
        {
            Ok(())
        } else {
            Err(format!(
                "no prewarm asked worker {worker_id} to materialize into {session_id:?}"
            ))
        }
    }
}

/// A housekeeping event (ADR-0014): only into the housekeeping stream, and
/// only about the journaling worker itself.
fn judge_housekeeping(
    worker_id: &str,
    session_id: &str,
    named: Option<&horde_proto::v1::WorkerId>,
) -> Result<(), String> {
    if !crate::workspace::is_housekeeping_stream(session_id) {
        return Err(format!(
            "workspace housekeeping belongs in {}, not {session_id:?}",
            horde_proto::WORKSPACE_HOUSEKEEPING_STREAM
        ));
    }
    let named = named.map(ToString::to_string).unwrap_or_default();
    if named == worker_id {
        Ok(())
    } else {
        Err(format!(
            "worker {worker_id} may not journal housekeeping for worker {named:?}"
        ))
    }
}

/// What a late journal entry from a former placement becomes (ADR-0011).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Late {
    /// Pure history — output, usage — lands as itself: it happened, it is
    /// attributable, and it overwrites nothing the current placement says.
    Land,
    /// Anything that would speak *for* the agent now — a lifecycle, a
    /// permission request, a result, an interrupt ack, an instance change —
    /// is recorded whole as a `StaleJournalEvent` in the agent's session.
    /// Carries where the agent is placed now, if anywhere.
    Record { current_worker_id: Option<String> },
}

/// The late-handling table by kind. Kinds a worker may never journal are
/// not late, they are refused, so they answer `None` and fall to the judge.
fn late_handling(kind: &Kind, current_worker_id: Option<String>) -> Option<Late> {
    match kind {
        Kind::AgentOutputChunk(_) | Kind::UsageRecorded(_) | Kind::CheckoutPrepared(_) => {
            Some(Late::Land)
        }
        Kind::AgentLifecycleChanged(_)
        | Kind::PermissionRequested(_)
        | Kind::ResultReported(_)
        | Kind::TurnInterrupted(_)
        | Kind::WorkspaceInstanceChanged(_) => Some(Late::Record { current_worker_id }),
        _ => None,
    }
}

/// The worker a placement names, when it is actually placed on one.
fn placed_worker(placement: &horde_proto::v1::Placement) -> Option<String> {
    (placement.state == PlacementState::Placed as i32)
        .then(|| placement.worker_id.as_ref().map(ToString::to_string))
        .flatten()
}

/// Ingests one journaled batch, translating journal positions into session
/// positions and dead-lettering what may not land.
///
/// Returns the highest journal sequence durably stored — accepted or
/// dead-lettered — which is what the worker needs in order to truncate.
///
/// The whole batch is one transaction (ADR-0013): every entry is judged
/// first, against the origin view as it stood when the batch arrived —
/// exactly what the one-append-per-entry loop judged against, which caught
/// the view up once per batch too — and then every resulting row, dead
/// letters included, is appended in entry order with one commit. A key
/// collision, only knowable at the append, is dead-lettered directly behind
/// the write that collided, where the loop put it.
///
/// # Errors
///
/// A batch from a stream that never named its worker, or the log failing —
/// in which case nothing of the batch is stored, and the worker re-sends it.
/// Never a refused event: those are dead letters.
pub async fn ingest_batch(
    core: &ServerCore,
    origins: &mut OriginView,
    worker_id: &str,
    events: Vec<JournaledEvent>,
) -> Result<u64, ServerError> {
    if worker_id.is_empty() {
        return Err(ServerError::Invalid(
            "a journal batch must follow the PushEventsStart that names its worker".to_owned(),
        ));
    }
    origins.catch_up(&core.log).await?;

    let mut highest = 0_u64;
    let mut rows = Vec::with_capacity(events.len());
    let mut landings = Vec::with_capacity(events.len());
    for journaled in events {
        highest = highest.max(journaled.journal_sequence);
        let (row, landing) = planned(origins, worker_id, journaled);
        rows.push(row);
        landings.push(landing);
    }

    let landings = std::sync::Arc::new(landings);
    let checked = std::sync::Arc::clone(&landings);
    let appended = core
        .log
        .append_batch(rows, move |index, appended| {
            checked
                .get(index)
                .and_then(Option::as_ref)
                .and_then(|landing| landing.collided(appended))
        })
        .await?;

    // A journaled instance report is also the freshest fact about that
    // worker's disk, so the cache placement reads hears it too — once it
    // has landed.
    for (landing, appended) in landings.iter().zip(&appended) {
        if let Some(landing) = landing
            && landing.collided(appended).is_none()
            && let Some(Kind::WorkspaceInstanceChanged(changed)) = &landing.event.kind
            && let Some(report) = &changed.report
        {
            core.instances.record(report);
        }
    }
    Ok(highest)
}

/// An admitted entry's event, kept beside its row so the append's outcome
/// can be checked against it.
struct Landing {
    session_id: String,
    event: Event,
    refusal: Refusal,
}

impl Landing {
    /// The dead letter owed when the append stored nothing new under a key
    /// another event already holds.
    ///
    /// A journal replay is at-least-once and deduplicates silently, as it
    /// should. Two *different* events under one key is a different thing
    /// entirely: nothing new was stored, so it is dead-lettered rather than
    /// left to wedge the watermark.
    fn collided(&self, appended: &crate::log::Appended) -> Option<PendingEvent> {
        crate::service::shared::refuse_kind_collision(
            appended,
            &self.session_id,
            &self.event.idempotency_key,
            &self.event,
        )
        .err()
        .map(|collision| dead_letter(&self.refusal, collision.to_string()))
    }
}

/// One journal entry's row — the event itself when admissible, its stale
/// record when late, a dead letter otherwise — and, for an admitted event,
/// what its append's outcome is checked against.
fn planned(
    origins: &OriginView,
    worker_id: &str,
    journaled: JournaledEvent,
) -> (PendingEvent, Option<Landing>) {
    // Stored above the worker's journal floor (ADR-0011): after a reset its
    // fresh journal restarts at one, and the log must neither dedupe those
    // entries onto the lost journal's rows nor take them for a regression.
    let journal_sequence = origins
        .floor(worker_id)
        .saturating_add(journaled.journal_sequence);
    let session_id = journaled
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .filter(|id| !id.is_empty());
    let refusal = Refusal {
        worker_id: worker_id.to_owned(),
        journal_sequence,
        session_id: session_id.clone(),
        kind: journaled
            .event
            .as_ref()
            .and_then(|event| event.kind.as_ref())
            .and_then(horde_proto::kind_field_name)
            .unwrap_or_default(),
    };

    let (session_id, event) = match shaped(session_id, journaled.event) {
        Ok(shaped) => shaped,
        Err(reason) => return (dead_letter(&refusal, reason), None),
    };
    match origins.lateness(worker_id, &session_id, &event) {
        Some(Late::Land) => {}
        Some(Late::Record { current_worker_id }) => {
            return (
                stale_record(&refusal, &session_id, event, current_worker_id),
                None,
            );
        }
        None => {
            if let Err(reason) = origins.judge(worker_id, &session_id, &event) {
                return (dead_letter(&refusal, reason), None);
            }
        }
    }

    let row = PendingEvent {
        session_id: session_id.clone(),
        origin_worker_id: worker_id.to_owned(),
        journal_sequence: Some(journal_sequence),
        idempotency_key: event.idempotency_key.clone(),
        payload: event.encode_to_vec(),
    };
    (
        row,
        Some(Landing {
            session_id,
            event,
            refusal,
        }),
    )
}

/// The shape checks, in one place: a session, an event, no inline bulk.
/// What the worker may say about whom is the origin view's question, asked
/// after this.
fn shaped(session_id: Option<String>, event: Option<Event>) -> Result<(String, Event), String> {
    let session_id =
        session_id.ok_or_else(|| "a journaled event must name its session".to_owned())?;
    let event = event.ok_or_else(|| "a journaled event must carry an event".to_owned())?;
    // The other append chokepoint: the worker journal must not carry bulk
    // into the log any more than a client verb may.
    crate::service::shared::refuse_inline_bulk(&event).map_err(|error| error.to_string())?;
    Ok((session_id, event))
}

/// Records a late journal entry from a former placement in the agent's own
/// session (ADR-0011), whole, under the worker's origin and the entry's
/// journal position — visible where the agent is, and still advancing the
/// worker's watermark like any other stored row.
fn stale_record(
    refusal: &Refusal,
    session_id: &str,
    event: Event,
    current_worker_id: Option<String>,
) -> PendingEvent {
    let reason = match current_worker_id.as_deref() {
        Some(current) => format!(
            "journaled by worker {} after the agent was placed on worker {current}",
            refusal.worker_id
        ),
        None => format!(
            "journaled by worker {} after the agent left it, and it is placed nowhere now",
            refusal.worker_id
        ),
    };
    tracing::info!(
        worker_id = refusal.worker_id,
        journal_sequence = refusal.journal_sequence,
        kind = refusal.kind,
        %reason,
        "a late journal entry was recorded as stale"
    );
    let stale = Event {
        agent_id: event.agent_id.clone(),
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::StaleJournalEvent(Box::new(StaleJournalEvent {
            worker_id: Some(WorkerId::new(refusal.worker_id.as_str())),
            journal_sequence: refusal.journal_sequence,
            current_worker_id: current_worker_id.map(WorkerId::new),
            kind: refusal.kind.to_owned(),
            reason,
            event: Some(Box::new(event)),
        }))),
    };
    refusal.row(session_id, &stale)
}

/// Everything a dead letter records except its reason.
struct Refusal {
    worker_id: String,
    journal_sequence: u64,
    session_id: Option<String>,
    kind: &'static str,
}

impl Refusal {
    /// A server-shaped row standing in for the refused entry: under the
    /// worker's origin and the entry's journal position, so it advances the
    /// worker's watermark like the entry would have, and carries no key.
    fn row(&self, session_id: &str, event: &Event) -> PendingEvent {
        PendingEvent {
            session_id: session_id.to_owned(),
            origin_worker_id: self.worker_id.clone(),
            journal_sequence: Some(self.journal_sequence),
            idempotency_key: String::new(),
            payload: event.encode_to_vec(),
        }
    }
}

/// The row recording a refused journal entry in its place (see the module
/// docs).
fn dead_letter(refusal: &Refusal, reason: String) -> PendingEvent {
    tracing::warn!(
        worker_id = refusal.worker_id,
        journal_sequence = refusal.journal_sequence,
        kind = refusal.kind,
        %reason,
        "a journaled event was refused"
    );
    let event = Event {
        agent_id: None,
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::JournalEventRefused(JournalEventRefused {
            worker_id: Some(WorkerId::new(refusal.worker_id.as_str())),
            journal_sequence: refusal.journal_sequence,
            session_id: refusal.session_id.clone().map(SessionId::new),
            kind: refusal.kind.to_owned(),
            reason,
        })),
    };
    refusal.row(DEAD_LETTER_STREAM, &event)
}

/// Places `agent_id` on `worker_id` in `session_id` through the production
/// placement append — the arrangement every ingest test opens with, shared
/// with the worker spine's suites.
#[cfg(test)]
pub(crate) async fn placed_for_test(
    core: &ServerCore,
    session_id: &str,
    agent_id: &str,
    worker_id: &str,
) {
    use horde_proto::v1::{AgentId, AgentSpawned};
    crate::service::shared::append_server_event(
        &core.log,
        session_id,
        "",
        Event {
            agent_id: Some(AgentId::new(agent_id)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::AgentSpawned(AgentSpawned::default())),
        },
    )
    .await
    .expect("the spawn appends");
    crate::placement::append_placement(
        core,
        session_id,
        crate::placement::wire_placement(
            agent_id,
            &crate::scheduler::Placement::Placed {
                worker_id: worker_id.to_owned(),
            },
        ),
        None,
    )
    .await
    .expect("the placement appends");
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, Event, JournaledEvent, PermissionDecided, PermissionRequest, PermissionRequested,
        SessionId, TurnInterrupted, WorkerId, WorkspaceInstanceChanged, WorkspaceInstanceReport,
        WorkspacePrewarmRequested, event::Kind,
    };
    use prost::Message;

    use super::{DEAD_LETTER_STREAM, OriginView, ingest_batch, placed_for_test};
    use crate::EventLog;
    use crate::service::shared::ServerCore;

    fn core() -> ServerCore {
        ServerCore::new(EventLog::in_memory().expect("in-memory log"))
    }

    fn event(agent: Option<&str>, kind: Kind) -> Event {
        Event {
            agent_id: agent.map(AgentId::new),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(kind),
        }
    }

    fn interrupted(agent: &str) -> Event {
        event(
            Some(agent),
            Kind::TurnInterrupted(TurnInterrupted::default()),
        )
    }

    fn journaled(session: &str, journal_sequence: u64, event: Event) -> JournaledEvent {
        JournaledEvent {
            journal_sequence,
            session_id: Some(SessionId::new(session)),
            event: Some(event),
        }
    }

    /// `kind` about `agent-1`, under the idempotency key `k`.
    fn keyed(kind: Kind) -> Event {
        Event {
            idempotency_key: "k".to_owned(),
            ..event(Some("agent-1"), kind)
        }
    }

    /// A keyed interrupt in `s1` at `journal_sequence` — the write a later
    /// entry under the same key collides with.
    fn keyed_interrupt(journal_sequence: u64) -> JournaledEvent {
        journaled(
            "s1",
            journal_sequence,
            keyed(Kind::TurnInterrupted(TurnInterrupted::default())),
        )
    }

    /// Ingests `entries` as `worker-a`'s batch, which must not fail.
    async fn ingest_from_a(core: &ServerCore, entries: Vec<JournaledEvent>) {
        ingest_batch(core, &mut OriginView::new(), "worker-a", entries)
            .await
            .expect("refusals are dead letters, not batch failures");
    }

    /// The reasons recorded in the dead-letter stream, in order.
    async fn dead_letters(core: &ServerCore) -> Vec<(u64, String)> {
        core.log
            .read_all(DEAD_LETTER_STREAM)
            .await
            .expect("the stream reads")
            .iter()
            .filter_map(|stored| Event::decode(stored.payload.as_slice()).ok())
            .filter_map(|event| match event.kind {
                Some(Kind::JournalEventRefused(refused)) => {
                    Some((refused.journal_sequence, refused.reason))
                }
                _ => None,
            })
            .collect()
    }

    async fn view_of(core: &ServerCore) -> OriginView {
        let mut view = OriginView::new();
        view.catch_up(&core.log).await.expect("catch up");
        view
    }

    #[tokio::test]
    async fn ingestion_translates_journal_positions_into_session_positions() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        placed_for_test(&core, "s2", "agent-2", "worker-a").await;
        let acked = ingest_batch(
            &core,
            &mut OriginView::new(),
            "worker-a",
            vec![
                journaled("s1", 1, interrupted("agent-1")),
                journaled("s1", 2, interrupted("agent-1")),
                journaled("s2", 3, interrupted("agent-2")),
            ],
        )
        .await
        .expect("ingest");

        assert_eq!(acked, 3, "the ack is in the worker's sequence space");
        assert_eq!(core.log.head("s1").await.expect("head"), 4);
        assert_eq!(
            core.log.head("s2").await.expect("head"),
            3,
            "each session numbers independently of the worker's journal"
        );
        assert!(dead_letters(&core).await.is_empty());
    }

    #[tokio::test]
    async fn re_ingesting_a_batch_after_a_reconnect_appends_nothing_new() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        let batch = vec![
            journaled("s1", 1, interrupted("agent-1")),
            journaled("s1", 2, interrupted("agent-1")),
        ];

        ingest_batch(&core, &mut OriginView::new(), "worker-a", batch.clone())
            .await
            .expect("ingest");
        let acked = ingest_batch(&core, &mut OriginView::new(), "worker-a", batch)
            .await
            .expect("re-ingest");

        assert_eq!(acked, 2, "the ack still reflects what is durably stored");
        assert_eq!(
            core.log.head("s1").await.expect("head"),
            4,
            "a replayed journal must not grow the session log"
        );
    }

    /// The wedge this module exists to prevent: a refused entry is recorded
    /// in its place, the ack and the stored watermark both pass it, and the
    /// entries after it in the same batch still land.
    #[tokio::test]
    async fn a_refused_event_is_dead_lettered_and_the_journal_moves_past_it() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        let decided = event(
            Some("agent-1"),
            Kind::PermissionDecided(PermissionDecided::default()),
        );
        let acked = ingest_batch(
            &core,
            &mut OriginView::new(),
            "worker-a",
            vec![
                journaled("s1", 1, decided),
                journaled("s1", 2, interrupted("agent-1")),
            ],
        )
        .await
        .expect("a refusal does not fail the batch");

        assert_eq!(acked, 2);
        assert_eq!(
            core.log
                .worker_journal_watermark("worker-a")
                .await
                .expect("watermark"),
            2,
            "a reconnect must resume after the refused entry, not at it"
        );
        assert_eq!(
            core.log.head("s1").await.expect("head"),
            3,
            "only the admissible entry landed in the session"
        );
        let letters = dead_letters(&core).await;
        assert_eq!(letters.len(), 1, "{letters:?}");
        assert_eq!(letters[0].0, 1);
        assert!(
            letters[0].1.contains("permission_decided"),
            "the reason names the refused kind: {letters:?}"
        );

        // The same poison entry offered again dedupes onto its dead letter.
        ingest_batch(
            &core,
            &mut OriginView::new(),
            "worker-a",
            vec![journaled(
                "s1",
                1,
                event(
                    Some("agent-1"),
                    Kind::PermissionDecided(PermissionDecided::default()),
                ),
            )],
        )
        .await
        .expect("a replay is not an error");
        assert_eq!(dead_letters(&core).await.len(), 1, "one refusal, one row");
    }

    /// Malformed entries dead-letter too: every per-event refusal is
    /// non-wedging, not only the origin ones.
    #[tokio::test]
    async fn malformed_entries_are_dead_lettered_rather_than_failing_the_batch() {
        let core = core();
        let orphan = JournaledEvent {
            journal_sequence: 1,
            session_id: None,
            event: Some(Event::default()),
        };
        let empty = JournaledEvent {
            journal_sequence: 2,
            session_id: Some(SessionId::new("s1")),
            event: None,
        };
        let acked = ingest_batch(
            &core,
            &mut OriginView::new(),
            "worker-a",
            vec![orphan, empty],
        )
        .await
        .expect("malformed entries are refusals, not batch failures");
        assert_eq!(acked, 2);
        let reasons: Vec<String> = dead_letters(&core).await.into_iter().map(|l| l.1).collect();
        assert!(reasons[0].contains("session"), "{reasons:?}");
        assert!(reasons[1].contains("carry an event"), "{reasons:?}");
    }

    /// A key reused for a different kind stores nothing new, so it too is a
    /// dead letter — left as an error it would wedge the watermark.
    #[tokio::test]
    async fn a_kind_collision_is_dead_lettered() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        ingest_from_a(
            &core,
            vec![
                keyed_interrupt(1),
                journaled(
                    "s1",
                    2,
                    keyed(Kind::UsageRecorded(
                        horde_proto::v1::UsageRecorded::default(),
                    )),
                ),
            ],
        )
        .await;
        assert_eq!(
            core.log
                .worker_journal_watermark("worker-a")
                .await
                .expect("watermark"),
            2
        );
        assert_eq!(dead_letters(&core).await.len(), 1);
    }

    /// One batch, one transaction (ADR-0013), in the one-at-a-time order: a
    /// collision's dead letter lands directly behind the write that collided,
    /// ahead of a later entry's refusal; and only an instance report that
    /// actually landed reaches the placement cache.
    #[tokio::test]
    async fn a_batch_keeps_entry_order_and_caches_only_landed_reports() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        let report = |definition: &str| {
            Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged {
                report: Some(WorkspaceInstanceReport {
                    workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                        definition,
                    )),
                    worker_id: Some(WorkerId::new("worker-a")),
                    ..WorkspaceInstanceReport::default()
                }),
                progress: None,
            })
        };
        let orphan = JournaledEvent {
            journal_sequence: 3,
            session_id: None,
            event: Some(Event::default()),
        };
        let mut notifications = core.log.subscribe();

        ingest_from_a(
            &core,
            vec![
                keyed_interrupt(1),
                journaled("s1", 2, keyed(report("collided"))),
                orphan,
                journaled("s1", 4, event(Some("agent-1"), report("landed"))),
            ],
        )
        .await;

        let order: Vec<u64> = dead_letters(&core).await.into_iter().map(|l| l.0).collect();
        assert_eq!(
            order,
            vec![2, 3],
            "the collision's letter precedes entry 3's"
        );
        assert!(core.instances.for_definition("collided").is_empty());
        assert_eq!(core.instances.for_definition("landed").len(), 1);
        let mut announced = 0;
        while notifications.try_recv().is_ok() {
            announced += 1;
        }
        assert_eq!(announced, 4, "two events and two dead letters were new");
    }

    #[tokio::test]
    async fn a_batch_before_its_start_is_refused() {
        let error = ingest_batch(&core(), &mut OriginView::new(), "", Vec::new())
            .await
            .expect_err("a nameless stream cannot journal");
        assert!(matches!(error, crate::error::ServerError::Invalid(_)));
    }

    /// The agent rule's table: placed here and in this session passes; placed
    /// elsewhere, in another session, unplaced, unknown, or unnamed fails.
    #[tokio::test]
    async fn an_agent_event_must_be_about_an_agent_placed_on_the_caller() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        placed_for_test(&core, "s1", "agent-2", "worker-b").await;
        let view = view_of(&core).await;

        assert!(
            view.judge("worker-a", "s1", &interrupted("agent-1"))
                .is_ok()
        );
        assert!(
            view.judge("worker-a", "s1", &interrupted("agent-2"))
                .is_err()
        );
        assert!(
            view.judge("worker-a", "s2", &interrupted("agent-1"))
                .is_err(),
            "the right agent journaled into the wrong session"
        );
        assert!(view.judge("worker-a", "s1", &interrupted("ghost")).is_err());
        let unnamed = event(None, Kind::TurnInterrupted(TurnInterrupted::default()));
        assert!(view.judge("worker-a", "s1", &unnamed).is_err());
        let kindless = Event {
            kind: None,
            ..interrupted("agent-1")
        };
        assert!(view.judge("worker-a", "s1", &kindless).is_err());
    }

    /// A re-queue takes the agent off its worker: what it journals after is
    /// no longer the worker's to say.
    #[tokio::test]
    async fn a_requeued_agent_is_no_longer_held() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        crate::placement::append_placement(
            &core,
            "s1",
            crate::placement::wire_placement(
                "agent-1",
                &crate::scheduler::Placement::Queued {
                    reason: "lost".to_owned(),
                },
            ),
            None,
        )
        .await
        .expect("the re-queue appends");
        let view = view_of(&core).await;
        assert!(
            view.judge("worker-a", "s1", &interrupted("agent-1"))
                .is_err()
        );
    }

    /// Every kind a worker observes is admissible for its placed agent, and
    /// every server decision is refused however it is addressed — the
    /// review's named cases (catalog, decision, enrollment, placement) among
    /// them.
    #[tokio::test]
    async fn server_only_kinds_are_refused_and_observations_admitted() {
        use horde_proto::v1::{
            AgentLifecycleChanged, AgentOutputChunk, AgentSpawned, EnrollmentRevoked,
            McpServerConfigChanged, PlacementChanged, ResultReported, UsageRecorded,
        };
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        let view = view_of(&core).await;

        for kind in [
            Kind::AgentOutputChunk(AgentOutputChunk::default()),
            Kind::AgentLifecycleChanged(AgentLifecycleChanged::default()),
            Kind::ResultReported(ResultReported::default()),
            Kind::TurnInterrupted(TurnInterrupted::default()),
            Kind::UsageRecorded(UsageRecorded::default()),
            Kind::PermissionRequested(PermissionRequested::default()),
        ] {
            let name = horde_proto::kind_field_name(&kind);
            assert!(
                view.judge("worker-a", "s1", &event(Some("agent-1"), kind))
                    .is_ok(),
                "{name:?} is an observation"
            );
        }
        for kind in [
            Kind::PermissionDecided(PermissionDecided::default()),
            Kind::McpServerConfigChanged(McpServerConfigChanged::default()),
            Kind::EnrollmentRevoked(EnrollmentRevoked::default()),
            Kind::PlacementChanged(PlacementChanged::default()),
            Kind::AgentSpawned(AgentSpawned::default()),
        ] {
            let name = horde_proto::kind_field_name(&kind).unwrap_or_default();
            let refused = view
                .judge("worker-a", "s1", &event(Some("agent-1"), kind))
                .expect_err("a server decision is refused");
            assert!(refused.contains(name), "{refused}");
            assert!(
                view.judge(
                    "worker-a",
                    crate::mcp::MCP_CATALOG_STREAM,
                    &event(
                        None,
                        Kind::McpServerConfigChanged(McpServerConfigChanged::default())
                    )
                )
                .is_err()
            );
        }
    }

    /// A permission request must be about the agent that raised it.
    #[tokio::test]
    async fn a_permission_request_cannot_speak_for_another_agent() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        let view = view_of(&core).await;
        let raised = |named: Option<&str>| {
            event(
                Some("agent-1"),
                Kind::PermissionRequested(PermissionRequested {
                    request: Some(PermissionRequest {
                        agent_id: named.map(AgentId::new),
                        ..PermissionRequest::default()
                    }),
                }),
            )
        };
        assert!(
            view.judge("worker-a", "s1", &raised(Some("agent-1")))
                .is_ok()
        );
        assert!(view.judge("worker-a", "s1", &raised(None)).is_ok());
        assert!(view.judge("worker-a", "s1", &raised(Some(""))).is_ok());
        let refused = view
            .judge("worker-a", "s1", &raised(Some("agent-9")))
            .expect_err("a request naming another agent is refused");
        assert!(refused.contains("agent-9"), "{refused}");
    }

    /// An instance change answers a prewarm naming this worker in this
    /// stream — the catalog stream included — or a placed agent; its report
    /// may not name another worker.
    /// ADR-0014's kinds: a checkout's provenance is an agent event; cleanup
    /// and disk pressure land only in the housekeeping stream and only about
    /// the journaling worker; a gc answer must answer a request that named
    /// that worker; and the request itself is the server's to write.
    #[tokio::test]
    async fn workspace_housekeeping_is_admitted_only_from_its_own_worker() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        let housekeeping = horde_proto::WORKSPACE_HOUSEKEEPING_STREAM;
        crate::service::shared::append_server_event(
            &core.log,
            housekeeping,
            "",
            event(
                None,
                Kind::WorkspaceGcRequested(horde_proto::v1::WorkspaceGcRequested {
                    request_id: "gc-1".to_owned(),
                    worker_ids: vec![WorkerId::new("worker-a")],
                    ..horde_proto::v1::WorkspaceGcRequested::default()
                }),
            ),
        )
        .await
        .expect("the request appends");
        let view = view_of(&core).await;

        let prepared = event(
            Some("agent-1"),
            Kind::CheckoutPrepared(horde_proto::v1::CheckoutPrepared::default()),
        );
        assert!(view.judge("worker-a", "s1", &prepared).is_ok());
        assert!(
            view.judge("worker-b", "s1", &prepared).is_err(),
            "not its agent"
        );

        let cleanup = |worker: &str| {
            event(
                None,
                Kind::WorkspaceCleanup(horde_proto::v1::WorkspaceCleanup {
                    worker_id: Some(WorkerId::new(worker)),
                    ..horde_proto::v1::WorkspaceCleanup::default()
                }),
            )
        };
        assert!(
            view.judge("worker-a", housekeeping, &cleanup("worker-a"))
                .is_ok()
        );
        assert!(
            view.judge("worker-a", housekeeping, &cleanup("worker-b"))
                .is_err(),
            "another worker's"
        );
        assert!(
            view.judge("worker-a", "s1", &cleanup("worker-a")).is_err(),
            "not into a session"
        );
        let pressure = event(
            None,
            Kind::WorkspaceDiskPressure(horde_proto::v1::WorkspaceDiskPressure {
                worker_id: Some(WorkerId::new("worker-a")),
                ..horde_proto::v1::WorkspaceDiskPressure::default()
            }),
        );
        assert!(view.judge("worker-a", housekeeping, &pressure).is_ok());
        assert!(view.judge("worker-b", housekeeping, &pressure).is_err());

        let completed = |request: &str| {
            event(
                None,
                Kind::WorkspaceGcCompleted(horde_proto::v1::WorkspaceGcCompleted {
                    request_id: request.to_owned(),
                    worker_id: Some(WorkerId::new("worker-a")),
                    ..horde_proto::v1::WorkspaceGcCompleted::default()
                }),
            )
        };
        assert!(
            view.judge("worker-a", housekeeping, &completed("gc-1"))
                .is_ok()
        );
        assert!(
            view.judge("worker-a", housekeeping, &completed("gc-ghost"))
                .is_err(),
            "no request asked"
        );
        let missing = event(
            None,
            Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged {
                report: Some(WorkspaceInstanceReport {
                    worker_id: Some(WorkerId::new("worker-a")),
                    ..WorkspaceInstanceReport::default()
                }),
                progress: None,
            }),
        );
        assert!(
            view.judge("worker-a", housekeeping, &missing).is_ok(),
            "a removal's MISSING report answers the request that targeted it"
        );
        let forged = event(
            None,
            Kind::WorkspaceGcRequested(horde_proto::v1::WorkspaceGcRequested::default()),
        );
        assert!(
            view.judge("worker-a", housekeeping, &forged).is_err(),
            "server-written only"
        );
    }

    #[tokio::test]
    async fn an_instance_change_must_answer_a_prewarm_or_a_placed_agent() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        crate::service::shared::append_server_event(
            &core.log,
            crate::workspace::CATALOG_STREAM,
            "",
            event(
                None,
                Kind::WorkspacePrewarmRequested(WorkspacePrewarmRequested {
                    workspace_definition_id: None,
                    worker_id: Some(WorkerId::new("worker-a")),
                }),
            ),
        )
        .await
        .expect("the prewarm appends");
        let view = view_of(&core).await;
        let changed = |agent: Option<&str>, named: Option<&str>| {
            event(
                agent,
                Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged {
                    report: Some(WorkspaceInstanceReport {
                        worker_id: named.map(WorkerId::new),
                        ..WorkspaceInstanceReport::default()
                    }),
                    progress: None,
                }),
            )
        };
        let catalog = crate::workspace::CATALOG_STREAM;

        assert!(
            view.judge("worker-a", catalog, &changed(None, Some("worker-a")))
                .is_ok()
        );
        assert!(
            view.judge("worker-a", catalog, &changed(None, None))
                .is_ok()
        );
        assert!(
            view.judge("worker-b", catalog, &changed(None, None))
                .is_err(),
            "the prewarm named another worker"
        );
        assert!(
            view.judge("worker-a", "s1", &changed(None, None)).is_err(),
            "no prewarm in this session"
        );
        assert!(
            view.judge("worker-a", "s1", &changed(Some("agent-1"), None))
                .is_ok()
        );
        assert!(
            view.judge("worker-a", "s1", &changed(Some("ghost"), None))
                .is_err()
        );
        let foreign = view
            .judge("worker-a", catalog, &changed(None, Some("worker-b")))
            .expect_err("a report naming another worker is refused");
        assert!(foreign.contains("worker-b"), "{foreign}");
        let no_report = event(
            None,
            Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged::default()),
        );
        assert!(view.judge("worker-a", catalog, &no_report).is_ok());
    }

    #[test]
    fn an_undecodable_row_folds_to_nothing() {
        let mut view = OriginView::new();
        view.fold(&crate::log::GlobalEvent {
            row: 1,
            event: crate::log::StoredEvent {
                session_id: "s1".to_owned(),
                sequence: 1,
                recorded_at_unix_ms: 0,
                payload: vec![0xff, 0xff, 0xff],
            },
            origin_worker_id: String::new(),
        });
        assert!(view.agents.is_empty() && view.prewarmed.is_empty());
    }

    /// A plaintext journal has no certified origin to judge, so it judges
    /// nothing — but a malformed entry from it still dead-letters.
    #[tokio::test]
    async fn an_ambient_view_judges_nothing_but_malformed_entries_still_dead_letter() {
        let core = core();
        let mut view = OriginView::ambient();
        view.catch_up(&core.log).await.expect("catch up");
        assert!(
            view.judge(
                "anyone",
                "s1",
                &event(
                    Some("ghost"),
                    Kind::PermissionDecided(PermissionDecided::default())
                )
            )
            .is_ok()
        );
        let orphan = JournaledEvent {
            journal_sequence: 1,
            session_id: None,
            event: Some(interrupted("ghost")),
        };
        ingest_batch(&core, &mut view, "anyone", vec![orphan])
            .await
            .expect("a refusal is not a batch failure");
        assert_eq!(dead_letters(&core).await.len(), 1);
    }

    #[test]
    fn the_dead_letter_stream_is_reserved() {
        assert!(super::is_dead_letter_stream(DEAD_LETTER_STREAM));
        assert!(!super::is_dead_letter_stream("session-1"));
        assert!(crate::enrollment::is_reserved_stream(DEAD_LETTER_STREAM));
    }

    /// Re-places `agent-1` from `worker-a` onto `worker-b`.
    async fn re_placed_onto_b(core: &ServerCore) {
        placed_for_test(core, "s1", "agent-1", "worker-a").await;
        crate::placement::append_placement(
            core,
            "s1",
            crate::placement::wire_placement(
                "agent-1",
                &crate::scheduler::Placement::Placed {
                    worker_id: "worker-b".to_owned(),
                },
            ),
            None,
        )
        .await
        .expect("the re-placement appends");
    }

    /// The `StaleJournalEvent`s in `s1`: (journaling worker, kind, current
    /// worker, whether the original rode along).
    async fn stale_in_s1(core: &ServerCore) -> Vec<(String, String, String, bool)> {
        core.log
            .read_all("s1")
            .await
            .expect("the session reads")
            .iter()
            .filter_map(|stored| Event::decode(stored.payload.as_slice()).ok())
            .filter_map(|event| match event.kind {
                Some(Kind::StaleJournalEvent(stale)) => Some((
                    stale.worker_id.map(|id| id.to_string()).unwrap_or_default(),
                    stale.kind,
                    stale
                        .current_worker_id
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                    stale.event.is_some(),
                )),
                _ => None,
            })
            .collect()
    }

    /// A late journal from the agent's former placement (ADR-0011): history
    /// lands as itself, anything that would speak for the agent now is
    /// recorded whole and visibly in the agent's session, and a worker that
    /// never held the agent is still a stranger whose entry dead-letters.
    #[tokio::test]
    async fn a_former_placements_late_journal_lands_as_history_or_is_recorded_stale() {
        use horde_proto::v1::{AgentLifecycleChanged, AgentOutputChunk};
        let core = core();
        re_placed_onto_b(&core).await;
        let before = core.log.read_all("s1").await.expect("read").len();

        let mut view = OriginView::new();
        ingest_batch(
            &core,
            &mut view,
            "worker-a",
            vec![
                journaled(
                    "s1",
                    1,
                    event(
                        Some("agent-1"),
                        Kind::AgentOutputChunk(AgentOutputChunk::default()),
                    ),
                ),
                journaled(
                    "s1",
                    2,
                    event(
                        Some("agent-1"),
                        Kind::AgentLifecycleChanged(AgentLifecycleChanged::default()),
                    ),
                ),
            ],
        )
        .await
        .expect("late entries are not a batch failure");
        ingest_batch(
            &core,
            &mut OriginView::new(),
            "worker-c",
            vec![journaled("s1", 1, interrupted("agent-1"))],
        )
        .await
        .expect("a stranger's refusal is not a batch failure");

        assert_eq!(
            core.log.read_all("s1").await.expect("read").len(),
            before + 2,
            "the output landed and the lifecycle was recorded; the stranger added nothing here"
        );
        assert_eq!(
            stale_in_s1(&core).await,
            vec![(
                "worker-a".to_owned(),
                "agent_lifecycle_changed".to_owned(),
                "worker-b".to_owned(),
                true
            )]
        );
        assert_eq!(
            dead_letters(&core).await.len(),
            1,
            "only the stranger dead-letters"
        );
        assert_eq!(
            core.log
                .worker_journal_watermark("worker-a")
                .await
                .expect("read"),
            2,
            "the stale record still advances the worker's watermark"
        );
    }

    /// A worker the agent comes back to is current again, not late.
    #[tokio::test]
    async fn returning_to_a_former_worker_makes_it_current_again() {
        let core = core();
        re_placed_onto_b(&core).await;
        crate::placement::append_placement(
            &core,
            "s1",
            crate::placement::wire_placement(
                "agent-1",
                &crate::scheduler::Placement::Placed {
                    worker_id: "worker-a".to_owned(),
                },
            ),
            None,
        )
        .await
        .expect("the return appends");
        let view = view_of(&core).await;
        assert_eq!(
            view.lateness("worker-a", "s1", &interrupted("agent-1")),
            None
        );
        assert_eq!(
            view.lateness("worker-b", "s1", &interrupted("agent-1")),
            Some(super::Late::Record {
                current_worker_id: Some("worker-a".to_owned())
            }),
            "and the worker it left is now the former one"
        );
        assert_eq!(
            view.lateness("worker-b", "other-session", &interrupted("agent-1")),
            None,
            "a late entry must name the agent's own session"
        );
        assert_eq!(
            OriginView::ambient().lateness("worker-b", "s1", &interrupted("agent-1")),
            None
        );
    }

    #[test]
    fn history_lands_and_what_speaks_for_the_agent_is_recorded() {
        use horde_proto::v1::{
            AgentLifecycleChanged, AgentOutputChunk, ResultReported, UsageRecorded,
        };
        let current = || Some("w2".to_owned());
        let record = Some(super::Late::Record {
            current_worker_id: current(),
        });
        for kind in [
            Kind::AgentOutputChunk(AgentOutputChunk::default()),
            Kind::UsageRecorded(UsageRecorded::default()),
        ] {
            assert_eq!(
                super::late_handling(&kind, current()),
                Some(super::Late::Land)
            );
        }
        for kind in [
            Kind::AgentLifecycleChanged(AgentLifecycleChanged::default()),
            Kind::PermissionRequested(PermissionRequested::default()),
            Kind::ResultReported(ResultReported::default()),
            Kind::TurnInterrupted(TurnInterrupted::default()),
            Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged::default()),
        ] {
            assert_eq!(super::late_handling(&kind, current()), record);
        }
        assert_eq!(
            super::late_handling(
                &Kind::PermissionDecided(PermissionDecided::default()),
                current()
            ),
            None,
            "a server-only kind is not late, it is refused"
        );
    }

    /// After a reset (ADR-0011) the worker's fresh journal is stored above
    /// the floor: journal position 1 is a new row, not a duplicate of the
    /// lost journal's first.
    #[tokio::test]
    async fn after_a_reset_the_fresh_journal_lands_above_the_floor() {
        let core = core();
        placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        let mut view = OriginView::new();
        ingest_batch(
            &core,
            &mut view,
            "worker-a",
            vec![journaled("s1", 1, interrupted("agent-1"))],
        )
        .await
        .expect("ingest");
        crate::liveness::reset_worker_journal(&core, "worker-a", "disk", None)
            .await
            .expect("reset");
        placed_for_test(&core, "s1", "agent-2", "worker-a").await;

        let acked = ingest_batch(
            &core,
            &mut view,
            "worker-a",
            vec![journaled("s1", 1, interrupted("agent-2"))],
        )
        .await
        .expect("ingest");
        assert_eq!(acked, 1, "acks speak the worker's own numbering");
        assert_eq!(view.floor("worker-a"), 1);
        assert_eq!(
            core.log
                .worker_journal_watermark("worker-a")
                .await
                .expect("read"),
            2,
            "stored above the floor, not deduplicated onto the lost row"
        );
    }
}
