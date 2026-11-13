//! Applying commands: accept, dispatch to per-agent lanes, and the pumps.
//!
//! The Runtime is the [`ApplyCommand`](crate::client::ApplyCommand) half of
//! the Worker: the command feed hands it wire commands, and it turns them into
//! adapter calls, journal entries, and running agents.
//!
//! # Accept, then run (ADR-0012)
//!
//! Applying a command is two transactions. [`ApplyCommand::apply`] only
//! *accepts* it — dedupe on `command_id`, both ack watermarks, and the
//! command itself into the journaled [inbox](crate::journal::InboxDisposition) — and hands it to
//! the [lane](lane) its target names: one per agent, one per workspace. The
//! lane runs it in order behind everything else that agent was sent, and
//! *completes* it — effects, bookkeeping, and the inbox row — in a second
//! transaction. So a slow spawn holds up its own agent and nobody else's, a
//! crash between the two loses nothing (the next start replays the inbox),
//! and the at-least-once rule still holds: a redelivered command is
//! recognized at acceptance, before anything executes, because the side
//! effect of applying a spawn is a live process.
//!
//! One ordering crosses lanes: a spawn naming a workspace waits on a fence
//! through that workspace's lane, so a materialize dispatched before it
//! finishes first — exactly the order the one sequential loop used to give.
//!
//! # What a command journals, and what it deliberately does not
//!
//! Every command's *causing* event is already in the Session log — that is
//! what the Server's command stream is folded from — so the Runtime journals
//! only what the Worker itself observes: lifecycle transitions it causes
//! (starting, archived, failed), interruptions it performs, and diagnostics
//! for commands it could not apply. Message deliveries and permission
//! decisions journal nothing here; the Server wrote `AssignmentSent` /
//! `PermissionDecided` when they happened, and a second copy from the Worker
//! would double every one of them in every client.

mod access;
mod housekeeping;
mod lane;
mod pump;
mod shape;
mod shared;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use horde_adapter::Adapter;
use horde_proto::v1::StreamCommandsResponse;
use horde_proto::v1::stream_commands_response::Command;
use tokio::sync::oneshot;

use crate::client::ApplyCommand;
use crate::error::WorkerError;
use crate::journal::{CommandApplication, Journal};
use crate::share::{ShareBroker, StampingSink};
pub use housekeeping::{Activity, HousekeepingConfig, InstanceReporter, Watermark};
use lane::{LaneHandle, LaneMessage, Ledger, lane_key, spawn_lane, workspace_lane};
use shared::{Shared, workspace_of};

/// The Worker's command-applying half: adapters in, running agents out.
pub struct Runtime {
    shared: Shared,
    lanes: BTreeMap<String, LaneHandle>,
    ledger: Ledger,
    /// Pumps reaped by lanes that retired themselves, owed to shutdown's
    /// count.
    reaped: usize,
    /// Whether the inbox left by a previous run has been replayed yet.
    replayed: bool,
    /// The scan loop's settings, when workspaces are enabled.
    housekeeping: HousekeepingConfig,
    /// The running scan loop, started with the first command.
    scan: Option<tokio::task::JoinHandle<()>>,
    /// The disk-pressure episode, shared by the loop and manual scans so
    /// "one exhausted event per episode" holds across both.
    episode: Arc<tokio::sync::Mutex<housekeeping::Episode>>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Runtime")
            .field("adapters", &self.shared.adapters.keys().collect::<Vec<_>>())
            .field("agents", &*self.ledger.running())
            .finish_non_exhaustive()
    }
}

impl Runtime {
    #[must_use]
    pub fn new(journal: Journal) -> Self {
        Self {
            shared: Shared::new(journal),
            lanes: BTreeMap::new(),
            ledger: Ledger::new(),
            reaped: 0,
            replayed: false,
            housekeeping: HousekeepingConfig::default(),
            scan: None,
            episode: Arc::new(tokio::sync::Mutex::new(housekeeping::Episode::default())),
        }
    }

    /// Where the Instance set is reported after every materialize, cleanup
    /// and scan that changed anything (ADR-0014).
    #[must_use]
    pub fn with_instance_reporter(mut self, reporter: Arc<dyn InstanceReporter>) -> Self {
        self.shared.reporter = Some(reporter);
        self
    }

    /// The workspace scan's machine-local settings.
    #[must_use]
    pub fn with_housekeeping(mut self, config: HousekeepingConfig) -> Self {
        self.housekeeping = config;
        self
    }

    /// Wires what needs the whole Runtime built: the endpoint's workspace
    /// verbs and the scan loop. Idempotent; run before the first command.
    fn finalize(&mut self) {
        if self.scan.is_some() {
            return;
        }
        let Some(housekeeper) = self.shared.housekeeper() else {
            return;
        };
        if let Some(endpoints) = &self.shared.endpoints {
            endpoints.set_workspace_access(Arc::new(access::WorkerWorkspaceAccess {
                shared: self.shared.clone(),
            }));
        }
        self.scan = Some(tokio::spawn(housekeeping::run(
            housekeeper,
            self.housekeeping,
            Arc::clone(&self.episode),
        )));
    }

    /// Runs one workspace scan now — what the scan loop does every
    /// interval — returning whether it changed anything. For tests and
    /// operators who will not wait a minute.
    pub async fn scan_workspaces(&self) -> bool {
        let Some(housekeeper) = self.shared.housekeeper() else {
            return false;
        };
        let mut episode = self.episode.lock().await;
        let changed = housekeeper.scan(&self.housekeeping, &mut episode).await;
        drop(episode);
        housekeeper.report().await;
        changed
    }

    /// Which agents have a turn in flight — the busy signal soft reclaim
    /// waits on.
    #[must_use]
    pub fn activity(&self) -> Arc<Activity> {
        Arc::clone(&self.shared.activity)
    }

    /// Enables the per-agent MCP endpoint (DESIGN §5): every spawned or
    /// revived agent gets its Session's loopback endpoint and a freshly
    /// minted identity pair. `channel` is the Worker's authenticated channel
    /// to the Server — the orchestrator verbs issue their RPCs over it, and
    /// the endpoint's hub link forwards hub tool calls over it too
    /// (ADR-0004's 2026-08-19 amendment), attributed under `worker_id`.
    #[must_use]
    pub fn with_agent_endpoints(
        mut self,
        channel: tonic::transport::Channel,
        worker_id: &str,
    ) -> Self {
        self.shared.endpoints = Some(Arc::new(crate::endpoint::EndpointManager::new(
            channel, worker_id,
        )));
        self
    }

    /// Points attachment transfer at the Server's blob store, over the
    /// Worker's own authenticated channel. Without one, inline attachments
    /// still work in both directions and anything needing the store refuses
    /// honestly.
    #[must_use]
    pub fn with_blob_service(mut self, channel: tonic::transport::Channel) -> Self {
        self.shared.attachments = Arc::new(crate::blob::AttachmentBroker::new(Some(channel)));
        self
    }

    /// Points payload shares at a Relay: stamped commits are pushed to their
    /// share refs, and commits absent from this Worker are fetched through
    /// it. Without one, only the local routes exist. Configure before any
    /// spawn — the broker is rebuilt, not updated.
    #[must_use]
    pub fn with_relay(mut self, relay: horde_relay::RelayLocation) -> Self {
        self.shared.broker = Arc::new(ShareBroker::new(Some(relay)));
        self
    }

    /// The journal sink the Worker hands its MCP endpoint: results reported
    /// without a payload are stamped with the reporting agent's observed
    /// HEAD on their way into the journal ("the Worker's to stamp",
    /// horde-mcp's `report_result`).
    #[must_use]
    pub fn stamping_sink(&self) -> StampingSink {
        self.shared.stamping_sink()
    }

    /// Enables workspace support: materialize commands are applied, spawns
    /// naming a workspace get a checkout, and the advertisement says so.
    #[must_use]
    pub fn with_workspaces(mut self, workspaces: crate::workspace::WorkspaceManager) -> Self {
        self.shared.workspaces = Some(workspaces);
        self
    }

    /// The workspace manager, when this Worker has one — the advertisement
    /// builder reads warmth and support from it.
    #[must_use]
    pub fn workspaces(&self) -> Option<&crate::workspace::WorkspaceManager> {
        self.shared.workspaces.as_ref()
    }

    /// Registers an adapter under its provider key. The first registered
    /// adapter becomes the default for spawn commands that name no provider.
    #[must_use]
    pub fn with_adapter(mut self, adapter: Arc<dyn Adapter>) -> Self {
        let provider = adapter.provider().to_owned();
        if self.shared.default_provider.is_none() {
            self.shared.default_provider = Some(provider.clone());
        }
        self.shared.adapters.insert(provider, adapter);
        self
    }

    /// The registered adapters, for building the capability advertisement.
    pub fn adapters(&self) -> impl Iterator<Item = &dyn Adapter> {
        self.shared.adapters.values().map(AsRef::as_ref)
    }

    /// How many agents this Runtime is currently running.
    #[must_use]
    pub fn running_agents(&self) -> usize {
        self.ledger.running().len()
    }

    /// Resolves once every command accepted so far has completed — spawned
    /// harnesses started, deliveries handed over, effects journaled. What a
    /// caller waits on before reading state a command's follow-up sets.
    pub async fn settled(&self) {
        let mut in_flight = self.ledger.in_flight.subscribe();
        // The sender lives in `self`; the wait cannot outlive it.
        let _ = in_flight.wait_for(|count| *count == 0).await;
    }

    /// Replays what a previous run accepted and never completed (ADR-0012):
    /// every inbox row runs again as the command it was, in acceptance
    /// order, on its own lane. Idempotent — only the first call replays —
    /// and also done by the first [`ApplyCommand::apply`], so no caller can
    /// run new commands ahead of old ones. Returns how many it replayed.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] or [`WorkerError::CorruptInboxEntry`] when
    /// the inbox cannot be read — worker-wide, since commands it holds would
    /// otherwise run out of order or never.
    pub async fn replay_inbox(&mut self) -> Result<usize, WorkerError> {
        if self.replayed {
            return Ok(0);
        }
        self.finalize();
        let entries = self.shared.journal.replay_inbox().await?;
        self.replayed = true;
        let replayed = entries.len();
        for entry in entries {
            self.dispatch(entry.lane, entry.command);
        }
        Ok(replayed)
    }

    /// Ends every agent and waits for their pumps to finish, returning how
    /// many pumps were reaped (running and retired alike).
    ///
    /// Bounded by structure: dropping a lane's sender ends it after the
    /// command it is running, and each pump's shutdown is a kill-by-handle,
    /// not a wait-for-exit. The count is returned rather than logged because
    /// "shutdown finished" and "shutdown actually reaped the agents" are
    /// different facts, and only the second one is evidence.
    pub async fn shutdown(mut self) -> usize {
        if let Some(scan) = self.scan.take() {
            scan.abort();
        }
        if let Some(endpoints) = &self.shared.endpoints {
            endpoints.shutdown().await;
        }
        let mut reaped = self.reaped;
        for lane in std::mem::take(&mut self.lanes).into_values() {
            drop(lane.sender);
            match lane.task.await {
                Ok(count) => reaped += count,
                Err(error) => tracing::warn!(%error, "a lane ended abnormally during shutdown"),
            }
        }
        reaped
    }

    /// Hands an accepted command to its lane, fenced behind its workspace's
    /// materializations when it is a spawn naming one.
    fn dispatch(&mut self, lane: String, command: StreamCommandsResponse) {
        let after = self.fence_for(&command);
        self.ledger.dispatched();
        self.send(
            lane,
            LaneMessage::Command {
                command: Box::new(command),
                after,
            },
        );
    }

    /// A fence through the lane of the workspace a spawn names, when that
    /// lane exists: it resolves once every materialization dispatched there
    /// before this spawn has run.
    fn fence_for(&mut self, command: &StreamCommandsResponse) -> Option<oneshot::Receiver<()>> {
        let Some(Command::SpawnAgent(spawn)) = command.command.as_ref() else {
            return None;
        };
        let lane = self.lanes.get(&workspace_lane(&workspace_of(spawn)?))?;
        let (done, after) = oneshot::channel();
        lane.sender.send(LaneMessage::Fence(done)).ok()?;
        Some(after)
    }

    /// Sends to `key`'s lane, starting one when there is none — or when the
    /// one there retired itself, in which case the new lane joins it first.
    fn send(&mut self, key: String, message: LaneMessage) {
        let message = match self.lanes.get(&key) {
            Some(lane) => match lane.sender.send(message) {
                Ok(()) => return,
                Err(refused) => refused.0,
            },
            None => message,
        };
        let predecessor = self.lanes.remove(&key).map(|lane| lane.task);
        let lane = spawn_lane(
            key.clone(),
            self.shared.clone(),
            self.ledger.clone(),
            predecessor,
        );
        // A fresh lane's receiver is alive until it reads this.
        let _ = lane.sender.send(message);
        self.lanes.insert(key, lane);
    }

    /// Drops lanes that retired themselves, keeping their reaped counts — so
    /// the map holds live agents and workspaces, not every agent this Worker
    /// ever ran.
    async fn prune_retired_lanes(&mut self) {
        let finished: Vec<String> = self
            .lanes
            .iter()
            .filter(|(_, lane)| lane.task.is_finished())
            .map(|(key, _)| key.clone())
            .collect();
        for key in finished {
            if let Some(lane) = self.lanes.remove(&key) {
                // Finished, so this resolves at once.
                self.reaped += lane.task.await.unwrap_or_default();
            }
        }
    }
}

#[async_trait]
impl ApplyCommand for Runtime {
    async fn apply(&mut self, command: StreamCommandsResponse) -> Result<(), WorkerError> {
        self.replay_inbox().await?;
        self.prune_retired_lanes().await;
        let lane = lane_key(&command);
        // At-least-once: a redelivery is recognized before anything runs,
        // because running a spawn twice is two harness processes.
        if self.shared.journal.accept_command(&command, &lane).await?
            == CommandApplication::AlreadyApplied
        {
            tracing::debug!(
                command_id = %command.command_id,
                "command redelivered; already applied"
            );
            return Ok(());
        }
        self.dispatch(lane, command);
        Ok(())
    }
}
