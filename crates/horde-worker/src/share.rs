//! Payload stamping and share execution — the Worker's half of "sharing is
//! implicit in the verbs" (DESIGN §5).
//!
//! An Agent reports a status and a summary; it does **not** report its
//! commits. `horde-mcp`'s `report_result` leaves `AgentResult.payload` unset
//! on purpose — "the Worker's to stamp" — and this module is the other side
//! of that seam: [`StampingSink`] observes the reporting Agent's checkout
//! (`observe_head`) and stamps what it *sees* into the result, because
//! observed state does not drift and a declared payload can.
//!
//! The receiving side is here too: when a delivered Assignment or Note
//! carries a Payload, [`ShareBroker::obtain`] resolves how each commit
//! travels — already shared through a common object store, fetched from a
//! peer checkout on this Worker, or fetched through the Relay — and makes it
//! present in the receiving Agent's checkout before the Agent reads the
//! message. Transport is never the Agent's concern (DESIGN §8).
//!
//! # Staging pushes to the Relay before any receiver exists
//!
//! Routes are resolved per sharing *pair*, but a result is stamped before
//! anyone knows who will read it. So staging takes the only decision that is
//! safe under that ignorance: with a Relay configured, every stamped commit
//! is pushed to its content-addressed share ref, making it obtainable from
//! anywhere; without one, staging still proves the sender has what the
//! Payload names. The Relay doubling as a clone cache is designed-in
//! (DESIGN §8), so the extra push is the mirror doing its job, not waste.
//! The receiver-side resolution then prefers the cheap local routes and only
//! falls back to the Relay when the commit is nowhere on this Worker.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use horde_proto::v1::event::Kind;
use horde_proto::v1::{GitRef, Payload};
use horde_relay::{
    CheckoutView, RelayLocation, ShareRoute, observe_head, obtain_share, resolve_route, stage_share,
};

use crate::error::WorkerError;
use crate::journal::{JournalEntry, JournalSink};

/// Where one Agent's repos are checked out, as the spawn composed them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCheckout {
    /// The agent-private root its working directory points at.
    pub root: PathBuf,
    /// `(repo name, relative directory under the root)`, from the Workspace
    /// Definition — the names Payload `GitRef`s carry.
    pub repos: Vec<(String, PathBuf)>,
}

impl AgentCheckout {
    /// The on-disk path of one repo of this checkout, by Definition name.
    fn repo_path(&self, repo_name: &str) -> Option<PathBuf> {
        self.repos
            .iter()
            .find(|(name, _)| name == repo_name)
            .map(|(_, relative)| self.root.join(relative))
    }
}

/// Knows every local Agent's checkout and executes shares between them.
///
/// One per Runtime. Checkouts register at spawn and unregister at archive;
/// the lock is a plain mutex because nothing holds it across an await — data
/// is cloned out, then git runs.
#[derive(Debug)]
pub struct ShareBroker {
    relay: Option<RelayLocation>,
    checkouts: Mutex<BTreeMap<String, AgentCheckout>>,
}

impl ShareBroker {
    #[must_use]
    pub fn new(relay: Option<RelayLocation>) -> Self {
        Self {
            relay,
            checkouts: Mutex::new(BTreeMap::new()),
        }
    }

    /// Records where a spawned Agent's repos live.
    pub fn register(&self, agent_id: &str, checkout: AgentCheckout) {
        self.lock().insert(agent_id.to_owned(), checkout);
    }

    /// Forgets an archived Agent's checkout.
    pub fn forget(&self, agent_id: &str) {
        self.lock().remove(agent_id);
    }

    /// One Agent's checkout, if it has one.
    #[must_use]
    pub fn checkout_of(&self, agent_id: &str) -> Option<AgentCheckout> {
        self.lock().get(agent_id).cloned()
    }

    /// Every other Agent's path for `repo_name` — the peer candidates a
    /// receiver may fetch from without leaving this Worker.
    fn peer_paths(&self, repo_name: &str, excluding_agent: &str) -> Vec<PathBuf> {
        self.lock()
            .iter()
            .filter(|(agent_id, _)| agent_id.as_str() != excluding_agent)
            .filter_map(|(_, checkout)| checkout.repo_path(repo_name))
            .collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, AgentCheckout>> {
        // A poisoned lock means a panic while registering; the map itself is
        // still coherent (single insert/remove operations).
        self.checkouts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Observes an Agent's checkout and builds the Payload its result
    /// carries: one `GitRef` per repo, commit id authoritative, branch
    /// advisory. `None` when the Agent has no checkout — an agent running
    /// where the Worker runs has no repos to observe, and stamping nothing
    /// is the honest answer.
    ///
    /// Staging runs per repo: with a Relay, the commit is pushed to its
    /// share ref so any receiver anywhere can obtain it; without one, the
    /// stage still proves the commit exists where the Payload says.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Share`] when observing or staging fails — a result
    /// stamped with commits nobody can fetch would be worse than the error.
    pub async fn stamp(&self, agent_id: &str) -> Result<Option<Payload>, WorkerError> {
        let Some(checkout) = self.checkout_of(agent_id) else {
            return Ok(None);
        };

        let mut git_refs = Vec::new();
        for (repo_name, relative) in &checkout.repos {
            let repo = checkout.root.join(relative);
            let observed = observe_head(&repo)
                .await
                .map_err(horde_relay::ShareError::from)?;

            let route = match &self.relay {
                Some(_) => ShareRoute::ViaRelay {
                    repo_name: repo_name.clone(),
                },
                // No Relay: local routes only. Staging still verifies the
                // sender holds the commit it is advertising.
                None => ShareRoute::AlreadyShared,
            };
            stage_share(&repo, &route, self.relay.as_ref(), &observed.commit_sha).await?;

            git_refs.push(GitRef {
                repo_name: repo_name.clone(),
                commit_sha: observed.commit_sha,
                branch: observed.branch.unwrap_or_default(),
            });
        }

        Ok(Some(Payload {
            git_refs,
            attachments: Vec::new(),
        }))
    }

    /// Makes every commit a Payload names present in the receiving Agent's
    /// checkout, resolving the route per repo: already present wins, a peer
    /// checkout on this Worker is next, and the Relay is the only route that
    /// crosses a machine boundary.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Workspace`] when the receiver has no checkout of a
    /// named repo, and [`WorkerError::Share`] when no route can deliver the
    /// commit — including [`ShareError::NoRelay`](horde_relay::ShareError)
    /// when the commit is nowhere on this Worker and no Relay is configured.
    pub async fn obtain(&self, agent_id: &str, payload: &Payload) -> Result<(), WorkerError> {
        for git_ref in &payload.git_refs {
            let receiver = self
                .checkout_of(agent_id)
                .and_then(|checkout| checkout.repo_path(&git_ref.repo_name))
                .ok_or_else(|| {
                    WorkerError::Workspace(format!(
                        "agent {agent_id} has no checkout of repo {} to receive \
                         commit {} into",
                        git_ref.repo_name, git_ref.commit_sha
                    ))
                })?;

            self.obtain_one(agent_id, git_ref, &receiver).await?;
        }
        Ok(())
    }

    /// Delivers one commit into one receiving repo, cheapest route first.
    async fn obtain_one(
        &self,
        agent_id: &str,
        git_ref: &GitRef,
        receiver: &std::path::Path,
    ) -> Result<(), WorkerError> {
        // Already present — worktrees of one clone, or a re-delivered
        // message. `AlreadyShared` verifies rather than trusts.
        if obtain_share(
            receiver,
            &ShareRoute::AlreadyShared,
            None,
            &git_ref.commit_sha,
        )
        .await
        .is_ok()
        {
            return Ok(());
        }

        // A peer checkout on this Worker that provably holds the commit:
        // resolve the route for the pair — the observation may still say
        // "already shared" (a common object store the first probe could not
        // see through a fetch-less verify is impossible, but the resolver is
        // the authority) — and execute it.
        for peer in self.peer_paths(&git_ref.repo_name, agent_id) {
            if stage_share(&peer, &ShareRoute::AlreadyShared, None, &git_ref.commit_sha)
                .await
                .is_err()
            {
                continue;
            }
            let sender_view = CheckoutView::observe("local", &peer)
                .await
                .map_err(horde_relay::ShareError::from)?;
            let receiver_view = CheckoutView::observe("local", receiver)
                .await
                .map_err(horde_relay::ShareError::from)?;
            let route = resolve_route(&git_ref.repo_name, &sender_view, &receiver_view);
            obtain_share(receiver, &route, None, &git_ref.commit_sha).await?;
            return Ok(());
        }

        // Nowhere on this Worker: the Relay is the only remaining route, and
        // `obtain_share` says `NoRelay` honestly when none is configured.
        let route = ShareRoute::ViaRelay {
            repo_name: git_ref.repo_name.clone(),
        };
        obtain_share(receiver, &route, self.relay.as_ref(), &git_ref.commit_sha).await?;
        Ok(())
    }
}

/// Whether this event is a result the Worker owes a payload: a
/// `ResultReported` whose result arrived with none. A result already
/// carrying one is left alone — re-stamping a redelivery could observe a
/// *later* HEAD than the report meant.
#[must_use]
pub fn needs_stamp(kind: &Kind) -> bool {
    matches!(
        kind,
        Kind::ResultReported(reported)
            if reported.result.as_ref().is_some_and(|result| result.payload.is_none())
    )
}

/// The same event, with the observed payload stamped into its result.
/// Kinds that owe no payload pass through untouched.
#[must_use]
pub fn stamped(kind: Kind, payload: Payload) -> Kind {
    match kind {
        Kind::ResultReported(mut reported) => {
            if let Some(result) = reported.result.as_mut() {
                result.payload = Some(payload);
            }
            Kind::ResultReported(reported)
        }
        other => other,
    }
}

/// The journal sink the Worker hands its MCP endpoint: every entry lands in
/// the journal, and a result reported without a payload is stamped with the
/// reporting Agent's observed HEAD on the way through.
///
/// This is the seam `horde-mcp`'s `report_result` writes into ("the Worker's
/// to stamp"): same `record` shape as the endpoint's `JournalSink` trait, so
/// wiring the endpoint into the Worker is an adapter, not a redesign.
#[derive(Debug, Clone)]
pub struct StampingSink {
    sink: JournalSink,
    broker: Arc<ShareBroker>,
}

impl StampingSink {
    #[must_use]
    pub fn new(sink: JournalSink, broker: Arc<ShareBroker>) -> Self {
        Self { sink, broker }
    }

    /// Journals one event, stamping a payload-less result first.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Share`] when the stamp fails — the result is *not*
    /// journaled then, because acking a result whose commits cannot be
    /// fetched is the acked-but-discarded shape QUALITY_GATES warns about —
    /// and the journal's own errors otherwise.
    pub async fn record(&self, mut entry: JournalEntry) -> Result<u64, WorkerError> {
        if needs_stamp(&entry.kind)
            && let Some(agent_id) = entry.agent_id.clone()
            && let Some(payload) = self.broker.stamp(&agent_id).await?
        {
            entry.kind = stamped(entry.kind, payload);
        }
        self.sink.record(entry).await
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::event::Kind;
    use horde_proto::v1::{AgentResult, Payload, ResultReported, ResultStatus};

    use super::{AgentCheckout, ShareBroker, needs_stamp, stamped};

    fn result_kind(payload: Option<Payload>) -> Kind {
        Kind::ResultReported(ResultReported {
            result: Some(AgentResult {
                assignment_id: None,
                status: ResultStatus::Success as i32,
                summary: "done".to_owned(),
                payload,
            }),
        })
    }

    /// The whole truth table of "owes a stamp": only a result that arrived
    /// without a payload does. A result already carrying one must not be
    /// re-stamped (a redelivery could observe a later HEAD), a bodyless
    /// report is malformed rather than stampable, and other kinds are not
    /// results at all.
    #[test]
    fn only_a_payloadless_result_needs_a_stamp() {
        assert!(needs_stamp(&result_kind(None)));
        assert!(!needs_stamp(&result_kind(Some(Payload::default()))));
        assert!(!needs_stamp(&Kind::ResultReported(ResultReported {
            result: None
        })));
        assert!(!needs_stamp(&Kind::NoteSent(
            horde_proto::v1::NoteSent::default()
        )));
    }

    #[test]
    fn stamping_fills_the_result_and_leaves_other_kinds_alone() {
        let payload = Payload {
            git_refs: vec![horde_proto::v1::GitRef {
                repo_name: "atlas".to_owned(),
                commit_sha: "abc".to_owned(),
                branch: "main".to_owned(),
            }],
            attachments: Vec::new(),
        };

        let Kind::ResultReported(reported) = stamped(result_kind(None), payload.clone()) else {
            panic!("stamping must not change the kind");
        };
        assert_eq!(
            reported.result.and_then(|result| result.payload),
            Some(payload.clone())
        );

        let note = Kind::NoteSent(horde_proto::v1::NoteSent::default());
        assert_eq!(
            stamped(note.clone(), payload),
            note,
            "a kind that owes no payload passes through untouched"
        );
    }

    /// The registry: an agent's checkout is queryable while registered,
    /// resolves repo paths by Definition name, and is gone after forget —
    /// a stale entry would stamp an archived agent's HEAD into somebody
    /// else's result.
    #[test]
    fn the_broker_tracks_checkouts_by_agent_and_repo_name() {
        let broker = ShareBroker::new(None);
        broker.register(
            "agent-1",
            AgentCheckout {
                root: "/w/agents/agent-1".into(),
                repos: vec![("atlas".to_owned(), "atlas".into())],
            },
        );

        let checkout = broker.checkout_of("agent-1").expect("registered");
        assert_eq!(
            checkout.repo_path("atlas"),
            Some("/w/agents/agent-1/atlas".into())
        );
        assert_eq!(
            checkout.repo_path("elsewhere"),
            None,
            "a repo the definition does not name resolves to nothing"
        );
        assert!(broker.checkout_of("agent-2").is_none());

        broker.forget("agent-1");
        assert!(broker.checkout_of("agent-1").is_none());
    }

    /// Peer candidates exclude the receiver itself: fetching from yourself
    /// would "succeed" while sharing nothing.
    #[test]
    fn peer_paths_exclude_the_asking_agent() {
        let broker = ShareBroker::new(None);
        for agent in ["agent-1", "agent-2"] {
            broker.register(
                agent,
                AgentCheckout {
                    root: format!("/w/agents/{agent}").into(),
                    repos: vec![("atlas".to_owned(), "atlas".into())],
                },
            );
        }

        let peers = broker.peer_paths("atlas", "agent-1");
        assert_eq!(peers, vec![std::path::PathBuf::from("/w/agents/agent-2/atlas")]);
    }

    /// An agent with no checkout stamps nothing — the honest answer for an
    /// agent running where the Worker runs, not an error and not an empty
    /// payload that would read as "observed: no commits".
    #[tokio::test]
    async fn stamping_an_agent_without_a_checkout_is_none() {
        let broker = ShareBroker::new(None);
        assert_eq!(broker.stamp("agent-1").await.expect("no checkout"), None);
    }
}
