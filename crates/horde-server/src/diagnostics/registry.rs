//! Which workers serve log reads, and the reads waiting for their tunnels.
//!
//! Ephemeral like the login registry it copies: nothing here is journaled
//! and nothing survives a restart. A worker's lane is the dispatch stream
//! it holds open; a pending read is a query pushed down that lane whose
//! tunnel has not arrived yet. Deregistration is epoch-guarded: a dying
//! stream must not knock out the one that replaced it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use horde_proto::v1::LogQuery;
use horde_wire::logs::Frame;
use tokio::sync::{mpsc, oneshot};

/// Undelivered queries a lane may hold before new reads are refused.
const LANE_DEPTH: usize = 16;

/// What a worker's lane looked like the last time it registered.
#[derive(Debug, Clone)]
pub enum LaneState {
    /// Holding a live dispatch stream.
    Connected(mpsc::Sender<LogQuery>),
    /// Registered with `logs.v1` but holds no stream now; seen at this time.
    Away(Option<SystemTime>),
    /// Registered without `logs.v1`: it predates log serving.
    Unsupported,
    /// Never seen.
    Unknown,
}

#[derive(Debug)]
struct Lane {
    epoch: u64,
    sender: mpsc::Sender<LogQuery>,
}

#[derive(Debug)]
struct Pending {
    worker_id: String,
    pair: oneshot::Sender<mpsc::Receiver<Frame>>,
}

#[derive(Debug, Default)]
struct Inner {
    next_epoch: u64,
    next_request: u64,
    lanes: BTreeMap<String, Lane>,
    serves_logs: BTreeMap<String, bool>,
    last_seen: BTreeMap<String, SystemTime>,
    pending: BTreeMap<String, Pending>,
}

/// The outcome of a tunnel claiming a pending read.
#[derive(Debug)]
pub enum Claim {
    Matched(oneshot::Sender<mpsc::Receiver<Frame>>),
    /// The read is pending for a different worker; it stays for that one.
    WrongWorker,
    Unknown,
}

/// The log plane's connection state, shared by every handler.
#[derive(Debug, Clone, Default)]
pub struct LogRegistry {
    inner: Arc<Mutex<Inner>>,
}

impl LogRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Records what a registration's Hello said about log serving.
    pub fn note_registration(&self, worker_id: &str, serves_logs: bool) {
        self.locked()
            .serves_logs
            .insert(worker_id.to_owned(), serves_logs);
    }

    /// Registers `worker_id`'s dispatch stream, replacing any previous one.
    #[must_use]
    pub fn register_worker(&self, worker_id: &str) -> (u64, mpsc::Receiver<LogQuery>) {
        let mut inner = self.locked();
        let (sender, receiver) = mpsc::channel(LANE_DEPTH);
        inner.next_epoch += 1;
        let epoch = inner.next_epoch;
        inner
            .lanes
            .insert(worker_id.to_owned(), Lane { epoch, sender });
        // Subscribing is itself the proof the worker serves logs.
        inner.serves_logs.insert(worker_id.to_owned(), true);
        (epoch, receiver)
    }

    /// Removes `worker_id`'s lane unless a newer stream replaced it, and
    /// remembers when it was last seen. Returns whether this call removed
    /// anything.
    pub fn deregister_worker_if_current(
        &self,
        worker_id: &str,
        epoch: u64,
        now: SystemTime,
    ) -> bool {
        let mut inner = self.locked();
        let current = inner
            .lanes
            .get(worker_id)
            .is_some_and(|lane| lane.epoch == epoch);
        if current {
            inner.lanes.remove(worker_id);
            inner.last_seen.insert(worker_id.to_owned(), now);
        }
        current
    }

    /// Where a read for `worker_id` stands.
    #[must_use]
    pub fn lane(&self, worker_id: &str) -> LaneState {
        let inner = self.locked();
        if let Some(lane) = inner.lanes.get(worker_id) {
            return LaneState::Connected(lane.sender.clone());
        }
        match inner.serves_logs.get(worker_id) {
            Some(true) => LaneState::Away(inner.last_seen.get(worker_id).copied()),
            Some(false) => LaneState::Unsupported,
            None => LaneState::Unknown,
        }
    }

    /// Parks a read for `worker_id`'s tunnel; returns its request id and the
    /// receiver the tunnel's frames arrive through.
    #[must_use]
    pub fn begin(&self, worker_id: &str) -> (String, oneshot::Receiver<mpsc::Receiver<Frame>>) {
        let mut inner = self.locked();
        inner.next_request += 1;
        let request_id = format!("logs-{}", inner.next_request);
        let (pair, paired) = oneshot::channel();
        inner.pending.insert(
            request_id.clone(),
            Pending {
                worker_id: worker_id.to_owned(),
                pair,
            },
        );
        (request_id, paired)
    }

    /// Forgets a pending read whose tunnel never came.
    pub fn abandon(&self, request_id: &str) {
        self.locked().pending.remove(request_id);
    }

    /// A tunnel from `worker_id` (or from an unidentified plaintext peer,
    /// `None`) claims `request_id`.
    pub fn claim(&self, request_id: &str, worker_id: Option<&str>) -> Claim {
        let mut inner = self.locked();
        let Some(pending) = inner.pending.get(request_id) else {
            return Claim::Unknown;
        };
        if worker_id.is_some_and(|worker| worker != pending.worker_id) {
            return Claim::WrongWorker;
        }
        match inner.pending.remove(request_id) {
            Some(pending) => Claim::Matched(pending.pair),
            None => Claim::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::{Claim, LaneState, LogRegistry};

    #[test]
    fn a_lane_moves_through_its_states() {
        let registry = LogRegistry::new();
        assert!(matches!(registry.lane("w1"), LaneState::Unknown));
        registry.note_registration("w1", false);
        assert!(matches!(registry.lane("w1"), LaneState::Unsupported));
        registry.note_registration("w1", true);
        assert!(matches!(registry.lane("w1"), LaneState::Away(None)));
        let (epoch, _receiver) = registry.register_worker("w1");
        assert!(matches!(registry.lane("w1"), LaneState::Connected(_)));
        let now = SystemTime::now();
        assert!(registry.deregister_worker_if_current("w1", epoch, now));
        assert!(matches!(registry.lane("w1"), LaneState::Away(Some(seen)) if seen == now));
    }

    #[test]
    fn a_stale_stream_cannot_knock_out_its_replacement() {
        let registry = LogRegistry::new();
        let (old, _a) = registry.register_worker("w1");
        let (_new, _b) = registry.register_worker("w1");
        assert!(!registry.deregister_worker_if_current("w1", old, SystemTime::now()));
        assert!(matches!(registry.lane("w1"), LaneState::Connected(_)));
    }

    #[test]
    fn a_pending_read_is_claimed_once_and_only_by_its_worker() {
        let registry = LogRegistry::new();
        let (request_id, _paired) = registry.begin("w1");
        assert!(matches!(
            registry.claim(&request_id, Some("w2")),
            Claim::WrongWorker
        ));
        assert!(matches!(
            registry.claim(&request_id, Some("w1")),
            Claim::Matched(_)
        ));
        assert!(matches!(
            registry.claim(&request_id, Some("w1")),
            Claim::Unknown
        ));
        let (other, _paired) = registry.begin("w1");
        assert_ne!(other, request_id);
        assert!(
            matches!(registry.claim(&other, None), Claim::Matched(_)),
            "plaintext peer"
        );
        let (abandoned, _paired) = registry.begin("w1");
        registry.abandon(&abandoned);
        assert!(matches!(
            registry.claim(&abandoned, Some("w1")),
            Claim::Unknown
        ));
    }
}
