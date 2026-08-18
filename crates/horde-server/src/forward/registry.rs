//! The ephemeral half of the forward plane: who is connected right now.
//!
//! Nothing here survives a restart, and nothing here is ever journaled —
//! that is the ADR's "durable intent, ephemeral realization" split made
//! structural. Three maps: the workers holding a live forward stream, the
//! clients holding a live serve stream (backing IS membership in this map),
//! and the tunnel legs waiting to be paired.
//!
//! Every deregistration is epoch-guarded, the same discipline as
//! `WorkerRegistry`: by the time a dying stream cleans up, its peer may have
//! reconnected, and a stale stream must not knock the fresh one out.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use horde_proto::v1::{ForwardConnectionRequest, ForwardTunnelFrame};
use tokio::sync::{mpsc, oneshot};

/// How many undelivered connection requests a lane may hold before new
/// tunnels are refused rather than queued. A backlog this deep means the
/// peer is not draining, and a bounded refusal beats an unbounded queue.
const LANE_DEPTH: usize = 64;

/// The half of a paired tunnel handed to the originator: frames pushed into
/// `to_passive` reach the answering peer, frames read from `from_passive`
/// came from it.
#[derive(Debug)]
pub struct PassiveLeg {
    pub to_passive: mpsc::Sender<ForwardTunnelFrame>,
    pub from_passive: mpsc::Receiver<ForwardTunnelFrame>,
}

/// Who a pending connection was dispatched to, and so who may answer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassiveTarget {
    Worker(String),
    Client(String),
}

/// Who a tunnel stream's channel says its peer is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TunnelPrincipal {
    /// An mTLS certificate naming a Client principal.
    Client(String),
    /// An mTLS certificate naming a Worker principal. Worker principal names
    /// are worker ids — the same equation `RegisterWorker` trusts.
    Worker(String),
    /// A plaintext loopback channel with no certificate: the bootstrap trust
    /// shape, same as the git relay's ambient door (DESIGN §8). Whoever
    /// reaches loopback already owns the disk, so ambient peers may answer
    /// any dispatch and originate either direction.
    Ambient,
}

/// Whether `claimer` is the peer a pending connection was dispatched to.
///
/// Exact name match per kind, and `Ambient` matches anything — the loopback
/// channel has no name to compare and is trusted by locality. The cross rows
/// (a client claiming a worker's dispatch and vice versa) are the ones this
/// predicate exists for: they are unreachable through any honest peer, so
/// inline they would be untestable.
#[must_use]
pub fn may_answer(target: &PassiveTarget, claimer: &TunnelPrincipal) -> bool {
    match (target, claimer) {
        (PassiveTarget::Worker(wanted), TunnelPrincipal::Worker(actual))
        | (PassiveTarget::Client(wanted), TunnelPrincipal::Client(actual)) => wanted == actual,
        (_, TunnelPrincipal::Ambient) => true,
        _ => false,
    }
}

/// The outcome of a passive peer claiming a pending connection.
#[derive(Debug)]
pub enum Claim {
    /// The claim matched; complete the pairing through this sender.
    Matched(oneshot::Sender<PassiveLeg>),
    /// The connection is pending, but was dispatched to somebody else. The
    /// pending entry survives for the right peer.
    WrongPrincipal,
    /// Nothing by that id is waiting — so the claimer is an originator.
    Unknown,
}

/// A registered stream: its sender, and the epoch that guards its removal.
#[derive(Debug, Clone)]
struct Lane {
    epoch: u64,
    sender: mpsc::Sender<ForwardConnectionRequest>,
}

#[derive(Debug)]
struct Pending {
    target: PassiveTarget,
    pair: oneshot::Sender<PassiveLeg>,
}

#[derive(Debug, Default)]
struct Inner {
    next_epoch: u64,
    workers: BTreeMap<String, Lane>,
    clients: BTreeMap<String, Lane>,
    pending: BTreeMap<String, Pending>,
}

/// The connection-plane state, shared by every forward RPC handler.
#[derive(Debug, Clone, Default)]
pub struct ForwardRegistry {
    inner: Arc<Mutex<Inner>>,
}

impl ForwardRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned registry mutex means a panic mid-update; the maps hold
        // only senders, which are safe to keep using, so recover the guard
        // rather than wedging the whole forward plane.
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Registers `worker_id`'s forward stream, replacing any previous one
    /// (most recent wins). Returns the epoch to deregister with and the
    /// receiving end of the lane.
    #[must_use]
    pub fn register_worker(&self, worker_id: &str) -> (u64, mpsc::Receiver<ForwardConnectionRequest>) {
        let (sender, receiver) = mpsc::channel(LANE_DEPTH);
        let mut inner = self.locked();
        inner.next_epoch += 1;
        let epoch = inner.next_epoch;
        inner
            .workers
            .insert(worker_id.to_owned(), Lane { epoch, sender });
        (epoch, receiver)
    }

    /// Removes `worker_id`'s lane, unless a newer stream has replaced it.
    /// Returns whether this call removed anything.
    pub fn deregister_worker_if_current(&self, worker_id: &str, epoch: u64) -> bool {
        let mut inner = self.locked();
        if lane_is_current(inner.workers.get(worker_id), epoch) {
            inner.workers.remove(worker_id);
            return true;
        }
        false
    }

    /// Registers `principal`'s serve stream — the act of backing. Most
    /// recent wins: the ADR's "exactly one backer connection" is this
    /// replacement, and an older stream from the same principal is simply
    /// never routed to again.
    #[must_use]
    pub fn register_client(&self, principal: &str) -> (u64, mpsc::Receiver<ForwardConnectionRequest>) {
        let (sender, receiver) = mpsc::channel(LANE_DEPTH);
        let mut inner = self.locked();
        inner.next_epoch += 1;
        let epoch = inner.next_epoch;
        inner
            .clients
            .insert(principal.to_owned(), Lane { epoch, sender });
        (epoch, receiver)
    }

    /// Removes `principal`'s lane, unless a newer stream re-backed since.
    pub fn deregister_client_if_current(&self, principal: &str, epoch: u64) -> bool {
        let mut inner = self.locked();
        if lane_is_current(inner.clients.get(principal), epoch) {
            inner.clients.remove(principal);
            return true;
        }
        false
    }

    /// Whether `worker_id` currently holds a forward stream.
    #[must_use]
    pub fn worker_reachable(&self, worker_id: &str) -> bool {
        self.locked().workers.contains_key(worker_id)
    }

    /// Whether `principal` currently backs — holds a live serve stream.
    #[must_use]
    pub fn client_backing(&self, principal: &str) -> bool {
        self.locked().clients.contains_key(principal)
    }

    /// Parks a connection awaiting its passive leg. The returned receiver
    /// resolves when [`claim`](Self::claim) matches.
    #[must_use]
    pub fn park_pending(
        &self,
        connection_id: &str,
        target: PassiveTarget,
    ) -> oneshot::Receiver<PassiveLeg> {
        let (pair, paired) = oneshot::channel();
        self.locked()
            .pending
            .insert(connection_id.to_owned(), Pending { target, pair });
        paired
    }

    /// Abandons a parked connection — the originator gave up waiting.
    pub fn abandon_pending(&self, connection_id: &str) {
        self.locked().pending.remove(connection_id);
    }

    /// A passive peer claiming the connection it was asked to carry.
    #[must_use]
    pub fn claim(&self, connection_id: &str, claimer: &TunnelPrincipal) -> Claim {
        let mut inner = self.locked();
        let Some(pending) = inner.pending.get(connection_id) else {
            return Claim::Unknown;
        };
        if !may_answer(&pending.target, claimer) {
            return Claim::WrongPrincipal;
        }
        match inner.pending.remove(connection_id) {
            Some(pending) => Claim::Matched(pending.pair),
            None => Claim::Unknown,
        }
    }

    /// The sender to dispatch a connection request through, if the target is
    /// connected. Cloned out so the send happens outside the lock.
    #[must_use]
    pub fn lane_to(&self, target: &PassiveTarget) -> Option<mpsc::Sender<ForwardConnectionRequest>> {
        let inner = self.locked();
        let lane = match target {
            PassiveTarget::Worker(worker_id) => inner.workers.get(worker_id),
            PassiveTarget::Client(principal) => inner.clients.get(principal),
        };
        lane.map(|lane| lane.sender.clone())
    }
}

/// Whether a stored lane is the one an epoch refers to.
///
/// Both halves matter and the mixed rows are unreachable end-to-end: a lane
/// that is absent was usually removed by the peer's own reconnect, and a lane
/// with a *newer* epoch is exactly the fresh stream a stale cleanup must not
/// remove — `&&` versus "is there a lane" decides whether a reconnect
/// survives its predecessor's teardown.
fn lane_is_current(lane: Option<&Lane>, epoch: u64) -> bool {
    lane.is_some_and(|lane| lane.epoch == epoch)
}

#[cfg(test)]
mod tests {
    use super::{Claim, ForwardRegistry, PassiveTarget, TunnelPrincipal};

    #[test]
    fn a_registered_worker_is_reachable_until_deregistered() {
        let registry = ForwardRegistry::new();
        assert!(!registry.worker_reachable("w1"));

        let (epoch, _lane) = registry.register_worker("w1");
        assert!(registry.worker_reachable("w1"));

        assert!(registry.deregister_worker_if_current("w1", epoch));
        assert!(!registry.worker_reachable("w1"));
    }

    /// The reconnect race: a stale stream's cleanup must not knock out the
    /// fresh stream that replaced it.
    #[test]
    fn a_stale_epoch_cannot_deregister_the_fresh_stream() {
        let registry = ForwardRegistry::new();
        let (old_epoch, _old) = registry.register_worker("w1");
        let (_new_epoch, _new) = registry.register_worker("w1");

        assert!(
            !registry.deregister_worker_if_current("w1", old_epoch),
            "the old stream's teardown is not its to declare any more"
        );
        assert!(registry.worker_reachable("w1"));
    }

    #[test]
    fn backing_is_membership_and_most_recent_wins() {
        let registry = ForwardRegistry::new();
        assert!(!registry.client_backing("laptop"));

        let (old_epoch, _old) = registry.register_client("laptop");
        assert!(registry.client_backing("laptop"));

        // A second horde process on the same machine re-backs; the old
        // stream's later death must not read as detachment.
        let (_new, _lane) = registry.register_client("laptop");
        assert!(!registry.deregister_client_if_current("laptop", old_epoch));
        assert!(registry.client_backing("laptop"));
    }

    #[test]
    fn a_client_lane_deregisters_like_a_worker_lane() {
        let registry = ForwardRegistry::new();
        let (epoch, _lane) = registry.register_client("laptop");
        assert!(registry.deregister_client_if_current("laptop", epoch));
        assert!(!registry.client_backing("laptop"));
    }

    #[test]
    fn lanes_route_by_target_kind() {
        let registry = ForwardRegistry::new();
        let (_we, _wl) = registry.register_worker("w1");
        let (_ce, _cl) = registry.register_client("laptop");

        assert!(
            registry
                .lane_to(&PassiveTarget::Worker("w1".to_owned()))
                .is_some()
        );
        assert!(
            registry
                .lane_to(&PassiveTarget::Client("laptop".to_owned()))
                .is_some()
        );
        assert!(
            registry
                .lane_to(&PassiveTarget::Worker("laptop".to_owned()))
                .is_none(),
            "a client principal is not a worker lane"
        );
        assert!(
            registry
                .lane_to(&PassiveTarget::Client("w1".to_owned()))
                .is_none()
        );
    }

    /// The claim table: matched, wrong principal (entry survives), unknown.
    #[test]
    fn a_pending_connection_is_claimable_exactly_by_its_target() {
        let registry = ForwardRegistry::new();
        let _paired = registry.park_pending("c1", PassiveTarget::Worker("w1".to_owned()));

        assert!(matches!(
            registry.claim("c1", &TunnelPrincipal::Worker("other".to_owned())),
            Claim::WrongPrincipal
        ));
        assert!(
            matches!(
                registry.claim("c1", &TunnelPrincipal::Worker("w1".to_owned())),
                Claim::Matched(_)
            ),
            "a wrong claim must not have consumed the entry"
        );
        assert!(matches!(
            registry.claim("c1", &TunnelPrincipal::Worker("w1".to_owned())),
            Claim::Unknown
        ));
        assert!(matches!(
            registry.claim("never-parked", &TunnelPrincipal::Worker("w1".to_owned())),
            Claim::Unknown
        ));
    }

    #[test]
    fn an_abandoned_connection_is_no_longer_claimable() {
        let registry = ForwardRegistry::new();
        let _paired = registry.park_pending("c1", PassiveTarget::Client("laptop".to_owned()));
        registry.abandon_pending("c1");
        assert!(matches!(
            registry.claim("c1", &TunnelPrincipal::Client("laptop".to_owned())),
            Claim::Unknown
        ));
    }

    /// The full answering table, cross rows included: those are unreachable
    /// through any honest peer, which is why the predicate is named.
    #[test]
    fn only_the_dispatched_peer_or_an_ambient_channel_may_answer() {
        use super::{TunnelPrincipal as P, may_answer};

        let to_worker = PassiveTarget::Worker("w1".to_owned());
        let to_client = PassiveTarget::Client("laptop".to_owned());

        assert!(may_answer(&to_worker, &P::Worker("w1".to_owned())));
        assert!(!may_answer(&to_worker, &P::Worker("w2".to_owned())));
        assert!(!may_answer(&to_worker, &P::Client("w1".to_owned())));
        assert!(may_answer(&to_worker, &P::Ambient));

        assert!(may_answer(&to_client, &P::Client("laptop".to_owned())));
        assert!(!may_answer(&to_client, &P::Client("desk".to_owned())));
        assert!(!may_answer(&to_client, &P::Worker("laptop".to_owned())));
        assert!(may_answer(&to_client, &P::Ambient));
    }
}
