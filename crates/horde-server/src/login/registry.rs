//! The ephemeral half of the login plane: who is connected and what is live.
//!
//! Nothing here survives a restart and nothing here is ever journaled — the
//! same "durable audit, ephemeral realization" split the forward plane made
//! structural, taken one step further because a login has no durable intent
//! at all. Two maps: the workers holding a live login-dispatch stream, and
//! the logins alive right now (pairing pending or bytes flowing).
//!
//! Deregistration is epoch-guarded exactly like the forward registry's: by
//! the time a dying stream cleans up, its Worker may have reconnected, and a
//! stale stream must not knock the fresh one out.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use horde_proto::v1::{ForwardDirection, ForwardId, ForwardIntent, ForwardSpec, LoginFrame};
use tokio::sync::{mpsc, oneshot};

use crate::forward::registry::{PassiveTarget, TunnelPrincipal, may_answer};

/// How many undelivered login dispatches a lane may hold before new logins
/// are refused rather than queued. Logins are human-driven and rare; a
/// backlog this deep means the Worker is not draining.
const LANE_DEPTH: usize = 16;

/// The worker leg of a paired login, handed to the client driver: frames
/// pushed into `to_worker` reach the Worker's tunnel, frames read from
/// `from_worker` came from it (the `attach` first).
#[derive(Debug)]
pub struct LoginLeg {
    pub to_worker: mpsc::Sender<LoginFrame>,
    pub from_worker: mpsc::Receiver<LoginFrame>,
}

/// The outcome of a Worker tunnel claiming a dispatched login.
#[derive(Debug)]
pub enum LoginClaim {
    /// The claim matched; complete the pairing through this sender.
    Matched(oneshot::Sender<LoginLeg>),
    /// The login is live, but was dispatched to a different Worker. The
    /// entry survives for the right peer.
    WrongPrincipal,
    /// Nothing by that id is waiting to be claimed — never begun, over, or
    /// already claimed.
    Unknown,
}

/// One successful [`LoginRegistry::begin`]: the minted id and the receiver
/// the pairing resolves through.
#[derive(Debug)]
pub struct BegunLogin {
    pub login_id: String,
    pub paired: oneshot::Receiver<LoginLeg>,
}

/// What [`LoginRegistry::end`] tore down, for the audit event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndedLogin {
    pub worker_id: String,
    pub provider: String,
}

/// One live login's callback expose, as the tunnel plane resolves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackExpose {
    pub login_id: String,
    pub worker_id: String,
    pub provider: String,
    /// The browser-facing port the client binds exactly.
    pub port: u16,
    /// The worker-loopback port the flow's server listens on — what the
    /// worker dials per tunneled connection. Usually `port`.
    pub listen_port: u16,
}

/// The ephemeral forward id a login's callback expose tunnels under.
///
/// Derived, not stored: the server mints it for `LoginAccepted` and resolves
/// it at tunnel-open time, and one derivation keeps those two spellings from
/// drifting. Login ids are `login-…` while journaled forwards are
/// `fwd-<uuid>`, so the namespaces cannot collide.
#[must_use]
pub fn callback_forward_id(login_id: &str) -> String {
    format!("fwd-{login_id}")
}

#[derive(Debug, Clone)]
struct Lane {
    epoch: u64,
    sender: mpsc::Sender<horde_proto::v1::LoginSessionRequest>,
}

#[derive(Debug)]
struct LiveLogin {
    worker_id: String,
    provider: String,
    /// `Some` until the Worker's tunnel claims the pairing.
    pair: Option<oneshot::Sender<LoginLeg>>,
    /// The attach's callback, recorded when one was declared: the
    /// browser-facing port and the worker-side listener.
    callback: Option<(u16, u16)>,
}

#[derive(Debug, Default)]
struct Inner {
    next_epoch: u64,
    next_login: u64,
    workers: BTreeMap<String, Lane>,
    live: BTreeMap<String, LiveLogin>,
}

/// The login plane's connection state, shared by every login RPC handler.
#[derive(Debug, Clone, Default)]
pub struct LoginRegistry {
    inner: Arc<Mutex<Inner>>,
}

/// Whether a live-login map already holds a login for this worker+provider.
///
/// One at a time, named: two PTYs racing one provider's credential store on
/// one machine would corrupt it, and this predicate is the whole enforcement
/// — inline it and the refusal the spec demands becomes untestable.
fn provider_is_busy(live: &BTreeMap<String, LiveLogin>, worker_id: &str, provider: &str) -> bool {
    live.values()
        .any(|login| login.worker_id == worker_id && login.provider == provider)
}

impl LoginRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned mutex means a panic mid-update; the maps hold plain
        // values and senders, so recover the guard rather than wedging the
        // whole login plane.
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Registers `worker_id`'s login-dispatch stream, replacing any previous
    /// one (most recent wins). Returns the epoch to deregister with and the
    /// receiving end of the lane.
    #[must_use]
    pub fn register_worker(
        &self,
        worker_id: &str,
    ) -> (u64, mpsc::Receiver<horde_proto::v1::LoginSessionRequest>) {
        let mut inner = self.locked();
        let (sender, receiver) = mpsc::channel(LANE_DEPTH);
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
        let current = inner
            .workers
            .get(worker_id)
            .is_some_and(|lane| lane.epoch == epoch);
        if current {
            inner.workers.remove(worker_id);
        }
        current
    }

    /// The sender to dispatch a login request through, if the Worker holds a
    /// live login stream. Cloned out so the send happens outside the lock.
    #[must_use]
    pub fn lane_to(
        &self,
        worker_id: &str,
    ) -> Option<mpsc::Sender<horde_proto::v1::LoginSessionRequest>> {
        self.locked()
            .workers
            .get(worker_id)
            .map(|lane| lane.sender.clone())
    }

    /// Starts a login: mints its id, records it live, and parks the pairing.
    ///
    /// # Errors
    ///
    /// The worker+provider pair already has a login in flight — one at a
    /// time, because two PTYs racing one credential store would corrupt it.
    pub fn begin(&self, worker_id: &str, provider: &str) -> Result<BegunLogin, ProviderBusy> {
        let mut inner = self.locked();
        if provider_is_busy(&inner.live, worker_id, provider) {
            return Err(ProviderBusy);
        }
        inner.next_login += 1;
        let login_id = format!("login-{}", inner.next_login);
        let (pair, paired) = oneshot::channel();
        inner.live.insert(
            login_id.clone(),
            LiveLogin {
                worker_id: worker_id.to_owned(),
                provider: provider.to_owned(),
                pair: Some(pair),
                callback: None,
            },
        );
        Ok(BegunLogin { login_id, paired })
    }

    /// A Worker tunnel claiming the login it was dispatched.
    #[must_use]
    pub fn claim(&self, login_id: &str, claimer: &TunnelPrincipal) -> LoginClaim {
        let mut inner = self.locked();
        let Some(login) = inner.live.get_mut(login_id) else {
            return LoginClaim::Unknown;
        };
        let target = PassiveTarget::Worker(login.worker_id.clone());
        if !may_answer(&target, claimer) {
            return LoginClaim::WrongPrincipal;
        }
        match login.pair.take() {
            Some(pair) => LoginClaim::Matched(pair),
            None => LoginClaim::Unknown,
        }
    }

    /// Records the attach's callback against a live login, making its
    /// ephemeral expose resolvable and pushable: the browser-facing port,
    /// and the worker-side listener when the flow splits them.
    pub fn attach_callback(&self, login_id: &str, port: u16, listen_port: u16) {
        if let Some(login) = self.locked().live.get_mut(login_id) {
            login.callback = Some((port, effective_listen(port, listen_port)));
        }
    }

    /// Ends a login: the entry — and with it the callback expose — is gone.
    pub fn end(&self, login_id: &str) -> Option<EndedLogin> {
        self.locked().live.remove(login_id).map(|login| EndedLogin {
            worker_id: login.worker_id,
            provider: login.provider,
        })
    }

    /// Resolves an ephemeral callback expose by its derived forward id, for
    /// the tunnel plane's origination ruling.
    #[must_use]
    pub fn callback_expose(&self, forward_id: &str) -> Option<CallbackExpose> {
        let inner = self.locked();
        inner.live.iter().find_map(|(login_id, login)| {
            let (port, listen_port) = login.callback?;
            (callback_forward_id(login_id) == forward_id).then(|| CallbackExpose {
                login_id: login_id.clone(),
                worker_id: login.worker_id.clone(),
                provider: login.provider.clone(),
                port,
                listen_port,
            })
        })
    }

    /// The ephemeral expose intents `worker_id` should realize right now —
    /// merged into every pushed [`ForwardIntentSet`], because the Worker
    /// replaces its dialable map wholesale on every push and a merge done in
    /// only some pushes would flap the callback route mid-login.
    #[must_use]
    pub fn callback_intents_for(&self, worker_id: &str) -> Vec<ForwardIntent> {
        let inner = self.locked();
        inner
            .live
            .iter()
            .filter(|(_, login)| login.worker_id == worker_id)
            .filter_map(|(login_id, login)| {
                let (port, listen_port) = login.callback?;
                Some(ForwardIntent {
                    forward_id: Some(ForwardId::new(callback_forward_id(login_id))),
                    session_id: None,
                    agent_id: None,
                    spec: Some(ForwardSpec {
                        direction: ForwardDirection::Expose as i32,
                        name: format!("{}-login-callback", login.provider),
                        // What the worker dials per tunneled connection: the
                        // flow's actual listener.
                        worker_port: u32::from(listen_port),
                        // What the client binds exactly: the redirect port.
                        client_port: u32::from(port),
                    }),
                    backer_client_id: None,
                })
            })
            .collect()
    }
}

/// The worker-side listener an attach names: the split port when one was
/// given, the redirect port itself when zero. One rule, shared with the
/// tests, because the worker's dial target must never silently become 0.
#[must_use]
pub fn effective_listen(callback_port: u16, listen_port: u16) -> u16 {
    if listen_port == 0 {
        callback_port
    } else {
        listen_port
    }
}

/// The refusal [`LoginRegistry::begin`] answers a busy worker+provider with.
#[derive(Debug, thiserror::Error)]
#[error("a login for this worker and provider is already in progress")]
pub struct ProviderBusy;

#[cfg(test)]
mod tests {
    use super::{LoginClaim, LoginRegistry, callback_forward_id};
    use crate::forward::registry::TunnelPrincipal;

    #[test]
    fn a_registered_worker_lane_routes_until_deregistered() {
        let registry = LoginRegistry::new();
        assert!(registry.lane_to("w1").is_none());

        let (epoch, _lane) = registry.register_worker("w1");
        assert!(registry.lane_to("w1").is_some());

        assert!(registry.deregister_worker_if_current("w1", epoch));
        assert!(registry.lane_to("w1").is_none());
    }

    /// The reconnect race, same discipline as the forward registry: a stale
    /// stream's cleanup must not knock out the fresh stream.
    #[test]
    fn a_stale_epoch_cannot_deregister_the_fresh_login_stream() {
        let registry = LoginRegistry::new();
        let (old_epoch, _old) = registry.register_worker("w1");
        let (_new_epoch, _new) = registry.register_worker("w1");

        assert!(!registry.deregister_worker_if_current("w1", old_epoch));
        assert!(registry.lane_to("w1").is_some());
    }

    /// One login per worker+provider: the second begin refuses while the
    /// first is live, a different provider or worker is untouched, and
    /// ending the first frees the slot.
    #[test]
    fn one_login_per_worker_and_provider_at_a_time() {
        let registry = LoginRegistry::new();
        let first = registry.begin("w1", "claude").expect("the slot is free");

        assert!(
            registry.begin("w1", "claude").is_err(),
            "two PTYs racing one credential store would corrupt it"
        );
        let other_provider = registry
            .begin("w1", "codex")
            .expect("a different provider is a different credential store");
        let other_worker = registry
            .begin("w2", "claude")
            .expect("a different worker is a different machine");

        assert!(registry.end(&first.login_id).is_some());
        let freed = registry.begin("w1", "claude");
        assert!(freed.is_ok(), "ending the login frees the slot");
        drop((other_provider, other_worker, freed));
    }

    /// The claim table: only the dispatched worker (or an ambient channel)
    /// matches, a wrong worker leaves the entry claimable, and a second
    /// claim finds the pairing gone.
    #[test]
    fn a_login_is_claimable_exactly_by_its_dispatched_worker() {
        let registry = LoginRegistry::new();
        let begun = registry.begin("w1", "claude").expect("begins");

        assert!(matches!(
            registry.claim(&begun.login_id, &TunnelPrincipal::Worker("w2".to_owned())),
            LoginClaim::WrongPrincipal
        ));
        assert!(matches!(
            registry.claim(
                &begun.login_id,
                &TunnelPrincipal::Client("laptop".to_owned())
            ),
            LoginClaim::WrongPrincipal,
        ));
        assert!(
            matches!(
                registry.claim(&begun.login_id, &TunnelPrincipal::Worker("w1".to_owned())),
                LoginClaim::Matched(_)
            ),
            "a wrong claim must not have consumed the pairing"
        );
        assert!(matches!(
            registry.claim(&begun.login_id, &TunnelPrincipal::Worker("w1".to_owned())),
            LoginClaim::Unknown
        ));
        assert!(matches!(
            registry.claim("login-never", &TunnelPrincipal::Ambient),
            LoginClaim::Unknown
        ));
    }

    /// The callback expose exists exactly between attach and end, resolves
    /// by its derived id, and rides the worker's merged intent set.
    #[test]
    fn a_callback_expose_lives_from_attach_to_end() {
        let registry = LoginRegistry::new();
        let begun = registry.begin("w1", "fake").expect("begins");
        let forward_id = callback_forward_id(&begun.login_id);

        assert!(
            registry.callback_expose(&forward_id).is_none(),
            "before the attach there is no port to expose"
        );
        assert!(registry.callback_intents_for("w1").is_empty());

        registry.attach_callback(&begun.login_id, 14555, 0);
        let expose = registry
            .callback_expose(&forward_id)
            .expect("the attach makes it resolvable");
        assert_eq!(expose.worker_id, "w1");
        assert_eq!(expose.port, 14555);
        assert_eq!(
            expose.listen_port, 14555,
            "zero listen means the flow listens on the redirect port"
        );
        assert_eq!(expose.provider, "fake");

        let intents = registry.callback_intents_for("w1");
        assert_eq!(intents.len(), 1);
        let spec = intents[0].spec.as_ref().expect("a spec");
        assert_eq!(spec.worker_port, 14555);
        assert_eq!(
            spec.client_port, 14555,
            "an OAuth redirect URI is port-exact; the client may not remap it"
        );
        assert!(
            registry.callback_intents_for("w2").is_empty(),
            "another worker's set must not carry this login's expose"
        );

        registry.end(&begun.login_id);
        assert!(registry.callback_expose(&forward_id).is_none());
        assert!(registry.callback_intents_for("w1").is_empty());
    }

    /// The derivation is one rule: `LoginAccepted` mints it and the tunnel
    /// plane resolves it, so the exact string is contract.
    #[test]
    fn the_callback_forward_id_is_the_login_id_prefixed() {
        assert_eq!(callback_forward_id("login-7"), "fwd-login-7");
    }

    /// A split listener flows into the merged intent's worker half while
    /// the client half keeps the redirect port — the arrangement that lets
    /// one machine host both ends.
    #[test]
    fn a_split_listener_reaches_the_worker_half_of_the_intent() {
        assert_eq!(super::effective_listen(1455, 0), 1455);
        assert_eq!(super::effective_listen(1455, 14556), 14556);

        let registry = LoginRegistry::new();
        let begun = registry.begin("w1", "fake").expect("begins");
        registry.attach_callback(&begun.login_id, 1455, 14556);

        let expose = registry
            .callback_expose(&callback_forward_id(&begun.login_id))
            .expect("resolvable");
        assert_eq!((expose.port, expose.listen_port), (1455, 14556));

        let intents = registry.callback_intents_for("w1");
        let spec = intents[0].spec.as_ref().expect("a spec");
        assert_eq!(spec.worker_port, 14556, "the worker dials the listener");
        assert_eq!(spec.client_port, 1455, "the client binds the redirect");
    }

    #[test]
    fn ending_an_unknown_login_finds_nothing() {
        let registry = LoginRegistry::new();
        assert!(registry.end("login-none").is_none());
    }

    /// Ending reports what died, for the audit event's fields.
    #[test]
    fn ending_a_live_login_names_its_worker_and_provider() {
        let registry = LoginRegistry::new();
        let begun = registry.begin("w1", "claude").expect("begins");
        let ended = registry.end(&begun.login_id).expect("was live");
        assert_eq!(ended.worker_id, "w1");
        assert_eq!(ended.provider, "claude");
    }
}
