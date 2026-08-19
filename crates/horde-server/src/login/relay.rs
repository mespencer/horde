//! Pairing and splicing the two legs of one interactive login.
//!
//! A login is two gRPC bidi streams meeting at the relay — the Client's
//! `WorkerLogin` and the Worker's `WorkerLoginTunnel`, opened in answer to a
//! `LoginSessionRequest`. This module owns the mechanics: handshakes,
//! rulings, dispatch, pairing, and the splice. Journaling and identity
//! translation live in `service::logins`, exactly as the forward relay's do
//! in `service::forwards` — the relay never writes the log.
//!
//! Every wait here is ceilinged except the splice itself, whose lifetime is
//! the login's — ended by structure (a leg closing), never by clock.

use std::time::Duration;

use horde_proto::login::login_refusal_frame;
use horde_proto::v1::{
    LoginFrame, LoginOpen, LoginRefusalReason, LoginSessionRequest, login_frame::Frame,
};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;

use super::registry::{BegunLogin, LoginClaim, LoginLeg, LoginRegistry};
use crate::forward::registry::TunnelPrincipal;

/// How long the opening frame may take to arrive on either spine.
pub(crate) const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// How long the Worker has to spawn the PTY and attach its tunnel.
pub(crate) const PAIR_DEADLINE: Duration = Duration::from_secs(10);

/// Depth of each splice channel: PTY traffic is keystrokes and screen
/// paints, so a stalled peer exerts backpressure within a few frames.
const SPLICE_DEPTH: usize = 16;

/// One leg's I/O, stripped to bare frames — the login plane's [`LegIo`].
///
/// [`LegIo`]: crate::forward::relay::LegIo
pub struct LoginLegIo {
    pub inbound: std::pin::Pin<Box<dyn Stream<Item = Result<LoginFrame, Status>> + Send>>,
    pub outbound: mpsc::Sender<LoginFrame>,
}

impl LoginLegIo {
    /// Sends one frame toward this leg's peer, shrugging when it is gone —
    /// a peer that hung up has no use for the refusal it caused.
    pub(crate) async fn say(&mut self, frame: LoginFrame) {
        let _ = self.outbound.send(frame).await;
    }
}

/// Whether this channel's principal may open a login.
///
/// Clients yes — a login is an operator act. Ambient (plaintext loopback)
/// yes — locality is the bootstrap trust, exactly as on the forward plane.
/// Workers no: a Worker driving another Worker's credential ceremony is
/// unreachable through any honest peer, which is why the refusal the spec
/// demands is a named predicate rather than an inline match.
#[must_use]
pub fn may_open_login(caller: &TunnelPrincipal) -> bool {
    match caller {
        TunnelPrincipal::Client(_) | TunnelPrincipal::Ambient => true,
        TunnelPrincipal::Worker(_) => false,
    }
}

/// Waits (ceilinged) for the client's opening frame and rules on the opener.
///
/// # Errors
///
/// The refusal frame to say on the client's own leg.
pub(crate) async fn client_handshake(
    caller: &TunnelPrincipal,
    io: &mut LoginLegIo,
) -> Result<LoginOpen, LoginFrame> {
    if !may_open_login(caller) {
        return Err(login_refusal_frame(
            LoginRefusalReason::NotAClient,
            "logins are operator acts on client channels; this channel names a worker",
        ));
    }
    let opened = tokio::time::timeout(HANDSHAKE_DEADLINE, io.inbound.next()).await;
    let open = match opened {
        Ok(Some(Ok(LoginFrame {
            frame: Some(Frame::Open(open)),
        }))) => open,
        _ => {
            return Err(login_refusal_frame(
                LoginRefusalReason::Unspecified,
                "a login's first frame must be `open`, within the handshake deadline",
            ));
        }
    };
    let worker_named = open
        .worker_id
        .as_ref()
        .is_some_and(|worker| !worker.to_string().is_empty());
    if !worker_named || open.provider.is_empty() {
        return Err(login_refusal_frame(
            LoginRefusalReason::Unspecified,
            "a login open must name both the worker and the provider",
        ));
    }
    Ok(open)
}

/// Everything between "an open frame passed the handshake" and "the Worker's
/// tunnel is paired": vacancy, dispatch, pairing — each with its refusal.
/// Returns the minted login id with the leg, because every cleanup from here
/// on is keyed by it.
///
/// # Errors
///
/// The refusal frame to say on the client's own leg; any begun entry is
/// already ended.
pub(crate) async fn arrange(
    logins: &LoginRegistry,
    open: &LoginOpen,
) -> Result<(String, LoginLeg), LoginFrame> {
    let worker_id = open
        .worker_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let Some(lane) = logins.lane_to(&worker_id) else {
        return Err(login_refusal_frame(
            LoginRefusalReason::WorkerUnreachable,
            format!("worker {worker_id} has no live login stream"),
        ));
    };
    let BegunLogin { login_id, paired } =
        logins.begin(&worker_id, &open.provider).map_err(|_busy| {
            login_refusal_frame(
                LoginRefusalReason::AlreadyInProgress,
                format!(
                    "a {} login on {worker_id} is already in progress; one at a time",
                    open.provider
                ),
            )
        })?;

    let request = LoginSessionRequest {
        login_id: login_id.clone(),
        provider: open.provider.clone(),
        cols: open.cols,
        rows: open.rows,
    };
    if lane.try_send(request).is_err() {
        logins.end(&login_id);
        return Err(login_refusal_frame(
            LoginRefusalReason::WorkerUnreachable,
            format!("worker {worker_id} is not draining its login stream"),
        ));
    }
    match tokio::time::timeout(PAIR_DEADLINE, paired).await {
        Ok(Ok(leg)) => Ok((login_id, leg)),
        _ => {
            logins.end(&login_id);
            Err(login_refusal_frame(
                LoginRefusalReason::WorkerUnreachable,
                format!(
                    "worker {worker_id} did not attach within {}s",
                    PAIR_DEADLINE.as_secs()
                ),
            ))
        }
    }
}

/// How a spliced login ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpliceEnd {
    /// The Worker reported the command's exit; the code rode to the Client.
    Exited(u32),
    /// The Client's stream ended first — a detach or a dead client.
    ClientEnded,
    /// The Worker's leg ended without an exit frame — worker loss.
    WorkerEnded,
}

impl SpliceEnd {
    /// The audit event's reason, one spelling per cause.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::Exited(_) => "the login command exited",
            Self::ClientEnded => "the client disconnected mid-login",
            Self::WorkerEnded => "the worker's leg ended mid-login",
        }
    }
}

/// Moves frames both ways until either leg ends, remembering why.
///
/// An exit frame still relays — the Client prints the code — and then the
/// Worker closing its leg is what ends the splice; ends by structure, no
/// clock.
pub(crate) async fn splice(mut io: LoginLegIo, mut leg: LoginLeg) -> SpliceEnd {
    let mut exited: Option<u32> = None;
    loop {
        tokio::select! {
            frame = io.inbound.next() => {
                let Some(Ok(frame)) = frame else {
                    return SpliceEnd::ClientEnded;
                };
                if leg.to_worker.send(frame).await.is_err() {
                    return worker_end(exited);
                }
            }
            frame = leg.from_worker.recv() => {
                let Some(frame) = frame else {
                    return worker_end(exited);
                };
                if let Some(Frame::Exited(exit)) = &frame.frame {
                    exited = Some(exit.code);
                }
                if io.outbound.send(frame).await.is_err() {
                    return SpliceEnd::ClientEnded;
                }
            }
        }
    }
}

/// What a worker-side leg ending means: a command that already reported its
/// exit ended normally; one that did not is a worker loss. Named because the
/// two rows journal different reasons and only one is reachable happily.
pub(crate) fn worker_end(exited: Option<u32>) -> SpliceEnd {
    match exited {
        Some(code) => SpliceEnd::Exited(code),
        None => SpliceEnd::WorkerEnded,
    }
}

/// Runs one Worker login tunnel to completion: attach handshake, claim,
/// pairing, splice. The client driver owns journaling and cleanup — this leg
/// only carries frames, and its stream ending (either direction) is the
/// Worker's order to kill and reap the login command.
pub async fn run_worker_login_tunnel(
    logins: &LoginRegistry,
    caller: TunnelPrincipal,
    mut io: LoginLegIo,
) {
    let attach_frame = match tokio::time::timeout(HANDSHAKE_DEADLINE, io.inbound.next()).await {
        Ok(Some(Ok(
            frame @ LoginFrame {
                frame: Some(Frame::Attach(_)),
            },
        ))) => frame,
        _ => {
            return io
                .say(login_refusal_frame(
                    LoginRefusalReason::Unspecified,
                    "a login tunnel's first frame must be `attach`, within the handshake deadline",
                ))
                .await;
        }
    };
    let login_id = match &attach_frame.frame {
        Some(Frame::Attach(attach)) => attach.login_id.clone(),
        _ => String::new(),
    };

    match logins.claim(&login_id, &caller) {
        LoginClaim::Matched(pair) => {
            let (to_client, from_worker) = mpsc::channel(SPLICE_DEPTH);
            let (to_worker, mut from_client) = mpsc::channel(SPLICE_DEPTH);
            // The attach travels the leg like any frame, so the client
            // driver reads the callback port off the same channel the PTY
            // bytes will use — no side channel to race it.
            if to_client.send(attach_frame).await.is_err() {
                return;
            }
            if pair
                .send(LoginLeg {
                    to_worker,
                    from_worker,
                })
                .is_err()
            {
                // The client driver stopped waiting; ending the stream tells
                // the Worker to kill and reap the login command.
                return;
            }
            loop {
                tokio::select! {
                    frame = io.inbound.next() => {
                        let Some(Ok(frame)) = frame else { return };
                        if to_client.send(frame).await.is_err() {
                            return;
                        }
                    }
                    frame = from_client.recv() => {
                        let Some(frame) = frame else { return };
                        if io.outbound.send(frame).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
        LoginClaim::WrongPrincipal => {
            io.say(login_refusal_frame(
                LoginRefusalReason::UnknownLogin,
                format!("login {login_id} was dispatched to a different worker"),
            ))
            .await;
        }
        LoginClaim::Unknown => {
            io.say(login_refusal_frame(
                LoginRefusalReason::UnknownLogin,
                format!("no login {login_id} is waiting to be claimed"),
            ))
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::login::{
        login_attach_frame, login_input_frame, login_open_frame, login_output_frame,
        login_refusal_in,
    };
    use horde_proto::v1::LoginRefusalReason;

    use super::{SpliceEnd, may_open_login, worker_end};
    use crate::forward::registry::TunnelPrincipal;
    use crate::forward::testfix::within;
    use crate::login::registry::LoginRegistry;
    use crate::login::testfix::scripted_leg;

    /// The opener ruling, all three principals: the refusal the spec demands
    /// (non-Client refused) and the two channels that may drive a login.
    #[test]
    fn only_clients_and_ambient_channels_may_open_a_login() {
        assert!(may_open_login(&TunnelPrincipal::Client(
            "laptop".to_owned()
        )));
        assert!(may_open_login(&TunnelPrincipal::Ambient));
        assert!(!may_open_login(&TunnelPrincipal::Worker("w1".to_owned())));
    }

    /// The worker-end table: an exit already reported means a normal end,
    /// silence means worker loss. The rows journal different reasons.
    #[test]
    fn a_worker_leg_ending_is_an_exit_only_after_one_was_reported() {
        assert_eq!(worker_end(Some(0)), SpliceEnd::Exited(0));
        assert_eq!(worker_end(Some(3)), SpliceEnd::Exited(3));
        assert_eq!(worker_end(None), SpliceEnd::WorkerEnded);
    }

    /// Each splice end names its own reason — the audit trail's vocabulary,
    /// pinned because a swapped arm would journal a client disconnect as a
    /// clean exit.
    #[test]
    fn every_splice_end_explains_itself_distinctly() {
        let reasons = [
            SpliceEnd::Exited(0).reason(),
            SpliceEnd::ClientEnded.reason(),
            SpliceEnd::WorkerEnded.reason(),
        ];
        assert!(reasons[0].contains("exited"));
        assert!(reasons[1].contains("client disconnected"));
        assert!(reasons[2].contains("worker"));
        let unique: std::collections::BTreeSet<_> = reasons.iter().collect();
        assert_eq!(unique.len(), reasons.len());
    }

    /// A worker principal is refused before any frame is read — the
    /// non-Client refusal, coded.
    #[tokio::test]
    async fn a_worker_principal_may_not_open_a_login() {
        let (mut io, _feed, _out) = scripted_leg();
        let refused = super::client_handshake(&TunnelPrincipal::Worker("w1".to_owned()), &mut io)
            .await
            .expect_err("a worker channel must be refused");
        let refusal = login_refusal_in(&refused).expect("a refusal frame");
        assert_eq!(refusal.reason, LoginRefusalReason::NotAClient as i32);
    }

    /// The handshake refuses a first frame that is not `open`, an open that
    /// names no worker or provider, and accepts a proper one.
    #[tokio::test]
    async fn the_client_handshake_refuses_wrong_and_incomplete_openings() {
        let caller = TunnelPrincipal::Client("laptop".to_owned());

        let (mut io, feed, _out) = scripted_leg();
        feed.send(Ok(login_input_frame(vec![3])))
            .await
            .expect("send");
        let refused = super::client_handshake(&caller, &mut io)
            .await
            .expect_err("input before open must refuse");
        assert!(login_refusal_in(&refused).is_some());

        let (mut io, feed, _out) = scripted_leg();
        feed.send(Ok(login_open_frame("", "claude", 80, 24)))
            .await
            .expect("send");
        let refused = super::client_handshake(&caller, &mut io)
            .await
            .expect_err("a nameless worker must refuse");
        assert!(
            login_refusal_in(&refused)
                .expect("a refusal")
                .detail
                .contains("name both"),
        );

        let (mut io, feed, _out) = scripted_leg();
        feed.send(Ok(login_open_frame("w1", "claude", 80, 24)))
            .await
            .expect("send");
        let open = super::client_handshake(&caller, &mut io)
            .await
            .expect("a proper open passes");
        assert_eq!(open.provider, "claude");
        assert_eq!((open.cols, open.rows), (80, 24));
    }

    /// Arrange's refusal ladder: no worker lane refuses as unreachable, a
    /// busy worker+provider refuses as already-in-progress — and the failed
    /// arrangement frees its slot rather than leaking it.
    #[tokio::test]
    async fn arranging_refuses_unreachable_and_busy_with_their_own_codes() {
        let logins = LoginRegistry::new();
        let open = |provider: &str| horde_proto::v1::LoginOpen {
            worker_id: Some(horde_proto::v1::WorkerId::new("w1")),
            provider: provider.to_owned(),
            cols: 80,
            rows: 24,
        };

        let refused = super::arrange(&logins, &open("claude"))
            .await
            .expect_err("no lane, no login");
        assert_eq!(
            login_refusal_in(&refused).expect("a refusal").reason,
            LoginRefusalReason::WorkerUnreachable as i32
        );

        let (_epoch, mut lane) = logins.register_worker("w1");
        let busy = logins.begin("w1", "claude").expect("occupy the slot");
        let refused = super::arrange(&logins, &open("claude"))
            .await
            .expect_err("the slot is taken");
        assert_eq!(
            login_refusal_in(&refused).expect("a refusal").reason,
            LoginRefusalReason::AlreadyInProgress as i32
        );
        drop(busy);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), lane.recv())
                .await
                .is_err(),
            "a refused arrangement must not have dispatched anything"
        );
    }

    /// The full pairing over scripted legs: dispatch heard, worker attaches
    /// and claims, the attach crosses to the client driver's leg, and PTY
    /// frames flow both ways until the worker leg ends as an exit.
    #[tokio::test]
    async fn two_login_legs_pair_and_carry_frames_both_ways() {
        let logins = LoginRegistry::new();
        let (_epoch, mut lane) = logins.register_worker("w1");

        let open = horde_proto::v1::LoginOpen {
            worker_id: Some(horde_proto::v1::WorkerId::new("w1")),
            provider: "fake".to_owned(),
            cols: 80,
            rows: 24,
        };
        let arranging = {
            let logins = logins.clone();
            tokio::spawn(async move { super::arrange(&logins, &open).await })
        };

        let dispatched = tokio::time::timeout(std::time::Duration::from_secs(5), lane.recv())
            .await
            .expect("the lane must hear the dispatch")
            .expect("a login request");
        assert_eq!(dispatched.provider, "fake");
        assert_eq!((dispatched.cols, dispatched.rows), (80, 24));

        // The worker answers: a tunnel whose first frame attaches.
        let (worker_io, worker_feed, mut worker_out) = scripted_leg();
        let tunnel = {
            let logins = logins.clone();
            tokio::spawn(async move {
                super::run_worker_login_tunnel(
                    &logins,
                    TunnelPrincipal::Worker("w1".to_owned()),
                    worker_io,
                )
                .await;
            })
        };
        worker_feed
            .send(Ok(login_attach_frame(&dispatched.login_id, 0, 0)))
            .await
            .expect("the attach sends");

        let (login_id, mut leg) =
            tokio::time::timeout(std::time::Duration::from_secs(5), arranging)
                .await
                .expect("the pairing resolves")
                .expect("the arrange task must not panic")
                .expect("the arrangement succeeds");
        assert_eq!(login_id, dispatched.login_id);

        // The attach crossed the leg first.
        let first = within("the attach crosses", &mut leg.from_worker).await;
        assert_eq!(first, login_attach_frame(&login_id, 0, 0));

        // Client → worker keystrokes; worker → client output.
        leg.to_worker
            .send(login_input_frame(b"hunter2\r".to_vec()))
            .await
            .expect("input sends");
        let crossed = within("input reaches the worker leg", &mut worker_out).await;
        assert_eq!(crossed, login_input_frame(b"hunter2\r".to_vec()));

        worker_feed
            .send(Ok(login_output_frame(b"welcome".to_vec())))
            .await
            .expect("output sends");
        let echoed = within("output reaches the client side", &mut leg.from_worker).await;
        assert_eq!(echoed, login_output_frame(b"welcome".to_vec()));

        // The exit rides through, then the worker closes; the client driver
        // would journal Exited — here the leg simply ends.
        worker_feed
            .send(Ok(horde_proto::login::login_exit_frame(0)))
            .await
            .expect("exit sends");
        drop(worker_feed);
        let exit = within("the exit crosses", &mut leg.from_worker).await;
        assert_eq!(exit, horde_proto::login::login_exit_frame(0));

        tokio::time::timeout(std::time::Duration::from_secs(5), tunnel)
            .await
            .expect("the tunnel ends with its leg")
            .expect("no panic");
    }

    /// A tunnel attaching to nothing is refused with the coded reason, and a
    /// wrong worker's claim leaves the pairing for the right one.
    #[tokio::test]
    async fn a_tunnel_for_an_unknown_or_misdirected_login_is_refused() {
        let logins = LoginRegistry::new();

        let (io, feed, mut out) = scripted_leg();
        let task = {
            let logins = logins.clone();
            tokio::spawn(async move {
                super::run_worker_login_tunnel(&logins, TunnelPrincipal::Ambient, io).await;
            })
        };
        feed.send(Ok(login_attach_frame("login-none", 0, 0)))
            .await
            .expect("send");
        let refused = within("the unknown refusal", &mut out).await;
        assert_eq!(
            login_refusal_in(&refused).expect("a refusal").reason,
            LoginRefusalReason::UnknownLogin as i32
        );
        task.abort();

        let begun = logins.begin("w1", "fake").expect("begins");
        let (io, feed, mut out) = scripted_leg();
        let task = {
            let logins = logins.clone();
            tokio::spawn(async move {
                super::run_worker_login_tunnel(
                    &logins,
                    TunnelPrincipal::Worker("w2".to_owned()),
                    io,
                )
                .await;
            })
        };
        feed.send(Ok(login_attach_frame(&begun.login_id, 0, 0)))
            .await
            .expect("send");
        let refused = within("the misdirected refusal", &mut out).await;
        assert_eq!(
            login_refusal_in(&refused).expect("a refusal").reason,
            LoginRefusalReason::UnknownLogin as i32
        );
        assert!(
            matches!(
                logins.claim(&begun.login_id, &TunnelPrincipal::Worker("w1".to_owned())),
                crate::login::registry::LoginClaim::Matched(_)
            ),
            "the wrong worker's claim must not have consumed the pairing"
        );
        task.abort();
    }

    /// A tunnel that opens with anything but an attach is refused on its own
    /// leg — the protocol breach path.
    #[tokio::test]
    async fn a_tunnel_opening_without_an_attach_is_refused() {
        let logins = LoginRegistry::new();
        let (io, feed, mut out) = scripted_leg();
        let task = tokio::spawn(async move {
            super::run_worker_login_tunnel(&logins, TunnelPrincipal::Ambient, io).await;
        });
        feed.send(Ok(login_output_frame(b"early".to_vec())))
            .await
            .expect("send");
        let refused = within("the protocol refusal", &mut out).await;
        assert!(
            login_refusal_in(&refused)
                .expect("a refusal")
                .detail
                .contains("first frame"),
        );
        task.abort();
    }
}
