//! The interactive-auth relay's service glue (DESIGN §4), shared by both
//! spines the way [`super::forwards`] is.
//!
//! The mechanics — handshakes, pairing, the splice — live in
//! [`crate::login::relay`]; this module owns what only the service layer
//! may: identity translation, the audit journaling, and the gRPC wrapper
//! plumbing for the three login RPCs.

use horde_proto::login::{login_accepted_frame, login_refusal_frame};
use horde_proto::v1::{
    Event, LoginFrame, LoginOpen, LoginRefusalReason, WorkerId, WorkerLoginEnded,
    WorkerLoginStarted, event::Kind, login_frame::Frame,
};
use tonic::{Request, Status};

use super::shared::{ServerCore, append_server_event};
use crate::forward::registry::TunnelPrincipal;
use crate::login::LOGIN_STREAM;
use crate::login::registry::{LoginLeg, callback_forward_id};
use crate::login::relay::{
    LoginLegIo, PAIR_DEADLINE, SpliceEnd, arrange, client_handshake, splice,
};

/// Runs one Client login stream to completion: handshake, refusals, begin,
/// dispatch, pairing, journaling, splice, cleanup.
pub async fn run_client_login(core: &ServerCore, caller: TunnelPrincipal, mut io: LoginLegIo) {
    let open = match client_handshake(&caller, &mut io).await {
        Ok(open) => open,
        Err(refusal) => return io.say(refusal).await,
    };
    let (login_id, leg) = match arrange(&core.logins, &open).await {
        Ok(paired) => paired,
        Err(refusal) => return io.say(refusal).await,
    };
    drive_paired_login(core, &caller, &open, &login_id, io, leg).await;
}

/// From pairing to cleanup: read the Worker's first frame, journal, accept,
/// splice, and always end the login — the one place a paired entry dies.
async fn drive_paired_login(
    core: &ServerCore,
    caller: &TunnelPrincipal,
    open: &LoginOpen,
    login_id: &str,
    mut io: LoginLegIo,
    mut leg: LoginLeg,
) {
    let worker_id = open
        .worker_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();

    // The Worker's first frame: an attach (the PTY lives), or a refusal
    // (no recipe, spawn failed) forwarded verbatim. The claim already keyed
    // this pairing by our login id, so the attach cannot name another.
    let first = tokio::time::timeout(PAIR_DEADLINE, leg.from_worker.recv()).await;
    let attach = match first {
        Ok(Some(LoginFrame {
            frame: Some(Frame::Attach(attach)),
        })) => attach,
        Ok(Some(
            refusal @ LoginFrame {
                frame: Some(Frame::Refused(_)),
            },
        )) => {
            core.logins.end(login_id);
            return io.say(refusal).await;
        }
        _ => {
            core.logins.end(login_id);
            return io
                .say(login_refusal_frame(
                    LoginRefusalReason::WorkerUnreachable,
                    format!("worker {worker_id}'s tunnel ended before it attached"),
                ))
                .await;
        }
    };

    let callback = u16::try_from(attach.callback_port)
        .ok()
        .filter(|port| *port != 0);
    if let Some(port) = callback {
        let listen = u16::try_from(attach.listen_port).unwrap_or(0);
        core.logins.attach_callback(login_id, port, listen);
    }

    // Journaled *before* the accept, deliberately: the append's log
    // notification is what makes this worker's forward stream push the
    // merged intent set carrying the callback expose, so it is in flight
    // before the Client can possibly originate a callback connection.
    if journal_started(core, login_id, &worker_id, open, caller, callback)
        .await
        .is_err()
    {
        core.logins.end(login_id);
        return io
            .say(login_refusal_frame(
                LoginRefusalReason::Unspecified,
                "the relay could not journal the login",
            ))
            .await;
    }

    io.say(login_accepted_frame(
        login_id,
        callback.map(|_| callback_forward_id(login_id)).as_deref(),
        callback.map(u32::from).unwrap_or_default(),
    ))
    .await;

    let end = splice(io, leg).await;
    let ended = core.logins.end(login_id);
    let reason = ended_reason(end);
    tracing::info!(login_id, worker_id, reason, "login over");
    // Best-effort by design: the login is already over, and a log that
    // cannot take the ended row will refuse the next started row too.
    let _ = journal_ended(
        core,
        login_id,
        ended.as_ref().map_or(&worker_id, |e| &e.worker_id),
        ended.as_ref().map_or(&open.provider, |e| &e.provider),
        &reason,
    )
    .await;
}

/// The audit reason a splice end journals, exit code included — the one
/// place the vocabulary is assembled, so the fault suites can grep it.
fn ended_reason(end: SpliceEnd) -> String {
    match end {
        SpliceEnd::Exited(code) => format!("{} with {code}", end.reason()),
        SpliceEnd::ClientEnded | SpliceEnd::WorkerEnded => end.reason().to_owned(),
    }
}

async fn journal_started(
    core: &ServerCore,
    login_id: &str,
    worker_id: &str,
    open: &LoginOpen,
    caller: &TunnelPrincipal,
    callback: Option<u16>,
) -> Result<u64, crate::error::ServerError> {
    let event = Event {
        agent_id: None,
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::WorkerLoginStarted(WorkerLoginStarted {
            login_id: login_id.to_owned(),
            worker_id: Some(WorkerId::new(worker_id)),
            provider: open.provider.clone(),
            opened_by: actor_of_principal(caller),
            callback_port: callback.map(u32::from).unwrap_or_default(),
        })),
    };
    append_server_event(
        &core.log,
        LOGIN_STREAM,
        &format!("login-started:{login_id}"),
        event,
    )
    .await
}

async fn journal_ended(
    core: &ServerCore,
    login_id: &str,
    worker_id: &str,
    provider: &str,
    reason: &str,
) -> Result<u64, crate::error::ServerError> {
    let event = Event {
        agent_id: None,
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::WorkerLoginEnded(WorkerLoginEnded {
            login_id: login_id.to_owned(),
            worker_id: Some(WorkerId::new(worker_id)),
            provider: provider.to_owned(),
            reason: reason.to_owned(),
        })),
    };
    append_server_event(
        &core.log,
        LOGIN_STREAM,
        &format!("login-ended:{login_id}"),
        event,
    )
    .await
}

/// The wire actor a channel principal attributes events to. Only a client
/// certificate names one — ambient channels attribute nothing, and workers
/// cannot reach this path at all ([`crate::login::relay::may_open_login`]).
fn actor_of_principal(caller: &TunnelPrincipal) -> Option<horde_proto::v1::Actor> {
    match caller {
        TunnelPrincipal::Client(name) => Some(horde_proto::v1::Actor {
            actor: Some(horde_proto::v1::actor::Actor::ClientId(
                horde_proto::v1::ClientId::new(name.clone()),
            )),
        }),
        _ => None,
    }
}

/// A login leg's driver, boxed so one wrapper serves both spines.
type Drive = Box<
    dyn FnOnce(TunnelPrincipal, LoginLegIo) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
        + Send,
>;

/// One login leg answered whole: identity and serial read off the request,
/// the driver run over bare frames, an adapter lifting them into the spine's
/// response type, and revocation severing the stream — the login plane's
/// [`super::forwards::tunnel_response`].
pub(crate) fn login_response<Req, T, U, W>(
    deny: Option<horde_identity::RevocationList>,
    request: Request<tonic::Streaming<Req>>,
    unwrap: U,
    wrap: W,
    drive: Drive,
) -> tonic::Response<super::forwards::TunnelAnswer<T>>
where
    Req: Send + 'static,
    U: Fn(Req) -> LoginFrame + Send + 'static,
    T: Send + 'static,
    W: Fn(LoginFrame) -> T + Send + 'static,
{
    use tokio_stream::StreamExt as _;
    let serial = super::shared::peer_serial(&request);
    let caller = super::forwards::tunnel_principal_of(&request);
    let inbound = request
        .into_inner()
        .map(move |message| message.map(&unwrap));

    let (typed, stream) = tokio::sync::mpsc::channel(16);
    let (frames, frame_rx) = tokio::sync::mpsc::channel(16);

    super::shared::lift_frames(frame_rx, typed.clone(), wrap);
    tokio::spawn(async move {
        let pump = drive(
            caller,
            LoginLegIo {
                inbound: Box::pin(inbound),
                outbound: frames,
            },
        );
        super::shared::run_severable(pump, deny, serial, &typed).await;
    });
    tonic::Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(
        stream,
    )))
}

/// The worker id a login stream speaks for: the certificate's principal
/// when there is one, the request's claim on a plaintext channel — the same
/// trust order as the forward stream's.
pub(crate) fn login_worker_id(request: &Request<horde_proto::v1::StreamLoginsRequest>) -> String {
    if let TunnelPrincipal::Worker(name) = super::forwards::tunnel_principal_of(request) {
        return name;
    }
    request
        .get_ref()
        .worker_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default()
}

/// The login-dispatch stream's life: relay login requests as they arrive,
/// until either side hangs up.
pub(crate) async fn pump_logins(
    mut lane: tokio::sync::mpsc::Receiver<horde_proto::v1::LoginSessionRequest>,
    sender: &tokio::sync::mpsc::Sender<Result<horde_proto::v1::StreamLoginsResponse, Status>>,
) {
    loop {
        tokio::select! {
            // The worker hung up. Noticed here rather than on the next
            // failed send, because deregistration is what turns login opens
            // into fast WORKER_UNREACHABLE refusals instead of pairing
            // stalls.
            () = sender.closed() => return,
            request = lane.recv() => {
                // A closed lane means a newer stream replaced this one.
                let Some(request) = request else { return };
                let update = horde_proto::v1::StreamLoginsResponse {
                    request: Some(request),
                };
                if sender.send(Ok(update)).await.is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::login::{login_attach_frame, login_open_frame, login_refusal_in};
    use horde_proto::v1::{LoginRefusalReason, login_frame::Frame};
    use prost::Message as _;

    use super::super::shared::ServerCore;
    use crate::forward::registry::TunnelPrincipal;
    use crate::forward::testfix::within;
    use crate::login::testfix::{answering_tunnel, scripted_leg};

    fn core() -> ServerCore {
        ServerCore::new(crate::log::EventLog::in_memory().expect("an in-memory log"))
    }

    /// The client driver spawned as `caller` over a scripted leg, its open
    /// for `w1`/`provider` already sent — the shared arrangement every
    /// driver test opens with.
    async fn driven(
        core: &ServerCore,
        caller: TunnelPrincipal,
        provider: &str,
    ) -> crate::login::testfix::ScriptedPeer {
        let (io, feed, out) = scripted_leg();
        let core = core.clone();
        let driver = tokio::spawn(async move {
            super::run_client_login(&core, caller, io).await;
        });
        feed.send(Ok(login_open_frame("w1", provider, 80, 24)))
            .await
            .expect("the open sends");
        (feed, out, driver)
    }

    /// A driven login whose dispatch the test itself has claimed as the
    /// worker — the shared opening of the pairing-fault tests: the client
    /// leg's feed (held open; its drop would end the client side first),
    /// its outbound, the driver, and the claimed pairing sender.
    async fn claimed(
        core: &ServerCore,
    ) -> (
        tokio::sync::mpsc::Sender<Result<horde_proto::v1::LoginFrame, tonic::Status>>,
        tokio::sync::mpsc::Receiver<horde_proto::v1::LoginFrame>,
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Sender<crate::login::registry::LoginLeg>,
    ) {
        let (_epoch, mut lane) = core.logins.register_worker("w1");
        let (feed, out, driver) = driven(core, TunnelPrincipal::Ambient, "fake").await;
        let dispatched = within("the dispatch", &mut lane).await;
        match core
            .logins
            .claim(&dispatched.login_id, &TunnelPrincipal::Ambient)
        {
            crate::login::registry::LoginClaim::Matched(pair) => (feed, out, driver, pair),
            other => panic!("the dispatched login must be claimable: {other:?}"),
        }
    }

    /// The kinds journaled into the reserved login stream, decoded.
    async fn login_kinds(core: &ServerCore) -> Vec<horde_proto::v1::event::Kind> {
        core.log
            .read_all(crate::login::LOGIN_STREAM)
            .await
            .expect("the stream reads")
            .into_iter()
            .filter_map(|stored| {
                horde_proto::v1::Event::decode(stored.payload.as_slice())
                    .ok()
                    .and_then(|event| event.kind)
            })
            .collect()
    }

    /// The full client driver over scripted legs: a worker attaches with a
    /// callback, the client hears accepted with the derived forward id and
    /// exact port, both audit events land in the reserved stream, and the
    /// callback expose exists exactly while the login lives.
    #[tokio::test]
    async fn a_driven_login_accepts_journals_and_cleans_up() {
        let core = core();
        let (_epoch, mut lane) = core.logins.register_worker("w1");

        let (_client_feed, mut client_out, driver) =
            driven(&core, TunnelPrincipal::Client("laptop".to_owned()), "fake").await;
        let dispatched = within("the dispatch", &mut lane).await;
        let login_id = dispatched.login_id.clone();

        // The worker's tunnel: attach with a callback port, then exit 0.
        let (worker_feed, _worker_out, tunnel) =
            answering_tunnel(&core.logins, TunnelPrincipal::Worker("w1".to_owned()));
        worker_feed
            .send(Ok(login_attach_frame(&login_id, 14555, 0)))
            .await
            .expect("the attach sends");

        let accepted = within("the accepted frame", &mut client_out).await;
        let accepted = match accepted.frame {
            Some(Frame::Accepted(accepted)) => accepted,
            other => panic!("expected accepted, got {other:?}"),
        };
        assert_eq!(accepted.login_id, login_id);
        assert_eq!(accepted.callback_port, 14555);
        assert_eq!(
            accepted
                .callback_forward_id
                .as_ref()
                .map(ToString::to_string),
            Some(crate::login::registry::callback_forward_id(&login_id))
        );

        // Alive: the callback expose resolves and the started event landed.
        let forward_id = crate::login::registry::callback_forward_id(&login_id);
        assert!(core.logins.callback_expose(&forward_id).is_some());
        let kinds = login_kinds(&core).await;
        assert!(matches!(
            &kinds[..],
            [horde_proto::v1::event::Kind::WorkerLoginStarted(started)]
                if started.login_id == login_id && started.callback_port == 14555
                    && started.opened_by.is_some()
        ));

        // The command exits; the exit crosses; both streams end; cleanup.
        worker_feed
            .send(Ok(horde_proto::login::login_exit_frame(0)))
            .await
            .expect("the exit sends");
        drop(worker_feed);

        let exited = within("the exit crosses to the client", &mut client_out).await;
        assert_eq!(exited, horde_proto::login::login_exit_frame(0));

        tokio::time::timeout(std::time::Duration::from_secs(5), driver)
            .await
            .expect("the driver ends with the worker leg")
            .expect("no panic");
        assert!(
            core.logins.callback_expose(&forward_id).is_none(),
            "the ephemeral expose dies with the login"
        );
        let kinds = login_kinds(&core).await;
        assert!(matches!(
            &kinds[..],
            [
                horde_proto::v1::event::Kind::WorkerLoginStarted(_),
                horde_proto::v1::event::Kind::WorkerLoginEnded(ended)
            ] if ended.login_id == login_id && ended.reason.contains("exited") && ended.reason.contains('0')
        ));
        tunnel.abort();
    }

    /// The refusal the spec demands, end to end on the driver: a worker
    /// principal is refused before anything is dispatched or journaled.
    #[tokio::test]
    async fn a_worker_principal_is_refused_with_nothing_journaled() {
        let core = core();
        let (_epoch, mut lane) = core.logins.register_worker("w1");
        let (_feed, mut out, driver) =
            driven(&core, TunnelPrincipal::Worker("w2".to_owned()), "fake").await;

        let refused = within("the refusal", &mut out).await;
        assert_eq!(
            login_refusal_in(&refused).expect("a refusal").reason,
            LoginRefusalReason::NotAClient as i32
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), driver)
            .await
            .expect("the driver ends on refusal")
            .expect("no panic");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), lane.recv())
                .await
                .is_err(),
            "nothing was dispatched"
        );
        assert!(login_kinds(&core).await.is_empty(), "nothing was journaled");
    }

    /// A worker whose tunnel claims the login and then dies before pairing:
    /// the client hears WORKER_UNREACHABLE and the slot is freed — no
    /// journaling, because no PTY ever lived. (The never-answered variant
    /// ends the same way at the pairing deadline; this stages the same
    /// refusal path by structure instead of by clock.)
    #[tokio::test]
    async fn an_unanswered_dispatch_refuses_and_frees_the_slot() {
        let core = core();
        // The worker's tunnel claims and immediately dies: the dropped
        // pairing sender is what the arrange hears.
        let (_feed, mut out, driver, pair) = claimed(&core).await;
        drop(pair);

        let refused = within("the refusal", &mut out).await;
        assert_eq!(
            login_refusal_in(&refused).expect("a refusal").reason,
            LoginRefusalReason::WorkerUnreachable as i32
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), driver)
            .await
            .expect("the driver ends")
            .expect("no panic");
        assert!(
            core.logins.begin("w1", "fake").is_ok(),
            "the abandoned slot must be free for the next attempt"
        );
        assert!(login_kinds(&core).await.is_empty());
    }

    /// A worker that attaches and then refuses (unknown provider, spawn
    /// failure — the wire's first tunnel frame is always the attach, because
    /// the claim needs its login id): the refusal reaches the client
    /// verbatim, after the accepted the attach earned.
    #[tokio::test]
    async fn a_worker_refusal_reaches_the_client_verbatim() {
        let core = core();
        let (_epoch, mut lane) = core.logins.register_worker("w1");
        let (_feed, mut out, driver) = driven(&core, TunnelPrincipal::Ambient, "nope").await;
        let dispatched = within("the dispatch", &mut lane).await;

        // The worker claims, then refuses instead of attaching. On the wire
        // the worker's first tunnel frame is always the attach; the refusal
        // follows it. The pairing carries both across in order.
        let (worker_feed, _worker_out, tunnel) =
            answering_tunnel(&core.logins, TunnelPrincipal::Worker("w1".to_owned()));
        worker_feed
            .send(Ok(login_attach_frame(&dispatched.login_id, 0, 0)))
            .await
            .expect("the attach sends");
        worker_feed
            .send(Ok(horde_proto::login::login_refusal_frame(
                LoginRefusalReason::UnknownProvider,
                "no adapter for nope",
            )))
            .await
            .expect("the refusal sends");

        // The client hears accepted (the PTY-less attach) then the refusal.
        let accepted = within("the accepted frame", &mut out).await;
        assert!(matches!(accepted.frame, Some(Frame::Accepted(_))));
        let refused = within("the refusal crosses", &mut out).await;
        assert_eq!(
            login_refusal_in(&refused).expect("a refusal").reason,
            LoginRefusalReason::UnknownProvider as i32
        );

        drop(worker_feed);
        tokio::time::timeout(std::time::Duration::from_secs(5), driver)
            .await
            .expect("the driver ends")
            .expect("no panic");
        tunnel.abort();
    }

    /// A worker leg whose FIRST frame is a refusal — no attach at all, the
    /// shape a future worker could send — is forwarded to the client
    /// verbatim, not blurred into "ended before it attached". Staged through
    /// the registry's own claim, because the wire's tunnel driver always
    /// attaches first.
    #[tokio::test]
    async fn a_refusal_first_worker_leg_reaches_the_client_verbatim() {
        let core = core();
        // Claim as the worker would, then pair a leg whose first frame is
        // the refusal itself.
        let (_feed, mut out, driver, pair) = claimed(&core).await;
        let (to_client, from_worker) = tokio::sync::mpsc::channel(8);
        let (to_worker, _from_client) = tokio::sync::mpsc::channel(8);
        to_client
            .send(horde_proto::login::login_refusal_frame(
                LoginRefusalReason::SpawnFailed,
                "could not spawn the recipe",
            ))
            .await
            .expect("the refusal queues");
        pair.send(crate::login::registry::LoginLeg {
            to_worker,
            from_worker,
        })
        .expect("the pairing resolves");

        let refused = within("the refusal crosses verbatim", &mut out).await;
        let refusal = login_refusal_in(&refused).expect("a refusal");
        assert_eq!(refusal.reason, LoginRefusalReason::SpawnFailed as i32);
        assert_eq!(
            refusal.detail, "could not spawn the recipe",
            "the worker's own words, not the relay's unreachable blur"
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), driver)
            .await
            .expect("the driver ends on the refusal")
            .expect("no panic");
        assert!(login_kinds(&core).await.is_empty(), "no PTY ever lived");
    }

    /// The worker id a login stream speaks for, both sources: the request's
    /// claim on a plaintext channel, and nothing when it names nobody — the
    /// handler refuses an empty answer, so a defaulted or invented id here
    /// is a stream registered under the wrong key.
    #[test]
    fn the_login_streams_worker_id_comes_from_the_request_on_plaintext() {
        let named = tonic::Request::new(horde_proto::v1::StreamLoginsRequest {
            worker_id: Some(horde_proto::v1::WorkerId::new("w9")),
        });
        assert_eq!(super::login_worker_id(&named), "w9");

        let nameless =
            tonic::Request::new(horde_proto::v1::StreamLoginsRequest { worker_id: None });
        assert_eq!(super::login_worker_id(&nameless), "");
    }

    /// The dispatch pump, both directions of its life: requests cross into
    /// wrapped responses, and the lane closing ends it — a pump hollowed to
    /// nothing forwards no login ever.
    #[tokio::test]
    async fn the_login_pump_forwards_dispatches_until_the_lane_closes() {
        let (lane_tx, lane) = tokio::sync::mpsc::channel(4);
        let (sender, mut received) = tokio::sync::mpsc::channel(4);
        let pump = tokio::spawn(async move { super::pump_logins(lane, &sender).await });

        lane_tx
            .send(horde_proto::v1::LoginSessionRequest {
                login_id: "login-7".to_owned(),
                provider: "fake".to_owned(),
                cols: 80,
                rows: 24,
            })
            .await
            .expect("the dispatch queues");
        let update = within("the dispatch crosses", &mut received)
            .await
            .expect("an Ok update");
        assert_eq!(
            update.request.map(|request| request.login_id),
            Some("login-7".to_owned())
        );

        drop(lane_tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), pump)
            .await
            .expect("the closed lane ends the pump")
            .expect("no panic");
    }

    /// The exit-code vocabulary the audit trail and fault suites grep.
    #[test]
    fn the_ended_reason_carries_the_exit_code() {
        use crate::login::relay::SpliceEnd;
        assert_eq!(
            super::ended_reason(SpliceEnd::Exited(0)),
            "the login command exited with 0"
        );
        assert_eq!(
            super::ended_reason(SpliceEnd::ClientEnded),
            "the client disconnected mid-login"
        );
        assert_eq!(
            super::ended_reason(SpliceEnd::WorkerEnded),
            "the worker's leg ended mid-login"
        );
    }

    /// Only a client certificate is attributed; ambient and worker channels
    /// name nobody.
    #[test]
    fn only_a_client_principal_becomes_the_events_actor() {
        assert!(super::actor_of_principal(&TunnelPrincipal::Client("laptop".to_owned())).is_some());
        assert!(super::actor_of_principal(&TunnelPrincipal::Ambient).is_none());
        assert!(super::actor_of_principal(&TunnelPrincipal::Worker("w1".to_owned())).is_none());
    }
}
