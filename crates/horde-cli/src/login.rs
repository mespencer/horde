//! Client-side interactive-auth relay (DESIGN §4): the terminal half.
//!
//! `horde worker login <worker> <provider>` attaches THIS terminal to the
//! provider's login command running in a PTY on the worker: raw mode in,
//! keystrokes relayed frame by frame, PTY bytes painted verbatim, until the
//! command exits or the relay refuses. When the worker's recipe declares an
//! OAuth-callback port, the same call realizes an **ephemeral expose** of it
//! — exactly that port on this machine's loopback, because a redirect URI is
//! port-exact — so the browser's callback lands on the worker's listener.
//! Both the PTY relay and the expose die with this process; nothing is left
//! behind to revive.

use std::io::Write;

use horde_proto::login::{login_input_frame, login_open_frame, login_resize_frame};
use horde_proto::v1::{LoginFrame, LoginRefusal, login_frame::Frame};
use tokio_stream::StreamExt;

use crate::client::{Client, ClientError};

/// How long the relay may take to accept or refuse a login: its own
/// handshake and pairing deadlines (10s each) with room to spare.
const ACCEPT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// How many non-answer frames the acceptance wait tolerates. Nothing
/// legitimate precedes the answer; a count bound on a misbehaving peer.
const ACCEPT_FRAME_BOUND: u32 = 16;

/// The open login stream, bare frames both ways.
pub struct LoginStream {
    pub to_relay: tokio::sync::mpsc::Sender<LoginFrame>,
    pub from_relay: std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<LoginFrame, tonic::Status>> + Send>,
    >,
}

impl std::fmt::Debug for LoginStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LoginStream")
            .finish_non_exhaustive()
    }
}

/// An accepted login, ready to pump.
#[derive(Debug)]
pub struct OpenedLogin {
    pub stream: LoginStream,
    pub login_id: String,
    /// The callback expose to realize, when the recipe declares one:
    /// `(forward_id, exact local port)`.
    pub callback: Option<(String, u16)>,
}

/// Opens a login against `worker`/`provider` and waits (ceilinged) for the
/// relay's answer.
///
/// # Errors
///
/// [`ClientError::Login`] carrying the relay's refusal detail (worker
/// unreachable, already in progress, not a client, unknown provider), or
/// the transport failure that ended the stream first.
pub async fn open_login(
    client: &mut Client,
    worker: &str,
    provider: &str,
    cols: u16,
    rows: u16,
) -> Result<OpenedLogin, ClientError> {
    let mut stream = client.worker_login().await?;
    stream
        .to_relay
        .send(login_open_frame(
            worker,
            provider,
            u32::from(cols),
            u32::from(rows),
        ))
        .await
        .map_err(|_| ClientError::Login("the login stream closed before the open".to_owned()))?;

    let answer = tokio::time::timeout(ACCEPT_DEADLINE, async {
        let mut seen = 0_u32;
        while seen < ACCEPT_FRAME_BOUND {
            match stream.from_relay.next().await {
                Some(Ok(LoginFrame {
                    frame: Some(Frame::Accepted(accepted)),
                })) => return Ok(accepted),
                Some(Ok(LoginFrame {
                    frame: Some(Frame::Refused(refusal)),
                })) => return Err(refusal_error(&refusal)),
                Some(Ok(_)) => seen += 1,
                Some(Err(status)) => return Err(ClientError::Rpc(status)),
                None => {
                    return Err(ClientError::Login(
                        "the relay ended the stream without an answer".to_owned(),
                    ));
                }
            }
        }
        Err(ClientError::Login(
            "the relay babbled instead of answering".to_owned(),
        ))
    })
    .await
    .map_err(|_elapsed| {
        ClientError::Login(format!(
            "the relay did not answer within {}s",
            ACCEPT_DEADLINE.as_secs()
        ))
    })??;

    Ok(OpenedLogin {
        stream,
        login_id: answer.login_id.clone(),
        callback: callback_of(
            answer.callback_forward_id.as_ref().map(ToString::to_string),
            answer.callback_port,
        ),
    })
}

/// The callback an accepted frame names, when it names a whole one.
///
/// Both halves or neither: a port without a forward id is untunnelable, and
/// an id without a port is unbindable — either alone would realize a
/// listener no byte can cross, so the mixed rows (unreachable through an
/// honest relay) collapse to "no callback".
#[must_use]
pub fn callback_of(forward_id: Option<String>, port: u32) -> Option<(String, u16)> {
    let port = u16::try_from(port).ok().filter(|port| *port != 0)?;
    let forward_id = forward_id.filter(|id| !id.is_empty())?;
    Some((forward_id, port))
}

/// A refusal as the operator reads it.
fn refusal_error(refusal: &LoginRefusal) -> ClientError {
    ClientError::Login(refusal.detail.clone())
}

/// How a pumped login ended.
#[derive(Debug, PartialEq, Eq)]
pub enum LoginEnd {
    /// The command finished; here is its exit code.
    Exited(u32),
    /// The relay refused mid-session.
    Refused(String),
    /// The stream ended without an exit — a worker loss or a partition.
    ServerEnded,
}

/// Pumps an accepted login until it ends: keystrokes and resizes toward the
/// worker, PTY bytes into `output` (flushed per chunk — a prompt with no
/// newline must still paint).
///
/// Ends by structure: the exit frame, a refusal, or either stream closing.
pub async fn pump_login(
    mut stream: LoginStream,
    mut input: tokio::sync::mpsc::Receiver<Vec<u8>>,
    mut resizes: tokio::sync::mpsc::Receiver<(u16, u16)>,
    output: &mut (dyn Write + Send),
) -> LoginEnd {
    loop {
        tokio::select! {
            frame = stream.from_relay.next() => {
                match frame {
                    Some(Ok(LoginFrame { frame: Some(Frame::Output(bytes)) })) => {
                        if output.write_all(&bytes).and_then(|()| output.flush()).is_err() {
                            return LoginEnd::ServerEnded;
                        }
                    }
                    Some(Ok(LoginFrame { frame: Some(Frame::Exited(exit)) })) => {
                        return LoginEnd::Exited(exit.code);
                    }
                    Some(Ok(LoginFrame { frame: Some(Frame::Refused(refusal)) })) => {
                        return LoginEnd::Refused(refusal.detail);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => return LoginEnd::ServerEnded,
                }
            }
            typed = input.recv() => {
                let Some(bytes) = typed else { continue };
                if stream.to_relay.send(login_input_frame(bytes)).await.is_err() {
                    return LoginEnd::ServerEnded;
                }
            }
            resized = resizes.recv() => {
                let Some((cols, rows)) = resized else { continue };
                if stream
                    .to_relay
                    .send(login_resize_frame(u32::from(cols), u32::from(rows)))
                    .await
                    .is_err()
                {
                    return LoginEnd::ServerEnded;
                }
            }
        }
    }
}

/// Raw mode, released on drop — including every early-return and panic path
/// between enabling it and the login's end.
struct RawModeGuard {
    enabled: bool,
}

impl RawModeGuard {
    /// Enables raw mode when stdin is a terminal; a piped stdin (scripts,
    /// the fault suites) pumps without one.
    fn enable_if_terminal() -> Self {
        use std::io::IsTerminal;
        let enabled = std::io::stdin().is_terminal()
            && ratatui::crossterm::terminal::enable_raw_mode().is_ok();
        Self { enabled }
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = ratatui::crossterm::terminal::disable_raw_mode();
        }
    }
}

/// The terminal's size, or the classic default off a pipe.
fn terminal_size() -> (u16, u16) {
    ratatui::crossterm::terminal::size().unwrap_or((80, 24))
}

/// Runs the whole interactive login: open, realize the callback expose,
/// raw-mode the terminal, pump until the command exits, clean up.
///
/// # Errors
///
/// [`ClientError::Login`] for refusals, a taken callback port, a lost
/// worker, and a login command that exited nonzero — each in the operator's
/// words; plus any transport failure underneath.
pub async fn run_interactive(
    client: &mut Client,
    worker: &str,
    provider: &str,
    format: crate::render::Format,
) -> Result<(), ClientError> {
    let (cols, rows) = terminal_size();
    let opened = open_login(client, worker, provider, cols, rows).await?;

    // The callback expose realizes before any byte flows, so the URL the
    // login command prints first is already reachable when it paints.
    let realized = match &opened.callback {
        Some((forward_id, port)) => Some(
            crate::forward::realize_expose_exact(
                client,
                forward_id,
                &format!("{provider}-login-callback"),
                *port,
            )
            .await
            .map_err(|error| {
                ClientError::Login(format!(
                    "could not bind the callback port {port} on this machine: {error} \
                     (the provider's redirect URI is port-exact, so there is no fallback)"
                ))
            })?,
        ),
        None => None,
    };
    println!(
        "{}",
        started_line(
            worker,
            provider,
            &opened.login_id,
            opened.callback.as_ref().map(|(_, port)| *port),
            format
        )
    );

    // Keystrokes: a blocking stdin reader thread. It ends with the process;
    // a blocked terminal read cannot be cancelled portably, and this process
    // exits as soon as the login ends.
    let (typed_tx, typed) = tokio::sync::mpsc::channel::<Vec<u8>>(16);
    std::thread::spawn(move || {
        use std::io::Read;
        let mut stdin = std::io::stdin();
        let mut buffer = [0_u8; 1024];
        while let Ok(count) = stdin.read(&mut buffer) {
            if count == 0 || typed_tx.blocking_send(buffer[..count].to_vec()).is_err() {
                return;
            }
        }
    });

    // Terminal resizes, relayed as frames (unix: SIGWINCH).
    let (resize_tx, resizes) = tokio::sync::mpsc::channel::<(u16, u16)>(4);
    let winch = tokio::spawn(async move {
        let Ok(mut signals) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())
        else {
            return;
        };
        while signals.recv().await.is_some() {
            if resize_tx.send(terminal_size()).await.is_err() {
                return;
            }
        }
    });

    let end = {
        let _raw = RawModeGuard::enable_if_terminal();
        let mut stdout = std::io::stdout();
        pump_login(opened.stream, typed, resizes, &mut stdout).await
        // _raw drops here: the terminal is sane again before anything prints.
    };
    winch.abort();
    if let Some(realized) = realized {
        realized.stop().await;
    }

    match end {
        LoginEnd::Exited(0) => {
            println!("{}", exited_line(&opened.login_id, 0, format));
            Ok(())
        }
        LoginEnd::Exited(code) => Err(ClientError::Login(format!(
            "the login command exited with {code}"
        ))),
        LoginEnd::Refused(detail) => Err(ClientError::Login(detail)),
        LoginEnd::ServerEnded => Err(ClientError::Login(
            "the relay ended mid-login; the worker may have restarted".to_owned(),
        )),
    }
}

/// The line printed before the terminal goes raw: what started, and where
/// the browser's callback will land.
#[must_use]
pub fn started_line(
    worker: &str,
    provider: &str,
    login_id: &str,
    callback_port: Option<u16>,
    format: crate::render::Format,
) -> String {
    match format {
        crate::render::Format::Json => serde_json::json!({
            "login_id": login_id,
            "worker": worker,
            "provider": provider,
            "callback_port": callback_port,
        })
        .to_string(),
        crate::render::Format::Human => match callback_port {
            Some(port) => format!(
                "login started worker={worker} provider={provider} \
                 callback=http://localhost:{port} (relayed to the worker)"
            ),
            None => format!("login started worker={worker} provider={provider}"),
        },
    }
}

/// The line printed after a clean exit.
#[must_use]
pub fn exited_line(login_id: &str, code: u32, format: crate::render::Format) -> String {
    match format {
        crate::render::Format::Json => serde_json::json!({
            "login_id": login_id,
            "exit_code": code,
        })
        .to_string(),
        crate::render::Format::Human => format!("login finished with exit code {code}"),
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::login::{login_exit_frame, login_output_frame, login_refusal_frame};
    use horde_proto::v1::LoginRefusalReason;
    use tokio_stream::wrappers::ReceiverStream;

    use super::{LoginEnd, LoginStream, callback_of, pump_login};

    /// The scripted relay: feed frames in, observe what the client sent.
    fn scripted_stream() -> (
        LoginStream,
        tokio::sync::mpsc::Sender<Result<horde_proto::v1::LoginFrame, tonic::Status>>,
        tokio::sync::mpsc::Receiver<horde_proto::v1::LoginFrame>,
    ) {
        let (to_relay, sent) = tokio::sync::mpsc::channel(16);
        let (feed, from_relay) = tokio::sync::mpsc::channel(16);
        (
            LoginStream {
                to_relay,
                from_relay: Box::pin(ReceiverStream::new(from_relay)),
            },
            feed,
            sent,
        )
    }

    async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(10), future)
            .await
            .unwrap_or_else(|_| panic!("{what}: nothing within the ceiling"))
    }

    /// The callback table: both halves present and sane, or no callback.
    /// The mixed rows are unreachable through an honest relay — named and
    /// pinned because either alone realizes a listener no byte can cross.
    #[test]
    fn a_callback_needs_both_its_forward_and_a_real_port() {
        assert_eq!(
            callback_of(Some("fwd-login-1".to_owned()), 14555),
            Some(("fwd-login-1".to_owned(), 14555))
        );
        assert_eq!(callback_of(Some("fwd-login-1".to_owned()), 0), None);
        assert_eq!(callback_of(None, 14555), None);
        assert_eq!(callback_of(Some(String::new()), 14555), None);
        assert_eq!(
            callback_of(Some("fwd-login-1".to_owned()), 70_000),
            None,
            "a port that fits no u16 is no port"
        );
        assert_eq!(callback_of(None, 0), None);
    }

    /// The pump end to end over scripted channels: output paints (and
    /// flushes), keystrokes and resizes become frames, and the exit frame
    /// ends it with the code.
    #[tokio::test]
    async fn a_pumped_login_paints_types_resizes_and_exits() {
        let (stream, feed, mut sent) = scripted_stream();
        let (typed_tx, typed) = tokio::sync::mpsc::channel(4);
        let (resize_tx, resizes) = tokio::sync::mpsc::channel(4);

        let pump = tokio::spawn(async move {
            let mut painted = Vec::new();
            let end = pump_login(stream, typed, resizes, &mut painted).await;
            (end, painted)
        });

        feed.send(Ok(login_output_frame(b"account? ".to_vec())))
            .await
            .expect("feed");
        typed_tx
            .send(b"tester\r".to_vec())
            .await
            .expect("the input channel is open");
        let frame = within("the keystrokes become a frame", sent.recv())
            .await
            .expect("a frame");
        assert_eq!(
            frame,
            horde_proto::login::login_input_frame(b"tester\r".to_vec())
        );

        resize_tx.send((120, 40)).await.expect("resize sends");
        let frame = within("the resize becomes a frame", sent.recv())
            .await
            .expect("a frame");
        assert_eq!(frame, horde_proto::login::login_resize_frame(120, 40));

        feed.send(Ok(login_exit_frame(0))).await.expect("feed");
        let (end, painted) = within("the exit ends the pump", pump)
            .await
            .expect("no panic");
        assert_eq!(end, LoginEnd::Exited(0));
        assert_eq!(painted, b"account? ".to_vec());
    }

    /// A refusal mid-session and a dying stream each end the pump with
    /// their own verdict — the operator hears which happened.
    #[tokio::test]
    async fn refusals_and_dead_streams_end_the_pump_distinctly() {
        let (stream, feed, _sent) = scripted_stream();
        let (_typed_tx, typed) = tokio::sync::mpsc::channel(1);
        let (_resize_tx, resizes) = tokio::sync::mpsc::channel(1);
        feed.send(Ok(login_refusal_frame(
            LoginRefusalReason::WorkerUnreachable,
            "worker w1's tunnel ended",
        )))
        .await
        .expect("feed");
        let mut sink = Vec::new();
        let end = within(
            "the refusal ends the pump",
            pump_login(stream, typed, resizes, &mut sink),
        )
        .await;
        assert_eq!(
            end,
            LoginEnd::Refused("worker w1's tunnel ended".to_owned())
        );

        let (stream, feed, _sent) = scripted_stream();
        let (_typed_tx, typed) = tokio::sync::mpsc::channel(1);
        let (_resize_tx, resizes) = tokio::sync::mpsc::channel(1);
        drop(feed);
        let mut sink = Vec::new();
        let end = within(
            "a dead stream ends the pump",
            pump_login(stream, typed, resizes, &mut sink),
        )
        .await;
        assert_eq!(end, LoginEnd::ServerEnded);
    }

    /// The two operator lines, both formats — pinned because the e2e suite
    /// greps them and a reworded line is a broken scenario, not a nicety.
    #[test]
    fn the_operator_lines_name_what_matters() {
        let started = super::started_line(
            "w1",
            "fake",
            "login-1",
            Some(14555),
            crate::render::Format::Human,
        );
        assert!(started.contains("login started"), "{started}");
        assert!(started.contains("http://localhost:14555"), "{started}");

        let bare = super::started_line("w1", "fake", "login-1", None, crate::render::Format::Human);
        assert!(!bare.contains("callback"), "{bare}");

        let json: serde_json::Value = serde_json::from_str(&super::started_line(
            "w1",
            "fake",
            "login-1",
            Some(14555),
            crate::render::Format::Json,
        ))
        .expect("one JSON value");
        assert_eq!(json["login_id"], "login-1");
        assert_eq!(json["callback_port"], 14555);

        let exited = super::exited_line("login-1", 0, crate::render::Format::Human);
        assert!(exited.contains("exit code 0"), "{exited}");
        let json: serde_json::Value = serde_json::from_str(&super::exited_line(
            "login-1",
            0,
            crate::render::Format::Json,
        ))
        .expect("one JSON value");
        assert_eq!(json["exit_code"], 0);
    }

    /// The worker's leg over the real worker spine: attach, then whatever
    /// the test scripts, through `WorkerLoginTunnel` on a loopback server.
    /// The inbound half rides back with the sender — dropping it would
    /// cancel the RPC and take the pairing down with it.
    async fn worker_tunnel(
        endpoint: &str,
        frames: Vec<horde_proto::v1::LoginFrame>,
    ) -> (
        tokio::sync::mpsc::Sender<horde_proto::v1::LoginFrame>,
        tonic::Streaming<horde_proto::v1::WorkerLoginTunnelResponse>,
    ) {
        use tokio_stream::StreamExt as _;
        let mut worker = horde_proto::v1::worker_service_client::WorkerServiceClient::connect(
            endpoint.to_owned(),
        )
        .await
        .expect("the worker spine dials");
        let (to_relay, outbound) = tokio::sync::mpsc::channel::<horde_proto::v1::LoginFrame>(16);
        let requests = ReceiverStream::new(outbound)
            .map(|frame| horde_proto::v1::WorkerLoginTunnelRequest { frame: Some(frame) });
        let inbound = worker
            .worker_login_tunnel(requests)
            .await
            .expect("the tunnel opens")
            .into_inner();
        for frame in frames {
            to_relay.send(frame).await.expect("the frame sends");
        }
        (to_relay, inbound)
    }

    /// The whole client engine against the real relay over loopback:
    /// `open_login` hears the accept with the callback halves once the
    /// (test-played) worker attaches, and the pump then carries output and
    /// the exit code end to end.
    #[tokio::test]
    async fn an_opened_login_pumps_output_and_exit_through_the_real_relay() {
        let (core, mut client, endpoint) = crate::forward::testing::served_with_endpoint().await;
        let (_epoch, mut lane) = core.logins.register_worker("w1");

        let opening =
            tokio::spawn(async move { super::open_login(&mut client, "w1", "fake", 80, 24).await });
        let dispatched = within("the dispatch reaches the lane", lane.recv())
            .await
            .expect("a login request");
        assert_eq!(dispatched.provider, "fake");
        assert_eq!((dispatched.cols, dispatched.rows), (80, 24));

        let (worker_send, _worker_inbound) = worker_tunnel(
            &endpoint,
            vec![horde_proto::login::login_attach_frame(
                &dispatched.login_id,
                14555,
                0,
            )],
        )
        .await;

        let opened = within("the open resolves", opening)
            .await
            .expect("no panic")
            .expect("the login is accepted");
        assert_eq!(opened.login_id, dispatched.login_id);
        assert_eq!(
            opened.callback,
            Some((format!("fwd-{}", dispatched.login_id), 14555)),
            "the accept carries the derived forward id and the exact port"
        );

        // The command paints and exits; the pump ends with the code.
        worker_send
            .send(login_output_frame(b"visit the url\r\n".to_vec()))
            .await
            .expect("output sends");
        worker_send
            .send(login_exit_frame(0))
            .await
            .expect("exit sends");

        let (_typed_tx, typed) = tokio::sync::mpsc::channel(1);
        let (_resize_tx, resizes) = tokio::sync::mpsc::channel(1);
        let mut painted = Vec::new();
        let end = within(
            "the pump ends on the exit",
            pump_login(opened.stream, typed, resizes, &mut painted),
        )
        .await;
        assert_eq!(end, LoginEnd::Exited(0));
        assert_eq!(painted, b"visit the url\r\n".to_vec());
    }

    /// The refusal ladder over the real relay: an unknown worker refuses as
    /// unreachable, and a second login for a busy worker+provider refuses
    /// as already-in-progress — the operator reads both from the error.
    #[tokio::test]
    async fn opening_refuses_unreachable_and_busy_workers() {
        let (core, mut client, _endpoint) = crate::forward::testing::served_with_endpoint().await;

        let refused = super::open_login(&mut client, "w-none", "fake", 80, 24)
            .await
            .expect_err("no login stream, no login");
        assert!(
            refused.to_string().contains("no live login stream"),
            "{refused}"
        );

        let (_epoch, _lane) = core.logins.register_worker("w1");
        let busy = core.logins.begin("w1", "fake").expect("occupy the slot");
        let refused = super::open_login(&mut client, "w1", "fake", 80, 24)
            .await
            .expect_err("the slot is taken");
        assert!(
            refused.to_string().contains("already in progress"),
            "{refused}"
        );
        drop(busy);
    }
}
