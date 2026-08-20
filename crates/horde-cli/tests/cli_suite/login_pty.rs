//! The real `horde worker login` on a real terminal.
//!
//! The in-process engine tests prove the relay; what they cannot vouch for
//! is the terminal shell around it — the size the open frame carries, raw
//! mode actually engaging (typed bytes must reach the worker *without* the
//! tty echoing them locally), and the guard restoring the terminal before
//! the closing line prints. A pty is the one terminal a test can own, so
//! this suite spawns the actual binary inside one and plays the worker
//! through the real worker spine.

use horde_server::EventLog;
use horde_server::service::ServerCore;
use horde_server_testkit::pty::PtyProcess;
use tokio_stream::StreamExt;

/// A ceilinged receive for the worker-side channels this suite holds.
async fn within<T>(what: &str, future: impl Future<Output = T>) -> crate::common::Setup<T> {
    match tokio::time::timeout(std::time::Duration::from_secs(30), future).await {
        Ok(value) => Ok(value),
        Err(_elapsed) => Err(format!("{what}: nothing within the ceiling").into()),
    }
}

#[tokio::test]
async fn the_login_verb_goes_raw_relays_and_restores_the_terminal() {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let endpoint = horde_server_testkit::serve_core(core.clone())
        .await
        .expect("the server binds loopback");
    let (_epoch, mut lane) = core.logins.register_worker("w1");

    let mut command = portable_pty::CommandBuilder::new(env!("CARGO_BIN_EXE_horde"));
    for argument in ["--endpoint", &endpoint, "worker", "login", "w1", "fake"] {
        command.arg(argument);
    }
    let mut login = PtyProcess::spawn(command).expect("the binary spawns in a pty");

    // The open frame carries THIS terminal's size: the pty is 80×24, and
    // the dispatch the server heard is the assertion — a defaulted or
    // invented size would spawn the worker's PTY the wrong shape.
    let dispatched = within("the dispatch reaches the lane", lane.recv())
        .await
        .expect("the dispatch within the ceiling")
        .expect("a login request");
    assert_eq!(
        (dispatched.cols, dispatched.rows),
        (80, 24),
        "the open must carry the real terminal's size"
    );

    // Answer as the worker over the real worker spine: attach, no callback.
    let mut worker =
        horde_proto::v1::worker_service_client::WorkerServiceClient::connect(endpoint.clone())
            .await
            .expect("the worker spine dials");
    let (to_relay, outbound) = tokio::sync::mpsc::channel::<horde_proto::v1::LoginFrame>(16);
    let requests = tokio_stream::wrappers::ReceiverStream::new(outbound)
        .map(|frame| horde_proto::v1::WorkerLoginTunnelRequest { frame: Some(frame) });
    let mut inbound = worker
        .worker_login_tunnel(requests)
        .await
        .expect("the tunnel opens")
        .into_inner();
    to_relay
        .send(horde_proto::login::login_attach_frame(
            &dispatched.login_id,
            0,
            0,
        ))
        .await
        .expect("the attach sends");

    login
        .await_painted("login started")
        .await
        .expect("the started line prints before the terminal goes raw");

    // The sync point raw mode needs: pump output only paints inside the
    // raw-mode block, so once the prompt shows, raw is provably on — typing
    // before that would race the guard and land on a cooked terminal.
    to_relay
        .send(horde_proto::login::login_output_frame(b"prompt> ".to_vec()))
        .await
        .expect("the prompt sends");
    login
        .await_painted("prompt>")
        .await
        .expect("pump output reaches the raw terminal");

    // Raw mode, proven from both ends: the typed marker must arrive at the
    // worker leg byte-for-byte (a cooked tty would have translated the
    // carriage return), and must NOT paint locally — raw has echo off, and
    // a login relaying an OAuth ceremony must not echo what the operator
    // types over the provider's own rendering.
    login
        .type_bytes(b"MARKER-ZQ\r")
        .expect("typing reaches the pty");
    let typed = within("the keystrokes reach the worker leg", async {
        loop {
            let message = inbound
                .message()
                .await
                .expect("the tunnel stays up")
                .expect("the tunnel does not end early");
            if let Some(horde_proto::v1::login_frame::Frame::Input(bytes)) =
                message.frame.and_then(|frame| frame.frame)
            {
                return bytes;
            }
        }
    })
    .await
    .expect("the input frame within the ceiling");
    assert_eq!(typed, b"MARKER-ZQ\r".to_vec());
    assert!(
        !login.seen.contains("MARKER-ZQ"),
        "raw mode has local echo off; the tty must not have painted the \
         marker itself: {:?}",
        login.seen
    );

    // The command finishes; the exit code rides home. The closing line must
    // paint as `\r\n`-terminated — the guard restored the terminal (OPOST
    // back on) before anything printed; a terminal left raw would emit the
    // bare `\n` the mutation of the guard's Drop leaves behind.
    to_relay
        .send(horde_proto::login::login_output_frame(b"done\r\n".to_vec()))
        .await
        .expect("the output sends");
    to_relay
        .send(horde_proto::login::login_exit_frame(0))
        .await
        .expect("the exit sends");
    drop(to_relay);

    login
        .await_painted("login finished with exit code 0\r\n")
        .await
        .expect("the exited line prints on a restored terminal");
    let status = login.exit_status().await.expect("the CLI exits");
    assert!(status.success(), "exit 0 is the verb's own success");
}
