//! The interactive-auth relay over the whole real system (DESIGN §4): the
//! real `horde worker login` inside a real pty, attached through the server
//! relay to the scripted provider login command running in a PTY inside a
//! real worker daemon — prompt out, keystrokes in, the OAuth callback
//! carried client → server → worker over the ephemeral expose, exit code
//! home, every hop a real process on a real socket under a real
//! certificate. Then the faults the spec names: a client that vanishes
//! mid-login leaves no orphan (the worker's own reap line is the witness —
//! never an assertion about OS process or port state), and a second login
//! for a busy worker+provider is refused.

use std::io::{Read, Write};

use crate::support::System;

/// A `horde worker login` process running inside a pty.
struct PtyLogin {
    /// Chunks of terminal output, as the child paints them.
    output: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    /// Everything read so far, accumulated across waits.
    seen: String,
    writer: Box<dyn Write + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
}

/// Spawns the real CLI's login verb in an 80×24 pty.
fn spawn_pty_login(system: &System, worker: &str, provider: &str) -> PtyLogin {
    let pair = portable_pty::native_pty_system()
        .openpty(portable_pty::PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("a pty opens");

    let mut command = portable_pty::CommandBuilder::new(system.horde_binary());
    for argument in system.operator_args() {
        command.arg(argument);
    }
    for argument in ["worker", "login", worker, provider] {
        command.arg(argument);
    }
    command.env("TERM", "xterm-256color");

    let child = pair.slave.spawn_command(command).expect("the CLI spawns");
    // The slave side belongs to the child now; holding it open here would
    // keep the pty from ever reporting EOF.
    drop(pair.slave);

    let mut reader = pair.master.try_clone_reader().expect("a reader");
    let writer = pair.master.take_writer().expect("a writer");
    let (sender, output) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        while let Ok(count) = reader.read(&mut buffer) {
            if count == 0 || sender.send(buffer[..count].to_vec()).is_err() {
                return;
            }
        }
    });

    PtyLogin {
        output,
        seen: String::new(),
        writer,
        child,
    }
}

impl PtyLogin {
    /// Reads until the terminal has shown `needle`, within a ceiling.
    async fn await_painted(&mut self, needle: &str) {
        let deadline = std::time::Duration::from_secs(30);
        let step = async {
            while !self.seen.contains(needle) {
                match self.output.recv().await {
                    Some(chunk) => self.seen.push_str(&String::from_utf8_lossy(&chunk)),
                    None => panic!(
                        "the pty closed before painting {needle:?}; saw: {:?}",
                        self.seen
                    ),
                }
            }
        };
        tokio::time::timeout(deadline, step)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{needle:?} never painted within the ceiling; saw: {:?}",
                    self.seen
                )
            });
    }

    /// Types at the child, as an operator would.
    fn type_str(&mut self, typed: &str) {
        self.writer
            .write_all(typed.as_bytes())
            .and_then(|()| self.writer.flush())
            .expect("the pty accepts input");
    }

    /// Waits (bounded, on a blocking thread) for the child to exit.
    async fn await_exit(mut self) -> portable_pty::ExitStatus {
        let waited =
            tokio::task::spawn_blocking(move || self.child.wait().expect("the wait returns"));
        tokio::time::timeout(std::time::Duration::from_secs(30), waited)
            .await
            .expect("the CLI must exit rather than hang")
            .expect("the wait thread must not panic")
    }
}

/// Two distinct free loopback ports, found by binding `:0` twice and
/// releasing. Both listeners are held until both ports are known, so the
/// two can never collide with each other.
fn two_free_ports() -> (u16, u16) {
    let first = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
    let second = std::net::TcpListener::bind("127.0.0.1:0").expect("a second free port");
    (
        first.local_addr().expect("an addr").port(),
        second.local_addr().expect("an addr").port(),
    )
}

/// Starts a worker whose fake adapter carries the scripted login recipe.
/// The callback (browser-facing) and listen (worker-side) ports are split
/// because everything here shares one machine — one port cannot be both the
/// client's listener and the fake provider's.
async fn spawn_login_worker(
    system: &System,
    name: &str,
    callback: u16,
    listen: u16,
) -> horde_server_testkit::process::SpawnedProcess {
    let fake_login = horde_server_testkit::process::product_binary("horde-e2e", "fake-login")
        .await
        .expect("the fake-login binary exists");
    system
        .spawn_worker(
            name,
            &system.state.path().join(name),
            &[
                "--fake-login",
                &fake_login.to_string_lossy(),
                "--fake-login-callback-port",
                &callback.to_string(),
                "--fake-login-listen-port",
                &listen.to_string(),
            ],
        )
        .await
}

/// One HTTP GET against the client-side callback listener — the browser's
/// part, played by the test. The response body comes back through client →
/// server → worker → fake-login and proves the whole path.
async fn hit_callback(port: u16) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut browser = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::TcpStream::connect(("127.0.0.1", port)),
    )
    .await
    .expect("the callback dial must finish")
    .expect("the client-side callback listener accepts");
    browser
        .write_all(b"GET /auth HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
        .await
        .expect("the request sends");
    let mut response = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        browser.read_to_end(&mut response),
    )
    .await
    .expect("the callback answer must arrive rather than hang")
    .expect("the response reads");
    String::from_utf8_lossy(&response).into_owned()
}

#[tokio::test]
async fn an_interactive_login_relays_the_pty_and_its_oauth_callback() {
    let system = System::launch().await;
    let (callback, listen) = two_free_ports();
    let worker = spawn_login_worker(&system, "w-login", callback, listen).await;

    let mut login = spawn_pty_login(&system, "w-login", "fake");

    // The prompt was painted by the fake provider on the WORKER and crossed
    // the relay into this pty.
    login.await_painted("account?").await;
    login.type_str("tester\r");

    // The provider's flow reaches its browser step; the URL names the
    // browser-facing port, which the client realized locally.
    login
        .await_painted(&format!("visit http://localhost:{callback}/auth"))
        .await;

    // The browser's callback, played from the client machine: through the
    // ephemeral expose to the fake provider's listener on the worker.
    let response = hit_callback(callback).await;
    assert!(
        response.contains("signed-in as tester"),
        "the callback answer must come from the worker-side flow: {response}"
    );

    // The flow completes; the exit code rides home; the CLI says so.
    login.await_painted("login complete for tester").await;
    login.await_painted("login finished with exit code 0").await;
    let status = login.await_exit().await;
    assert!(status.success(), "the login verb exits 0 on success");

    // The worker's own account of the same story.
    worker
        .await_stdout("the spawn and exit lines", |out| {
            out.contains("login spawned") && out.contains("login exited") && out.contains("code=0")
        })
        .await
        .expect("the worker announces the login's life");
}

#[tokio::test]
async fn a_vanished_client_leaves_no_orphan_and_a_busy_provider_refuses() {
    let system = System::launch().await;
    let (callback, listen) = two_free_ports();
    let worker = spawn_login_worker(&system, "w-login2", callback, listen).await;

    // The first login, headless (piped stdio, no pty): it opens, the worker
    // spawns the flow, and the flow parks at its account prompt.
    let first = system.spawn_cli("horde[login-1]", &["worker", "login", "w-login2", "fake"]);
    first
        .await_stdout("the started line", |out| out.contains("login_id"))
        .await
        .expect("the first login starts");
    worker
        .await_stdout("the spawn line", |out| out.contains("login spawned"))
        .await
        .expect("the worker spawned the flow");

    // The refusal the spec demands: one login per worker+provider at a time.
    let refused = system.cli(&["worker", "login", "w-login2", "fake"]).await;
    assert!(
        !refused.status.success(),
        "a second login for a busy worker+provider must refuse"
    );
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("already in progress"),
        "the refusal names the reason: {}",
        String::from_utf8_lossy(&refused.stderr)
    );

    // The fault: the client dies mid-login, uncleanly.
    let mut first = first;
    first.kill().await.expect("the client dies on demand");

    // The owned witness that no orphan survived: the worker's reap line is
    // printed only after its wait() on the killed child returned. No OS
    // process table, no port state — the worker's own account.
    worker
        .await_stdout("the reap witness", |out| {
            out.contains("login reaped") && out.contains("client-vanished")
        })
        .await
        .expect("the worker kills and reaps the abandoned login");

    // And the slot is free again: a fresh login starts rather than refusing.
    let again = system.spawn_cli("horde[login-3]", &["worker", "login", "w-login2", "fake"]);
    again
        .await_stdout("the fresh started line", |out| out.contains("login_id"))
        .await
        .expect("the freed slot admits a new login");
    let mut again = again;
    again.kill().await.expect("cleanup");
}
