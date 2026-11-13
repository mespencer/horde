//! Worker-side interactive-auth relay (DESIGN §4): the PTY half.
//!
//! The Server pushes one `LoginSessionRequest` per login a Client opened
//! against this Worker; this module answers each by spawning the provider's
//! login recipe in a PTY and relaying bytes over a dedicated tunnel. The
//! tunnel's life IS the login's: when it ends — client detach, server loss,
//! partition — the child is killed **and reaped**, and the reap is announced
//! on stdout (`login reaped …`) so the fault suites have an owned witness
//! that no orphan survived, without ever asserting OS process state.
//!
//! Login sessions are spawned as detached tasks rather than into the
//! connection's own scope, deliberately: a task aborted mid-kill would leak
//! the child it was about to reap. Each task owns its tunnel stream, and the
//! stream dying with the connection is what ends the task — through the
//! kill-and-reap path, never around it.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::Arc;

use horde_adapter::{Adapter, LoginRecipe};
use horde_proto::login::{
    login_attach_frame, login_exit_frame, login_output_frame, login_refusal_frame,
};
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::{
    LoginFrame, LoginRefusalReason, LoginSessionRequest, RegisterWorkerRequest,
    StreamLoginsRequest, login_frame::Frame,
};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use crate::error::WorkerError;

/// Operator lines out. `Arc` because every login session announces through
/// the same sink concurrently.
pub type Announce = Arc<dyn Fn(String) + Send + Sync>;

/// How long a killed login command may take to be reaped before the wait is
/// declared failed. SIGKILL-class prompt; the ceiling guards a wedged wait.
const REAP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

/// How many trailing output chunks are drained after the command exits. A
/// count bound — a clock cannot bound a loop that reads without waiting.
const DRAIN_CHUNK_BOUND: u32 = 256;

/// The terminal size a request that names none gets.
const DEFAULT_COLS: u16 = 80;
const DEFAULT_ROWS: u16 = 24;

/// What this Worker knows about signing providers in: the recipes its
/// registered adapters carry, and where operator lines go.
pub struct LoginPlane {
    recipes: BTreeMap<String, LoginRecipe>,
    announce: Announce,
    /// Woken by every login that exits cleanly: what it signed in may be an
    /// account placement can now match, so the advertisement is refreshed.
    signed_in: tokio::sync::Notify,
    /// The instances a re-advertisement reports, rescanned rather than
    /// carried over from the dial — a re-registration replaces the Server's
    /// view of this Worker's instances wholesale.
    workspaces: Option<crate::WorkspaceManager>,
}

impl std::fmt::Debug for LoginPlane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LoginPlane")
            .field("recipes", &self.recipes.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl LoginPlane {
    /// A plane over explicit recipes — the test constructor.
    #[must_use]
    pub fn new(recipes: BTreeMap<String, LoginRecipe>, announce: Announce) -> Self {
        Self {
            recipes,
            announce,
            signed_in: tokio::sync::Notify::new(),
            workspaces: None,
        }
    }

    /// Re-advertisements report the instances `workspaces` holds on disk.
    #[must_use]
    pub fn with_workspaces(mut self, workspaces: Option<crate::WorkspaceManager>) -> Self {
        self.workspaces = workspaces;
        self
    }

    /// The provider accounts this Worker holds a sign-in for — the
    /// advertisement's `provider_accounts`. Read from the very directories a
    /// per-account login signs in (`<profile>/accounts/<account>`, ADR-0010)
    /// and an agent bound to that account launches under, for every provider
    /// whose recipe signs in per account.
    #[must_use]
    pub fn held_accounts(&self) -> Vec<horde_proto::v1::ProviderAccountCapability> {
        self.recipes
            .iter()
            .filter_map(|(provider, recipe)| {
                recipe
                    .accounts
                    .as_ref()
                    .map(|(_, root)| (provider, crate::harness::HarnessProfile::new(root)))
            })
            .flat_map(|(provider, profile)| {
                profile.signed_in_accounts().into_iter().map(|account| {
                    horde_proto::v1::ProviderAccountCapability {
                        provider: provider.clone(),
                        account,
                        ..horde_proto::v1::ProviderAccountCapability::default()
                    }
                })
            })
            .collect()
    }

    /// A plane over the registered adapters' recipes — the daemon path. An
    /// adapter without a recipe simply is not loginable here, which the
    /// tunnel refuses honestly per attempt.
    #[must_use]
    pub fn from_adapters<'a>(
        adapters: impl IntoIterator<Item = &'a dyn Adapter>,
        announce: Announce,
    ) -> Self {
        let recipes = adapters
            .into_iter()
            .filter_map(|adapter| {
                adapter
                    .login_recipe()
                    .map(|recipe| (adapter.provider().to_owned(), recipe))
            })
            .collect();
        Self::new(recipes, announce)
    }

    /// A plane whose operator lines go nowhere — for in-process test
    /// workers, which have no stdout an operator reads.
    #[must_use]
    pub fn discarding<'a>(adapters: impl IntoIterator<Item = &'a dyn Adapter>) -> Arc<Self> {
        Arc::new(Self::from_adapters(adapters, Arc::new(|_line| {})))
    }

    /// The recipe a provider name resolves to, if this Worker can log it in.
    #[must_use]
    pub fn recipe_for(&self, provider: &str) -> Option<&LoginRecipe> {
        self.recipes.get(provider)
    }

    /// The recipe one dispatched login runs: the provider's, and for an
    /// `account`, that recipe retargeted at the account's profile directory
    /// (ADR-0010's `<root>/accounts/<account>`, prepared owner-only) — the
    /// very directory an agent bound to that account launches under.
    ///
    /// # Errors
    ///
    /// `(reason, announce tag, detail)`: no recipe for the provider; a
    /// recipe that cannot sign in per account; or an account name that is
    /// not one plain directory component, refused rather than joined.
    pub fn resolve(
        &self,
        provider: &str,
        account: Option<&str>,
    ) -> Result<LoginRecipe, (LoginRefusalReason, &'static str, String)> {
        let mut recipe = self.recipe_for(provider).cloned().ok_or_else(|| {
            (
                LoginRefusalReason::UnknownProvider,
                "unknown-provider",
                format!("this worker has no login recipe for {provider}"),
            )
        })?;
        let Some(account) = account else {
            return Ok(recipe);
        };
        let (variable, root) = recipe.accounts.clone().ok_or_else(|| {
            (
                LoginRefusalReason::UnknownProvider,
                "no-accounts",
                format!("{provider} on this worker cannot sign in per account"),
            )
        })?;
        let dir = crate::harness::HarnessProfile::new(root)
            .config_dir(Some(&horde_adapter::ProviderAccount::new(account)))
            .map_err(|detail| (LoginRefusalReason::SpawnFailed, "bad-account", detail))?;
        recipe.env.retain(|(name, _)| *name != variable);
        recipe.env.push((variable, dir.display().to_string()));
        Ok(recipe)
    }
}

/// Runs the login plane over one connection until the Server ends it.
///
/// # Errors
///
/// The stream's failure. Ending cleanly is `Ok` — reconnection is the
/// daemon's policy, exactly as for the other three streams.
pub async fn serve(
    mut client: WorkerServiceClient<Channel>,
    worker_id: &str,
    registered: &RegisterWorkerRequest,
    plane: Arc<LoginPlane>,
) -> Result<(), WorkerError> {
    let mut stream = client
        .stream_logins(StreamLoginsRequest {
            worker_id: Some(horde_proto::v1::WorkerId::new(worker_id)),
        })
        .await?
        .into_inner();

    loop {
        tokio::select! {
            update = stream.message() => {
                let Some(update) = update? else {
                    return Ok(());
                };
                let Some(request) = update.request else {
                    continue;
                };
                // Detached on purpose (module docs): the session ends when
                // its own tunnel does, through the kill-and-reap path — an
                // abort here would leak the child mid-kill.
                tokio::spawn(handle_login(client.clone(), request, Arc::clone(&plane)));
            }
            () = plane.signed_in.notified() => {
                readvertise(&mut client, registered, &plane).await?;
            }
        }
    }
}

/// Registers again with what a login may have changed — the accounts held,
/// and the instances on disk now — so a spawn requiring the account just
/// signed in is placeable without waiting for a reconnect. The rest is the
/// registration this connection opened with; the journal head is left
/// unset, because the regression check belongs to the dial, and a head
/// carried from then would only be stale.
async fn readvertise(
    client: &mut WorkerServiceClient<Channel>,
    registered: &RegisterWorkerRequest,
    plane: &LoginPlane,
) -> Result<(), WorkerError> {
    let mut request = registered.clone();
    request.last_local_journal_sequence = None;
    let capabilities = request.capabilities.get_or_insert_default();
    capabilities.provider_accounts = plane.held_accounts();
    if let Some(workspaces) = &plane.workspaces {
        capabilities.warm_instances = workspaces.warm_instances().await;
    }
    let accounts = capabilities.provider_accounts.len();
    client.register_worker(request).await?;
    (plane.announce)(format!("readvertised provider_accounts={accounts}"));
    Ok(())
}

/// One login tunnel leg on the worker spine, stripped to bare frames.
struct LoginTunnel {
    to_relay: tokio::sync::mpsc::Sender<LoginFrame>,
    from_relay: std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<LoginFrame, tonic::Status>> + Send>,
    >,
}

impl LoginTunnel {
    async fn open(client: &mut WorkerServiceClient<Channel>) -> Result<Self, WorkerError> {
        let (to_relay, outbound) = tokio::sync::mpsc::channel(16);
        let requests = tokio_stream::wrappers::ReceiverStream::new(outbound)
            .map(|frame| horde_proto::v1::WorkerLoginTunnelRequest { frame: Some(frame) });
        let inbound = client.worker_login_tunnel(requests).await?.into_inner();
        Ok(Self {
            to_relay,
            from_relay: Box::pin(
                inbound.map(|message| message.map(|reply| reply.frame.unwrap_or_default())),
            ),
        })
    }

    /// Ends this leg gracefully: the send half closes cleanly — which is
    /// what actually transmits any still-queued frames — and the receive
    /// half is drained (count-bounded, ceilinged) until the relay closes it.
    /// The same lesson the forward tunnel learned the hard way.
    async fn finish(self) {
        drop(self.to_relay);
        let mut trailing = self.from_relay.take(64);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while trailing.next().await.is_some() {}
        })
        .await;
    }
}

/// Opens the tunnel for one dispatched login and answers it.
async fn handle_login(
    mut client: WorkerServiceClient<Channel>,
    request: LoginSessionRequest,
    plane: Arc<LoginPlane>,
) {
    let Ok(tunnel) = LoginTunnel::open(&mut client).await else {
        return; // No relay leg: the client's pairing deadline refuses it.
    };
    answer_dispatch(tunnel, request, &plane).await;
}

/// Answers one dispatched login over an open tunnel, whatever happens:
/// recipe missing, spawn failure, a full PTY session, a client that
/// vanishes. Split from the tunnel-open so the refusal and supervision
/// paths are drivable over scripted channels.
async fn answer_dispatch(tunnel: LoginTunnel, request: LoginSessionRequest, plane: &LoginPlane) {
    let login_id = request.login_id.clone();

    let recipe = match plane.resolve(&request.provider, request.account.as_deref()) {
        Ok(recipe) => recipe,
        Err((reason, tag, detail)) => {
            // The attach carries the claim; the refusal follows it (the
            // wire's first tunnel frame is always the attach, because the
            // claim needs its login id).
            let _ = tunnel
                .to_relay
                .send(login_attach_frame(&login_id, 0, 0))
                .await;
            let _ = tunnel
                .to_relay
                .send(login_refusal_frame(reason, detail))
                .await;
            (plane.announce)(format!(
                "login refused login_id={login_id} provider={} reason={tag}",
                request.provider
            ));
            return tunnel.finish().await;
        }
    };

    let callback = recipe.callback_port.map(u32::from).unwrap_or_default();
    let listen = recipe.listen_port.map(u32::from).unwrap_or_default();
    if tunnel
        .to_relay
        .send(login_attach_frame(&login_id, callback, listen))
        .await
        .is_err()
    {
        return;
    }

    let spawned = spawn_login_pty(&recipe, size_of(&request));
    let session = match spawned {
        Ok(session) => session,
        Err(error) => {
            let _ = tunnel
                .to_relay
                .send(login_refusal_frame(
                    LoginRefusalReason::SpawnFailed,
                    format!("could not spawn {}: {error}", recipe.program),
                ))
                .await;
            (plane.announce)(format!(
                "login refused login_id={login_id} provider={} reason=spawn-failed",
                request.provider
            ));
            return tunnel.finish().await;
        }
    };
    (plane.announce)(format!(
        "login spawned login_id={login_id} provider={}",
        request.provider
    ));

    let exited = pump_login(
        tunnel,
        session,
        &login_id,
        &request.provider,
        &plane.announce,
    )
    .await;
    if exited == Some(0) {
        plane.signed_in.notify_one();
    }
}

/// The PTY size a request asks for, defaulted and clamped to what a PTY can
/// actually be.
fn size_of(request: &LoginSessionRequest) -> PtySize {
    let dimension = |wanted: u32, fallback: u16| match u16::try_from(wanted) {
        Ok(0) | Err(_) => fallback,
        Ok(size) => size,
    };
    PtySize {
        rows: dimension(request.rows, DEFAULT_ROWS),
        cols: dimension(request.cols, DEFAULT_COLS),
        pixel_width: 0,
        pixel_height: 0,
    }
}

/// The PTY's controlling side, kept for resizes.
#[cfg(not(windows))]
type Master = Box<dyn portable_pty::MasterPty + Send>;

/// The PTY's controlling side on Windows: a ConPTY, shared with the reaping
/// wait, which closes it the moment the command exits. A ConPTY's output
/// pipe reports EOF only when the pseudoconsole itself closes — never when
/// its child exits — so without this the pump would wait for an EOF that
/// cannot come (ADR-0017).
#[cfg(windows)]
type Master = Arc<std::sync::Mutex<Option<Box<dyn portable_pty::MasterPty + Send>>>>;

/// A spawned login command and every handle its supervision needs.
struct PtySession {
    master: Master,
    /// PTY output chunks, read by a dedicated thread; the channel closing is
    /// the EOF that follows the command's exit.
    output: tokio::sync::mpsc::Receiver<Vec<u8>>,
    /// Keystrokes toward the PTY, written by a dedicated thread so a full
    /// PTY buffer can never wedge the async pump.
    input: std::sync::mpsc::Sender<Vec<u8>>,
    /// Kills by handle — never by name or pattern.
    killer: Box<dyn portable_pty::ChildKiller + Send + Sync>,
    /// Resolves when the child is reaped, with its exit code. The wait runs
    /// on a blocking thread from the moment of spawn, so the reap needs no
    /// separate arrangement — awaiting this IS the reap witness.
    reaped: tokio::task::JoinHandle<Option<u32>>,
}

/// Spawns the recipe in a fresh PTY with its supervision threads.
fn spawn_login_pty(recipe: &LoginRecipe, size: PtySize) -> Result<PtySession, String> {
    let pair = native_pty_system()
        .openpty(size)
        .map_err(|error| format!("openpty failed: {error}"))?;

    let mut command = CommandBuilder::new(&recipe.program);
    for argument in &recipe.args {
        command.arg(argument);
    }
    // A daemon has no terminal of its own to inherit TERM from, and a login
    // flow that paints prompts needs one.
    command.env("TERM", "xterm-256color");
    // The directory the login signs in: the Worker-owned one its agents run
    // under, never the operator's own (ADR-0010).
    for (key, value) in &recipe.env {
        command.env(key, value);
    }

    let mut child = pair
        .slave
        .spawn_command(command)
        .map_err(|error| error.to_string())?;
    // The slave side belongs to the child now; holding it open here would
    // keep the pty from ever reporting EOF.
    drop(pair.slave);

    let killer = child.clone_killer();

    let mut reader = pair
        .master
        .try_clone_reader()
        .map_err(|error| error.to_string())?;
    let (output_tx, output) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            // EOF and error retire the reader alike — a closed pty reports
            // either, platform's choice — and only real bytes are worth a
            // send; a closed pump retires it too.
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(count) => {
                    if output_tx.blocking_send(buffer[..count].to_vec()).is_err() {
                        return;
                    }
                }
            }
        }
    });

    let mut writer = pair
        .master
        .take_writer()
        .map_err(|error| error.to_string())?;

    #[cfg(not(windows))]
    let master = pair.master;
    #[cfg(not(windows))]
    let reaped =
        tokio::task::spawn_blocking(move || child.wait().ok().map(|status| status.exit_code()));
    #[cfg(windows)]
    let master: Master = Arc::new(std::sync::Mutex::new(Some(pair.master)));
    #[cfg(windows)]
    let reaped = {
        let closing = Arc::clone(&master);
        tokio::task::spawn_blocking(move || {
            let code = child.wait().ok().map(|status| status.exit_code());
            // Close the pseudoconsole so the output pipe reports EOF.
            drop(
                closing
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take(),
            );
            code
        })
    };
    let (input, input_rx) = std::sync::mpsc::channel::<Vec<u8>>();
    std::thread::spawn(move || {
        while let Ok(bytes) = input_rx.recv() {
            // Two retirements, stated separately: a failed write and a
            // failed flush each mean the PTY is gone, and neither may be
            // asked to vouch for the other.
            if writer.write_all(&bytes).is_err() {
                return;
            }
            if writer.flush().is_err() {
                return;
            }
        }
    });

    Ok(PtySession {
        master,
        output,
        input,
        killer,
        reaped,
    })
}

/// Relays until the command exits or the tunnel dies, then reaps either way.
/// Returns the command's exit code when it exited on its own; `None` when it
/// was killed because its tunnel died, or its code is unknown.
async fn pump_login(
    mut tunnel: LoginTunnel,
    mut session: PtySession,
    login_id: &str,
    provider: &str,
    announce: &Announce,
) -> Option<u32> {
    loop {
        tokio::select! {
            frame = tunnel.from_relay.next() => {
                match frame {
                    Some(Ok(LoginFrame { frame: Some(Frame::Input(bytes)) })) => {
                        if session.input.send(bytes).is_err() {
                            // The writer thread died with the PTY; the exit
                            // is on its way through the output channel.
                        }
                    }
                    Some(Ok(LoginFrame { frame: Some(Frame::Resize(resize)) })) => {
                        resize_pty(&session.master, PtySize {
                            rows: u16::try_from(resize.rows).unwrap_or(DEFAULT_ROWS),
                            cols: u16::try_from(resize.cols).unwrap_or(DEFAULT_COLS),
                            pixel_width: 0,
                            pixel_height: 0,
                        });
                    }
                    Some(Ok(_)) => {}
                    // The client (or the whole connection) vanished: kill
                    // and reap, and say so — the fault suites' owned witness.
                    Some(Err(_)) | None => {
                        let code = kill_and_reap(&mut session).await;
                        announce(format!(
                            "login reaped login_id={login_id} provider={provider} \
                             reason=client-vanished exit={}",
                            code.map_or_else(|| "unknown".to_owned(), |code| code.to_string())
                        ));
                        return None;
                    }
                }
            }
            chunk = session.output.recv() => {
                match chunk {
                    Some(bytes) => {
                        if tunnel.to_relay.send(login_output_frame(bytes)).await.is_err() {
                            let code = kill_and_reap(&mut session).await;
                            announce(format!(
                                "login reaped login_id={login_id} provider={provider} \
                                 reason=relay-gone exit={}",
                                code.map_or_else(|| "unknown".to_owned(), |code| code.to_string())
                            ));
                            return None;
                        }
                    }
                    // PTY EOF: the command is done. Reap, report, finish.
                    None => {
                        let code = await_reap(&mut session.reaped).await;
                        let _ = tunnel
                            .to_relay
                            .send(login_exit_frame(code.unwrap_or(u32::MAX)))
                            .await;
                        announce(format!(
                            "login exited login_id={login_id} provider={provider} code={}",
                            code.map_or_else(|| "unknown".to_owned(), |code| code.to_string())
                        ));
                        tunnel.finish().await;
                        return code;
                    }
                }
            }
        }
    }
}

/// Resizes the PTY, best effort: a PTY already gone has nothing to resize.
/// On Windows the ConPTY is resized only while it is open
/// ([`crate::windows::resize_pty`]).
fn resize_pty(master: &Master, size: PtySize) {
    #[cfg(windows)]
    crate::windows::resize_pty(master, size);
    #[cfg(not(windows))]
    let _ = master.resize(size);
}

/// Kills the child by handle and waits for the reap, draining whatever
/// output the PTY still held (count-bounded by the loop's own range — a
/// clock cannot bound a loop that reads without waiting) so the reader
/// thread can end.
async fn kill_and_reap(session: &mut PtySession) -> Option<u32> {
    let _ = session.killer.kill();
    for _ in 0..DRAIN_CHUNK_BOUND {
        if session.output.try_recv().is_err() {
            break;
        }
    }
    await_reap(&mut session.reaped).await
}

/// Awaits the blocking wait's verdict, ceilinged. `None` means the wait
/// failed or timed out — announced as `unknown` rather than invented.
async fn await_reap(reaped: &mut tokio::task::JoinHandle<Option<u32>>) -> Option<u32> {
    match tokio::time::timeout(REAP_DEADLINE, reaped).await {
        Ok(Ok(code)) => code,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use horde_adapter::LoginRecipe;
    use horde_proto::v1::{
        LoginFrame, LoginRefusalReason, LoginSessionRequest, login_frame::Frame,
    };
    use portable_pty::PtySize;

    use super::{LoginPlane, spawn_login_pty};

    /// An announce sink the test can read back.
    fn announcing() -> (super::Announce, std::sync::mpsc::Receiver<String>) {
        let (tx, rx) = std::sync::mpsc::channel();
        let tx = std::sync::Mutex::new(tx);
        (
            Arc::new(move |line| {
                if let Ok(sender) = tx.lock() {
                    let _ = sender.send(line);
                }
            }),
            rx,
        )
    }

    /// A ceilinged wait for this suite, on the reap deadline's own budget —
    /// the slowest thing any of these tests wait on is a reap.
    async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
        match tokio::time::timeout(super::REAP_DEADLINE, future).await {
            Ok(value) => value,
            Err(_elapsed) => panic!("{what}: nothing within the ceiling"),
        }
    }

    #[test]
    fn the_plane_resolves_exactly_the_recipes_its_adapters_carry() {
        let (announce, _lines) = announcing();
        let fake = horde_adapter_fake::FakeAdapter::default().with_login_recipe(
            LoginRecipe::with_callback("/bin/echo", vec!["hi".to_owned()], 14555),
        );
        let bare = horde_adapter_fake::FakeAdapter::default().with_provider("bare");
        let plane = LoginPlane::from_adapters(
            [
                &fake as &dyn horde_adapter::Adapter,
                &bare as &dyn horde_adapter::Adapter,
            ],
            announce,
        );

        let recipe = plane.recipe_for("fake").expect("the fake carries one");
        assert_eq!(recipe.callback_port, Some(14555));
        assert!(
            plane.recipe_for("bare").is_none(),
            "an adapter without a recipe is not loginable"
        );
        assert!(plane.recipe_for("claude").is_none());
        assert!(
            format!("{plane:?}").contains("fake"),
            "the debug view names what is loginable"
        );
    }

    /// Per-account resolution: no account keeps the recipe as published; an
    /// account retargets the config variable at `accounts/<account>` under
    /// the profile, created owner-only; a recipe without account support and
    /// an account that is not a directory name are each refused by name.
    #[test]
    fn an_account_login_targets_that_accounts_profile_directory() {
        let scratch = tempfile::tempdir().expect("a tempdir");
        let profile = scratch.path().join("claude");
        let per_account = LoginRecipe::terminal_only("claude", vec!["login".to_owned()])
            .with_env(vec![
                ("KEEP".to_owned(), "kept".to_owned()),
                ("CLAUDE_CONFIG_DIR".to_owned(), "/default".to_owned()),
            ])
            .with_accounts("CLAUDE_CONFIG_DIR", profile.clone());
        let plane = LoginPlane::new(
            [
                ("claude".to_owned(), per_account.clone()),
                (
                    "fake".to_owned(),
                    LoginRecipe::terminal_only("fake", Vec::new()),
                ),
            ]
            .into_iter()
            .collect(),
            Arc::new(|_line| {}),
        );

        assert_eq!(
            plane.resolve("claude", None).expect("the default resolves"),
            per_account
        );
        let resolved = plane
            .resolve("claude", Some("work"))
            .expect("an account resolves");
        let dir = profile.join("accounts").join("work");
        assert_eq!(
            resolved.env,
            vec![
                ("KEEP".to_owned(), "kept".to_owned()),
                ("CLAUDE_CONFIG_DIR".to_owned(), dir.display().to_string()),
            ]
        );
        assert!(dir.is_dir(), "the directory is prepared");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&dir)
                .expect("the directory is prepared")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o700);
        }

        // Prepared is not signed in; once the login writes there, the
        // account is advertised under the provider whose recipe signs it in.
        assert!(plane.held_accounts().is_empty());
        std::fs::write(dir.join(".credentials.json"), "{}").expect("a sign-in");
        assert_eq!(
            plane.held_accounts(),
            vec![horde_proto::v1::ProviderAccountCapability {
                provider: "claude".to_owned(),
                account: "work".to_owned(),
                ..horde_proto::v1::ProviderAccountCapability::default()
            }]
        );

        let refused = |provider: &str, account: Option<&str>| {
            plane
                .resolve(provider, account)
                .expect_err("this login must be refused")
        };
        let (reason, tag, detail) = refused("codex", None);
        assert_eq!(
            (reason, tag),
            (LoginRefusalReason::UnknownProvider, "unknown-provider")
        );
        assert!(detail.contains("codex"));
        let (reason, tag, _) = refused("fake", Some("work"));
        assert_eq!(
            (reason, tag),
            (LoginRefusalReason::UnknownProvider, "no-accounts")
        );
        let (reason, tag, detail) = refused("claude", Some("../escape"));
        assert_eq!(
            (reason, tag),
            (LoginRefusalReason::SpawnFailed, "bad-account")
        );
        assert!(detail.contains("escape"), "{detail}");
    }

    /// The size table: zero and overflow fall back, sane values pass.
    #[test]
    fn the_requested_size_is_defaulted_and_clamped() {
        let sized = |cols, rows| {
            super::size_of(&LoginSessionRequest {
                login_id: "login-1".to_owned(),
                provider: "fake".to_owned(),
                cols,
                rows,
                account: None,
            })
        };
        assert_eq!((sized(120, 40).cols, sized(120, 40).rows), (120, 40));
        assert_eq!((sized(0, 0).cols, sized(0, 0).rows), (80, 24));
        assert_eq!(
            (sized(70_000, 70_000).cols, sized(70_000, 70_000).rows),
            (80, 24),
            "a size that fits no u16 falls back rather than truncating"
        );
    }

    /// A real PTY session end to end without any relay: spawn a prompt
    /// script, type at it, read its output, and see the reap deliver the
    /// exit code — the supervision half the e2e drives remotely.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_pty_session_carries_prompts_input_and_its_exit_code() {
        let recipe = LoginRecipe::terminal_only(
            "/bin/sh",
            vec![
                "-c".to_owned(),
                "printf 'account? '; read account; \
                 printf 'hello %s in %s\\n' \"$account\" \"$HORDE_LOGIN_TARGET\"; exit 7"
                    .to_owned(),
            ],
        )
        // The recipe's env reaches the command: it is how a login signs in
        // the Worker-owned config directory rather than the operator's.
        .with_env(vec![(
            "HORDE_LOGIN_TARGET".to_owned(),
            "/state/harness/claude/default".to_owned(),
        )]);
        let mut session = spawn_login_pty(
            &recipe,
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .expect("the shell spawns");

        let mut seen = String::new();
        within("the prompt paints", async {
            while !seen.contains("account?") {
                let chunk = session.output.recv().await.expect("output before EOF");
                seen.push_str(&String::from_utf8_lossy(&chunk));
            }
        })
        .await;

        session
            .input
            .send(b"tester\r".to_vec())
            .expect("the writer thread is alive");
        within("the answer echoes through", async {
            while !seen.contains("hello tester in /state/harness/claude/default") {
                let chunk = session.output.recv().await.expect("output before EOF");
                seen.push_str(&String::from_utf8_lossy(&chunk));
            }
        })
        .await;

        // EOF follows the exit; the reap owns the code.
        within("the pty reaches EOF", async {
            while session.output.recv().await.is_some() {}
        })
        .await;
        let code = within("the reap returns", &mut session.reaped)
            .await
            .expect("the wait thread must not panic");
        assert_eq!(code, Some(7), "the exit code is the command's own");
    }

    /// The kill path: a command that would run forever is killed by handle
    /// and provably reaped — the wait returns — with no orphan left behind.
    #[cfg(unix)]
    #[tokio::test]
    async fn killing_a_login_reaps_it_provably() {
        let recipe = LoginRecipe::terminal_only(
            "/bin/sh",
            vec!["-c".to_owned(), "while :; do sleep 1; done".to_owned()],
        );
        let mut session = spawn_login_pty(
            &recipe,
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .expect("the shell spawns");

        // A signal death has whatever code the platform reports; the reap
        // returning at all is the witness that matters.
        let _code = within("kill and reap", super::kill_and_reap(&mut session)).await;
        assert!(
            session.reaped.is_finished(),
            "the wait thread is done: the child was reaped, not abandoned"
        );
    }

    /// A resize reaches the PTY: its size afterwards is the one asked for.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_resize_reaches_the_pty() {
        let recipe = LoginRecipe::terminal_only(
            "/bin/sh",
            vec!["-c".to_owned(), "while :; do sleep 1; done".to_owned()],
        );
        let mut session = spawn_login_pty(
            &recipe,
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .expect("the shell spawns");
        let wanted = PtySize {
            rows: 40,
            cols: 132,
            pixel_width: 0,
            pixel_height: 0,
        };
        super::resize_pty(&session.master, wanted);
        let size = session.master.get_size().expect("the pty answers its size");
        assert_eq!((size.rows, size.cols), (40, 132));
        let _code = within("kill and reap", super::kill_and_reap(&mut session)).await;
    }

    /// A ConPTY session reaches EOF once its command exits: the reaping wait
    /// closes the pseudoconsole, without which a ConPTY's output never ends
    /// (ADR-0017) — and the code is the command's own.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_conpty_session_reaches_eof_when_its_command_exits() {
        let recipe = LoginRecipe::terminal_only(
            "cmd.exe",
            vec![
                "/C".to_owned(),
                "echo hello %HORDE_LOGIN_TARGET% & exit 7".to_owned(),
            ],
        )
        .with_env(vec![(
            "HORDE_LOGIN_TARGET".to_owned(),
            "default".to_owned(),
        )]);
        let mut session = spawn_login_pty(
            &recipe,
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .expect("cmd spawns in a ConPTY");

        let mut seen = String::new();
        within("the output reaches EOF", async {
            while let Some(chunk) = session.output.recv().await {
                seen.push_str(&String::from_utf8_lossy(&chunk));
            }
        })
        .await;
        assert!(seen.contains("hello default"), "{seen:?}");
        let code = within("the reap returns", &mut session.reaped)
            .await
            .expect("the wait thread must not panic");
        assert_eq!(code, Some(7), "the exit code is the command's own");
    }

    /// The ConPTY kill path: a command that would run forever is killed by
    /// handle and reaped, and the closed pseudoconsole ends its output.
    #[cfg(windows)]
    #[tokio::test]
    async fn killing_a_conpty_login_reaps_it_and_ends_its_output() {
        let recipe = LoginRecipe::terminal_only(
            "cmd.exe",
            vec!["/C".to_owned(), "ping -n 600 127.0.0.1 >NUL".to_owned()],
        );
        let mut session = spawn_login_pty(
            &recipe,
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .expect("cmd spawns in a ConPTY");
        let _code = within("kill and reap", super::kill_and_reap(&mut session)).await;
        assert!(session.reaped.is_finished());
        within("the output ends", async {
            while session.output.recv().await.is_some() {}
        })
        .await;
    }

    /// A scripted tunnel: the test plays the relay side, feeding
    /// `from_relay` and reading what the worker sent.
    fn scripted_tunnel() -> (
        super::LoginTunnel,
        tokio::sync::mpsc::Sender<Result<LoginFrame, tonic::Status>>,
        tokio::sync::mpsc::Receiver<LoginFrame>,
    ) {
        let (to_relay, sent) = tokio::sync::mpsc::channel(16);
        let (feed, from_relay) = tokio::sync::mpsc::channel(16);
        (
            super::LoginTunnel {
                to_relay,
                from_relay: Box::pin(tokio_stream::wrappers::ReceiverStream::new(from_relay)),
            },
            feed,
            sent,
        )
    }

    fn request(provider: &str) -> LoginSessionRequest {
        LoginSessionRequest {
            login_id: "login-9".to_owned(),
            provider: provider.to_owned(),
            cols: 80,
            rows: 24,
            account: None,
        }
    }

    /// A finishing tunnel closes its send half first and keeps draining the
    /// receive half until the relay closes it — a finish hollowed down to a
    /// drop lets both halves die at once, which is exactly the race that
    /// loses the queued refusal this method exists to deliver.
    #[tokio::test]
    async fn a_finishing_tunnel_drains_until_the_relay_ends() {
        let (tunnel, feed, mut sent) = scripted_tunnel();
        let finishing = tokio::spawn(tunnel.finish());

        within("the send half closes first", async {
            assert!(
                sent.recv().await.is_none(),
                "finish transmits by closing, not by sending"
            );
        })
        .await;
        feed.send(Ok(horde_proto::login::login_output_frame(
            b"trailing".to_vec(),
        )))
        .await
        .expect("the drain must still hold the receiver open");

        drop(feed);
        within("the finish returns once the relay closes", finishing)
            .await
            .expect("no panic");
    }

    /// A dispatched provider with no recipe answers attach + refusal — the
    /// production `answer_dispatch`, driven over the scripted tunnel.
    #[tokio::test]
    async fn an_unknown_provider_is_refused_after_the_attach() {
        let (announce, lines) = announcing();
        let plane = LoginPlane::new(BTreeMap::new(), announce);
        let (tunnel, feed, mut sent) = scripted_tunnel();
        drop(feed); // The relay half is already gone; the answer must not hang.

        super::answer_dispatch(tunnel, request("nope"), &plane).await;

        let attach = within("the attach", sent.recv()).await.expect("a frame");
        assert!(matches!(attach.frame, Some(Frame::Attach(attach)) if attach.callback_port == 0));
        let refused = within("the refusal", sent.recv()).await.expect("a frame");
        assert_eq!(
            horde_proto::login::login_refusal_in(&refused)
                .expect("the refusal follows the attach")
                .reason,
            horde_proto::v1::LoginRefusalReason::UnknownProvider as i32
        );
        let line = lines.recv().expect("an announce line");
        assert!(line.contains("unknown-provider"), "{line}");
    }

    /// A recipe whose program does not exist answers attach + SPAWN_FAILED,
    /// naming the program.
    #[tokio::test]
    async fn a_spawn_failure_is_refused_with_its_own_code() {
        let (announce, lines) = announcing();
        let mut recipes = BTreeMap::new();
        recipes.insert(
            "fake".to_owned(),
            LoginRecipe::terminal_only("/nonexistent/definitely-not-a-login", vec![]),
        );
        let plane = LoginPlane::new(recipes, announce);
        let (tunnel, feed, mut sent) = scripted_tunnel();
        drop(feed);

        super::answer_dispatch(tunnel, request("fake"), &plane).await;

        let attach = within("the attach", sent.recv()).await.expect("a frame");
        assert!(matches!(attach.frame, Some(Frame::Attach(_))));
        let refused = within("the refusal", sent.recv()).await.expect("a frame");
        let refusal =
            horde_proto::login::login_refusal_in(&refused).expect("a spawn failure refuses");
        assert_eq!(
            refusal.reason,
            horde_proto::v1::LoginRefusalReason::SpawnFailed as i32
        );
        assert!(
            refusal.detail.contains("definitely-not-a-login"),
            "{}",
            refusal.detail
        );
        let line = lines.recv().expect("an announce line");
        assert!(line.contains("spawn-failed"), "{line}");
    }

    /// One recipe dispatched through the production `answer_dispatch` over
    /// a scripted tunnel, with the attach already read — the arrangement
    /// the full-relay and fault tests share.
    async fn dispatched(
        recipe: LoginRecipe,
    ) -> (
        tokio::sync::mpsc::Sender<Result<LoginFrame, tonic::Status>>,
        tokio::sync::mpsc::Receiver<LoginFrame>,
        tokio::task::JoinHandle<()>,
        std::sync::mpsc::Receiver<String>,
        LoginFrame,
    ) {
        let (announce, lines) = announcing();
        let mut recipes = BTreeMap::new();
        recipes.insert("fake".to_owned(), recipe);
        let plane = LoginPlane::new(recipes, announce);
        let (tunnel, feed, mut sent) = scripted_tunnel();
        let answering = tokio::spawn(async move {
            super::answer_dispatch(tunnel, request("fake"), &plane).await;
        });
        let attach = within("the attach", sent.recv()).await.expect("a frame");
        (feed, sent, answering, lines, attach)
    }

    /// The full dispatch over a scripted tunnel: attach carries the recipe's
    /// callback port, the PTY's prompt crosses as output, typed input
    /// reaches the command, the exit frame carries the code, and the exited
    /// announce line lands — the whole worker half without a server.
    #[tokio::test]
    async fn a_dispatched_login_relays_prompts_input_and_exit() {
        let recipe = LoginRecipe {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "printf 'account? '; read account; printf 'signed in as %s\\n' \"$account\"; exit 0"
                    .to_owned(),
            ],
            callback_port: Some(14555),
            listen_port: None,
            env: Vec::new(),
            accounts: None,
        };
        let (feed, mut sent, answering, lines, attach) = dispatched(recipe).await;
        assert!(
            matches!(attach.frame, Some(Frame::Attach(attach)) if attach.callback_port == 14555),
            "the attach carries the recipe's callback port"
        );

        let mut seen = String::new();
        within("the prompt crosses as output", async {
            while !seen.contains("account?") {
                match sent.recv().await.expect("a frame").frame {
                    Some(Frame::Output(bytes)) => seen.push_str(&String::from_utf8_lossy(&bytes)),
                    other => panic!("expected output, got {other:?}"),
                }
            }
        })
        .await;

        feed.send(Ok(horde_proto::login::login_input_frame(
            b"tester\r".to_vec(),
        )))
        .await
        .expect("the input frame sends");

        let code = within("the exit frame arrives", async {
            loop {
                match sent.recv().await.expect("a frame").frame {
                    Some(Frame::Output(bytes)) => seen.push_str(&String::from_utf8_lossy(&bytes)),
                    Some(Frame::Exited(exit)) => return exit.code,
                    other => panic!("expected output or exit, got {other:?}"),
                }
            }
        })
        .await;
        assert_eq!(code, 0);
        assert!(seen.contains("signed in as tester"), "{seen}");

        // The relay closes its half; the worker's graceful finish ends fast
        // rather than spending its whole drain ceiling.
        drop(feed);
        within("the answer ends", answering)
            .await
            .expect("no panic");
        let spawned = lines.recv().expect("the spawned line");
        assert!(spawned.contains("login spawned"), "{spawned}");
        let exited = lines.recv().expect("the exited line");
        assert!(
            exited.contains("login exited") && exited.contains("code=0"),
            "{exited}"
        );
    }

    /// The fault the spec names: the client vanishes mid-login. The relay
    /// leg ending is the order to kill; the announce line lands only after
    /// the wait returned — the owned reap witness, no OS state asserted.
    #[tokio::test]
    async fn a_vanished_client_kills_and_provably_reaps_the_login() {
        let recipe = LoginRecipe::terminal_only(
            "/bin/sh",
            vec!["-c".to_owned(), "while :; do sleep 1; done".to_owned()],
        );
        let (feed, _sent, answering, lines, attach) = dispatched(recipe).await;
        assert!(matches!(attach.frame, Some(Frame::Attach(_))));

        // The client vanishes: the relay leg closes.
        drop(feed);

        within("the answer ends through the reap", answering)
            .await
            .expect("no panic");
        let mut lines_seen = Vec::new();
        while let Ok(line) = lines.try_recv() {
            lines_seen.push(line);
        }
        assert!(
            lines_seen
                .iter()
                .any(|line| line.contains("login reaped") && line.contains("client-vanished")),
            "the reap witness line must land after the wait returns: {lines_seen:?}"
        );
    }
}
