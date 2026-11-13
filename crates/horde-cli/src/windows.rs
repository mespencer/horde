//! The CLI's Windows bodies (ADR-0017), each behind the one function its
//! callers use on every platform.
//!
//! Every item here is `cfg(windows)`: the Linux gauntlet compiles none of
//! it, so its mutants are listed but can never be built — which is why
//! `.cargo/mutants.toml` excludes this file, and why its tests are
//! `cfg(windows)` and run on a Windows machine. The module itself is always
//! declared, so the module-structure gate sees a file it can account for.

/// [`crate::doctor`]'s harness lookup on Windows: `name.exe` — the only
/// form the Worker can launch, since `CreateProcess` resolves a bare name to
/// `.exe` alone and a `.cmd` shim (npm's) is not a program.
#[cfg(windows)]
pub(crate) fn on_path(name: &str, path: Option<&std::ffi::OsStr>) -> Option<std::path::PathBuf> {
    std::env::split_paths(path?)
        .map(|dir| dir.join(crate::doctor::windows_program(name)))
        .find(|candidate| std::fs::metadata(candidate).is_ok_and(|meta| meta.is_file()))
}

/// A state directory's privacy on Windows: there are no mode bits, only the
/// ACL it inherits — the user's profile grants the user, SYSTEM and
/// Administrators alone. Reported as such, with the command that shows it.
#[cfg(windows)]
pub(crate) fn permission_check(
    label: &str,
    dir: &std::path::Path,
    _meta: &std::fs::Metadata,
) -> crate::doctor::Check {
    crate::doctor::Check::ok(
        label,
        format!(
            "inherits its ACL (no mode bits on this platform); `icacls {}` shows who may look \
             inside",
            dir.display()
        ),
    )
}

/// Free space where there is no `df`: `GetDiskFreeSpaceExW` (through
/// `fs4`), the bytes this user may still write, in KiB.
#[cfg(windows)]
pub(crate) fn disk_check(dir: &std::path::Path) -> crate::doctor::Check {
    let label = format!("disk {}", dir.display());
    let available = fs4::available_space(dir).ok().map(|bytes| bytes / 1024);
    crate::doctor::disk_verdict(&label, available)
}

/// `horde worker login`'s resizes on Windows: there is no SIGWINCH, and a
/// console resize arrives only as an input record the raw stdin reader would
/// have to decode, so the size is polled twice a second and each change sent.
#[cfg(windows)]
pub(crate) async fn watch_resizes(resize_tx: tokio::sync::mpsc::Sender<(u16, u16)>) {
    let mut last = crate::login::terminal_size();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
    loop {
        tick.tick().await;
        let now = crate::login::terminal_size();
        if !crate::login::send_if_changed(&resize_tx, &mut last, now).await {
            return;
        }
    }
}

/// `horde up`'s stop signals on Windows, registered now: ctrl-c, ctrl-break,
/// or the console closing. The daemons share this console, so the same
/// ctrl-c reaches them too, and the teardown's bounded wait lets them act on
/// it.
#[cfg(windows)]
pub(crate) fn stop_signals() -> Result<impl std::future::Future<Output = ()>, crate::ClientError> {
    use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close};
    let (mut interrupt, mut broke, mut closed) = ctrl_c()
        .and_then(|interrupt| Ok((interrupt, ctrl_break()?, ctrl_close()?)))
        .map_err(|error| crate::ClientError::Up(format!("cannot listen for signals: {error}")))?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = broke.recv() => {}
            _ = closed.recv() => {}
        }
    })
}
