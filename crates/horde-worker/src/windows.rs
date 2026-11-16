//! The Worker's Windows bodies (ADR-0017), each behind the one function its
//! callers use on every platform.
//!
//! Every item here is `cfg(windows)`: the Linux gauntlet compiles none of
//! it, so its mutants are listed but can never be built — which is why
//! `.cargo/mutants.toml` excludes this file, and why the tests that pin it
//! are `cfg(windows)` and run on a Windows machine. The module itself is
//! always declared, so the module-structure gate sees a file it can account
//! for.

/// [`crate::process_tree::ProcessTree::spawn`] on Windows: `command` spawned
/// into a new Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
/// `process-wrap` does the Win32 work — `CREATE_SUSPENDED`, the job, the
/// assignment, the resume — because the workspace forbids `unsafe`; the
/// child is in the job before its first instruction runs, so nothing it
/// starts can be outside it, and the job's last handle closing kills every
/// member.
///
/// # Errors
///
/// The spawn failing, or the job failing to form.
#[cfg(windows)]
pub(crate) fn spawn_in_job(
    command: tokio::process::Command,
) -> std::io::Result<Box<dyn process_wrap::tokio::ChildWrapper>> {
    use process_wrap::tokio::{CommandWrap, JobObject, KillOnDrop};
    let mut wrapped = CommandWrap::from(command);
    // KillOnDrop is what makes the job kill-on-close. process-wrap 10 is
    // required: 9.x hid the registered wrappers from the job wrapper, which
    // then made a job that outlived its last handle (ADR-0017).
    wrapped.wrap(KillOnDrop).wrap(JobObject);
    wrapped.spawn()
}

/// The workspace pressure check's disk figures on Windows:
/// `GetDiskFreeSpaceExW` through `fs4` — the bytes available to this user,
/// and the volume's total.
#[cfg(windows)]
pub(crate) fn disk_stats(path: &std::path::Path) -> Option<(u64, u64)> {
    let stats = fs4::statvfs(path).ok()?;
    Some((stats.available_space(), stats.total_space()))
}

/// An unmanaged workspace path's directory link on Windows: a directory
/// symlink where the account may make one (Developer Mode, or an elevated
/// Worker), and otherwise an NTFS junction, which needs no privilege and
/// reads the same to every program that opens a path through it. `target`
/// is absolute — an Instance root joined to a path — which a junction
/// requires.
#[cfg(windows)]
pub(crate) async fn link_dir(
    target: &std::path::Path,
    link: &std::path::Path,
) -> std::io::Result<()> {
    if tokio::fs::symlink_dir(target, link).await.is_ok() {
        return Ok(());
    }
    let (target, link) = (target.to_owned(), link.to_owned());
    tokio::task::spawn_blocking(move || junction::create(target, link))
        .await
        .map_err(std::io::Error::other)?
}

/// Resizes a login's ConPTY while it is open; once the command has exited
/// and the reaping wait has closed the pseudoconsole, there is nothing to
/// resize.
#[cfg(windows)]
pub(crate) fn resize_pty(
    master: &std::sync::Mutex<Option<Box<dyn portable_pty::MasterPty + Send>>>,
    size: portable_pty::PtySize,
) {
    if let Some(master) = master
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
    {
        let _ = master.resize(size);
    }
}

/// Resolves when the daemon is asked to stop on Windows: ctrl-c or
/// ctrl-break (how an operator, or a console-sharing parent, stops it), the
/// console closing, or the system shutting down — Windows' SIGTERMs.
/// Failing to listen for the console events degrades to ctrl-c alone, and
/// failing to listen for ctrl-c too parks forever (the operator still holds
/// `taskkill /F`).
#[cfg(windows)]
pub async fn console_stop() {
    use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close, ctrl_shutdown};
    let (Ok(mut interrupt), Ok(mut broke), Ok(mut closed), Ok(mut shut_down)) =
        (ctrl_c(), ctrl_break(), ctrl_close(), ctrl_shutdown())
    else {
        tracing::warn!("could not listen for ctrl-break, close or shutdown; ctrl-c only");
        match ctrl_c() {
            Ok(mut interrupt) => {
                let _ = interrupt.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "could not listen for shutdown");
                std::future::pending::<()>().await;
            }
        }
        return;
    };
    tokio::select! {
        _ = interrupt.recv() => {}
        _ = broke.recv() => {}
        _ = closed.recv() => {}
        _ = shut_down.recv() => {}
    }
}
