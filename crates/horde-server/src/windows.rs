//! The server's Windows bodies (ADR-0017), each behind the one function its
//! callers use on every platform.
//!
//! Every item here is `cfg(windows)`: the Linux gauntlet compiles none of
//! it, so its mutants are listed but can never be built — which is why
//! `.cargo/mutants.toml` excludes this file, and why its tests are
//! `cfg(windows)` and run on a Windows machine. The module itself is always
//! declared, so the module-structure gate sees a file it can account for.

/// [`crate::startup::forwarded_hangups`] on Windows: there is no SIGHUP, so
/// the channel is born closed, which `hold_until` reads as "no re-mints"
/// while the server stays up. A fresh bootstrap token comes from a restart.
#[cfg(windows)]
#[expect(
    clippy::unnecessary_wraps,
    reason = "the signature is the Unix one, whose handler can fail to install"
)]
pub(crate) fn forwarded_hangups() -> std::io::Result<tokio::sync::mpsc::Receiver<()>> {
    let (_closed, receiver) = tokio::sync::mpsc::channel(1);
    Ok(receiver)
}

/// [`crate::remint::send_hangup`] on Windows: no SIGHUP to send, so the
/// re-mint is refused in words that name the restart that works.
#[cfg(windows)]
pub(crate) fn send_hangup(pid: u32) -> std::io::Result<()> {
    Err(crate::remint::unsupported_remint(pid))
}
