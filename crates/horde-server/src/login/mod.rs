//! The interactive-auth relay (DESIGN §4): server half.
//!
//! A login attaches a Client terminal to a provider's login command running
//! in a PTY on a Worker, over the same two authenticated hops every forward
//! rides — Client ↔ Server ↔ Worker — plus an **ephemeral expose** of the
//! provider's OAuth-callback port so the Client's browser can land on the
//! Worker's loopback listener.
//!
//! Unlike a forward, a login is **EPHEMERAL intent** (decided here, per
//! DESIGN §4's open door): it is journaled as `WorkerLoginStarted` /
//! `WorkerLoginEnded` for the audit trail, but liveness is the in-memory
//! pairing in [`registry::LoginRegistry`] and nothing else. A PTY cannot be
//! replayed into existence and half a login is worth nothing, so no fold
//! revives one — the operator simply runs the verb again. The journaled
//! events double as the log notifications that make the worker forward
//! streams recompute their intent sets, which is how the ephemeral callback
//! expose reaches the Worker's dialable map without ever being log state.

pub mod registry;
pub mod relay;
#[cfg(test)]
pub(crate) mod testfix;

/// The reserved stream login audit events live in, like enrollment's.
///
/// Server-assigned session ids are `session-…`, so nothing user-created can
/// collide with it.
pub const LOGIN_STREAM: &str = "horde-logins";

/// Whether a log stream is the login audit stream rather than a session.
#[must_use]
pub fn is_login_stream(session_id: &str) -> bool {
    session_id == LOGIN_STREAM
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_login_stream_is_reserved_and_recognizable() {
        assert!(super::is_login_stream("horde-logins"));
        assert!(!super::is_login_stream("session-1"));
        assert!(
            crate::enrollment::is_reserved_stream(super::LOGIN_STREAM),
            "a reserved stream leaking into session listings becomes a phantom session"
        );
    }
}
