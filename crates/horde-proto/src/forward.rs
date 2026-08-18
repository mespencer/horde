//! Ergonomics for the forward tunnel frames (ADR-0006).
//!
//! Every peer of a tunnel — the relay, a worker leg, a client leg — speaks
//! the same five-frame vocabulary, and each would otherwise grow its own
//! constructors for it. Like [`crate::ids`], this is transport ergonomics
//! only: no policy, no I/O.

use crate::v1::{
    ForwardId, ForwardRefusalReason, ForwardTunnelAccepted, ForwardTunnelEof, ForwardTunnelFrame,
    ForwardTunnelOpen, ForwardTunnelRefusal, forward_tunnel_frame::Frame,
};

/// The first frame the opening peer sends.
#[must_use]
pub fn open_frame(forward_id: &str, connection_id: &str) -> ForwardTunnelFrame {
    ForwardTunnelFrame {
        frame: Some(Frame::Open(ForwardTunnelOpen {
            forward_id: Some(ForwardId::new(forward_id)),
            connection_id: connection_id.to_owned(),
        })),
    }
}

/// A run of bytes.
#[must_use]
pub fn data_frame(bytes: Vec<u8>) -> ForwardTunnelFrame {
    ForwardTunnelFrame {
        frame: Some(Frame::Data(bytes)),
    }
}

/// Half-close: the sender will write no more bytes.
#[must_use]
pub fn eof_frame() -> ForwardTunnelFrame {
    ForwardTunnelFrame {
        frame: Some(Frame::Eof(ForwardTunnelEof {})),
    }
}

/// Both legs are up; bytes may flow.
#[must_use]
pub fn accepted_frame() -> ForwardTunnelFrame {
    ForwardTunnelFrame {
        frame: Some(Frame::Accepted(ForwardTunnelAccepted {})),
    }
}

/// A terminal refusal, with the reason a peer can branch on and the detail a
/// human can read.
#[must_use]
pub fn refusal_frame(
    reason: ForwardRefusalReason,
    detail: impl Into<String>,
) -> ForwardTunnelFrame {
    ForwardTunnelFrame {
        frame: Some(Frame::Refused(ForwardTunnelRefusal {
            reason: reason as i32,
            detail: detail.into(),
        })),
    }
}

/// The refusal a frame carries, if it carries one.
#[must_use]
pub fn refusal_in(frame: &ForwardTunnelFrame) -> Option<&ForwardTunnelRefusal> {
    match frame.frame.as_ref()? {
        Frame::Refused(refusal) => Some(refusal),
        _ => None,
    }
}

/// The wire direction as the word every surface prints — `forward ls`, the
/// TUI panel, the feed. One owner so the vocabulary cannot drift between
/// frontends (the same reasoning as the result-status words in horde-wire).
#[must_use]
pub fn direction_name(direction: i32) -> &'static str {
    match crate::v1::ForwardDirection::try_from(direction) {
        Ok(crate::v1::ForwardDirection::Expose) => "expose",
        Ok(crate::v1::ForwardDirection::Inject) => "inject",
        _ => "unspecified",
    }
}

/// The forward id a chain-approved request opens under (ADR-0006).
///
/// Shared vocabulary rather than a server detail: the server's opener derives
/// it to make its keyed append idempotent across restarts, and the agent
/// endpoint derives the same id to tell the agent what its approved expose is
/// called — two crates, one rule, or the id the agent hears drifts from the
/// one the log holds.
#[must_use]
pub fn derived_forward_id(request_id: &str) -> String {
    format!("fwd-{request_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trips every constructor through the oneof, so a renumbered field
    /// or a constructor filling the wrong arm cannot survive.
    #[test]
    fn every_frame_carries_its_own_kind() {
        assert!(matches!(
            open_frame("fwd-1", "c1").frame,
            Some(Frame::Open(ForwardTunnelOpen { connection_id, .. })) if connection_id == "c1"
        ));
        assert!(matches!(
            data_frame(vec![7, 8]).frame,
            Some(Frame::Data(bytes)) if bytes == vec![7, 8]
        ));
        assert!(matches!(eof_frame().frame, Some(Frame::Eof(_))));
        assert!(matches!(accepted_frame().frame, Some(Frame::Accepted(_))));

        let refused = refusal_frame(ForwardRefusalReason::Unbacked, "backer detached");
        let refusal = refusal_in(&refused).expect("a refusal frame carries its refusal");
        assert_eq!(refusal.reason, ForwardRefusalReason::Unbacked as i32);
        assert_eq!(refusal.detail, "backer detached");

        assert!(refusal_in(&eof_frame()).is_none());
        assert!(refusal_in(&ForwardTunnelFrame::default()).is_none());
    }

    /// The words every surface prints, pinned literally — arm by arm,
    /// because a dropped arm falls to "unspecified" and every panel and
    /// render would print it without failing anything else in this crate.
    #[test]
    fn every_direction_prints_its_own_word() {
        assert_eq!(
            direction_name(crate::v1::ForwardDirection::Expose as i32),
            "expose"
        );
        assert_eq!(
            direction_name(crate::v1::ForwardDirection::Inject as i32),
            "inject"
        );
        assert_eq!(direction_name(0), "unspecified");
        assert_eq!(direction_name(99), "unspecified");
    }

    /// The derivation is shared vocabulary: the exact string, not just
    /// non-emptiness, because the server's keyed append and the agent's
    /// answer must mint the same id.
    #[test]
    fn the_derived_forward_id_is_the_request_id_prefixed() {
        assert_eq!(derived_forward_id("req-9"), "fwd-req-9");
        assert_eq!(derived_forward_id(""), "fwd-");
    }
}
