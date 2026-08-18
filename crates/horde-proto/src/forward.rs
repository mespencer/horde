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
pub fn refusal_frame(reason: ForwardRefusalReason, detail: impl Into<String>) -> ForwardTunnelFrame {
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
}
