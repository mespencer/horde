//! Ergonomics for the interactive-auth relay frames (DESIGN §4).
//!
//! Every peer of a login — the relay, the worker leg, the client leg —
//! speaks the same frame vocabulary, and each would otherwise grow its own
//! constructors for it. Like [`crate::forward`], this is transport
//! ergonomics only: no policy, no I/O.

use crate::v1::{
    ForwardId, LoginAccepted, LoginAttach, LoginExit, LoginFrame, LoginOpen, LoginRefusal,
    LoginRefusalReason, LoginResize, WorkerId, login_frame::Frame,
};

/// The first frame a client sends: which worker, which provider, how big
/// the terminal is.
#[must_use]
pub fn login_open_frame(worker_id: &str, provider: &str, cols: u32, rows: u32) -> LoginFrame {
    LoginFrame {
        frame: Some(Frame::Open(LoginOpen {
            worker_id: Some(WorkerId::new(worker_id)),
            provider: provider.to_owned(),
            cols,
            rows,
        })),
    }
}

/// The first frame a worker's tunnel sends: which dispatched login it
/// answers, the browser-facing callback port (zero for none), and the
/// worker-side listener when it differs (zero for "the same").
#[must_use]
pub fn login_attach_frame(login_id: &str, callback_port: u32, listen_port: u32) -> LoginFrame {
    LoginFrame {
        frame: Some(Frame::Attach(LoginAttach {
            login_id: login_id.to_owned(),
            callback_port,
            listen_port,
        })),
    }
}

/// The server's answer once both legs are paired. An empty
/// `callback_forward_id` means no callback expose rides this login.
#[must_use]
pub fn login_accepted_frame(
    login_id: &str,
    callback_forward_id: Option<&str>,
    callback_port: u32,
) -> LoginFrame {
    LoginFrame {
        frame: Some(Frame::Accepted(LoginAccepted {
            login_id: login_id.to_owned(),
            callback_forward_id: callback_forward_id.map(ForwardId::new),
            callback_port,
        })),
    }
}

/// A terminal refusal, with the reason a peer can branch on and the detail
/// a human can read.
#[must_use]
pub fn login_refusal_frame(reason: LoginRefusalReason, detail: impl Into<String>) -> LoginFrame {
    LoginFrame {
        frame: Some(Frame::Refused(LoginRefusal {
            reason: reason as i32,
            detail: detail.into(),
        })),
    }
}

/// Client keystrokes toward the PTY.
#[must_use]
pub fn login_input_frame(bytes: Vec<u8>) -> LoginFrame {
    LoginFrame {
        frame: Some(Frame::Input(bytes)),
    }
}

/// PTY output toward the client terminal.
#[must_use]
pub fn login_output_frame(bytes: Vec<u8>) -> LoginFrame {
    LoginFrame {
        frame: Some(Frame::Output(bytes)),
    }
}

/// The client terminal changed size.
#[must_use]
pub fn login_resize_frame(cols: u32, rows: u32) -> LoginFrame {
    LoginFrame {
        frame: Some(Frame::Resize(LoginResize { cols, rows })),
    }
}

/// The login command ended with this code.
#[must_use]
pub fn login_exit_frame(code: u32) -> LoginFrame {
    LoginFrame {
        frame: Some(Frame::Exited(LoginExit { code })),
    }
}

/// The refusal a frame carries, if it carries one.
#[must_use]
pub fn login_refusal_in(frame: &LoginFrame) -> Option<&LoginRefusal> {
    match frame.frame.as_ref()? {
        Frame::Refused(refusal) => Some(refusal),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trips every constructor through the oneof, so a renumbered
    /// field or a constructor filling the wrong arm cannot survive.
    #[test]
    fn every_login_frame_carries_its_own_kind() {
        assert!(matches!(
            login_open_frame("w1", "claude", 80, 24).frame,
            Some(Frame::Open(LoginOpen { provider, cols, rows, .. }))
                if provider == "claude" && cols == 80 && rows == 24
        ));
        assert!(matches!(
            login_attach_frame("login-1", 1455, 0).frame,
            Some(Frame::Attach(LoginAttach { login_id, callback_port, listen_port }))
                if login_id == "login-1" && callback_port == 1455 && listen_port == 0
        ));
        assert!(matches!(
            login_attach_frame("login-1", 1455, 14556).frame,
            Some(Frame::Attach(LoginAttach {
                listen_port: 14556,
                ..
            }))
        ));
        assert!(matches!(
            login_input_frame(vec![3]).frame,
            Some(Frame::Input(bytes)) if bytes == vec![3]
        ));
        assert!(matches!(
            login_output_frame(vec![7, 8]).frame,
            Some(Frame::Output(bytes)) if bytes == vec![7, 8]
        ));
        assert!(matches!(
            login_resize_frame(120, 40).frame,
            Some(Frame::Resize(LoginResize {
                cols: 120,
                rows: 40
            }))
        ));
        assert!(matches!(
            login_exit_frame(0).frame,
            Some(Frame::Exited(LoginExit { code: 0 }))
        ));

        let refused = login_refusal_frame(LoginRefusalReason::AlreadyInProgress, "busy");
        let refusal = login_refusal_in(&refused).expect("a refusal frame carries its refusal");
        assert_eq!(refusal.reason, LoginRefusalReason::AlreadyInProgress as i32);
        assert_eq!(refusal.detail, "busy");
        assert!(login_refusal_in(&login_exit_frame(0)).is_none());
        assert!(login_refusal_in(&LoginFrame::default()).is_none());
    }

    /// The accepted frame's callback halves travel together: a login with a
    /// callback names both the ephemeral forward and its exact port, and a
    /// login without one names neither.
    #[test]
    fn an_accepted_frame_carries_its_callback_or_none() {
        let with = login_accepted_frame("login-1", Some("fwd-login-1"), 1455);
        match with.frame {
            Some(Frame::Accepted(accepted)) => {
                assert_eq!(accepted.login_id, "login-1");
                assert_eq!(
                    accepted
                        .callback_forward_id
                        .as_ref()
                        .map(ToString::to_string),
                    Some("fwd-login-1".to_owned())
                );
                assert_eq!(accepted.callback_port, 1455);
            }
            other => panic!("expected Accepted, got {other:?}"),
        }

        let without = login_accepted_frame("login-2", None, 0);
        match without.frame {
            Some(Frame::Accepted(accepted)) => {
                assert!(accepted.callback_forward_id.is_none());
                assert_eq!(accepted.callback_port, 0);
            }
            other => panic!("expected Accepted, got {other:?}"),
        }
    }
}
