//! Loopback ports a test asks a product to bind *exactly* — a login's OAuth
//! callback, whose redirect URI admits no other port.
//!
//! The usual way to find one, binding `:0` and releasing it, races: the
//! released port is back in the kernel's pool at once, and the next `:0`
//! bind or outgoing connection anywhere on the machine can take it before
//! the product binds it. A loaded test run makes plenty of both.
//!
//! [`hold_port`] keeps the port instead: a socket bound to it with
//! `SO_REUSEADDR`, never listening, for as long as the [`HeldPort`] lives.
//! The kernel's own port picks (a `:0` bind, a connection's local port) pass
//! over a port something is bound to, while a listener that also asks for
//! `SO_REUSEADDR` — std's and tokio's both do on Unix — binds and listens
//! on it beside a socket that does not listen.

/// A loopback port held for a product to bind exactly; released on drop.
#[derive(Debug)]
pub struct HeldPort {
    /// Bound, never listening: what keeps the kernel from handing the port out.
    _socket: tokio::net::TcpSocket,
    port: u16,
}

impl HeldPort {
    /// The held port.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Holds a free loopback port until the returned [`HeldPort`] is dropped.
///
/// # Errors
///
/// When no socket can be made or bound.
pub fn hold_port() -> std::io::Result<HeldPort> {
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let port = socket.local_addr()?.port();
    Ok(HeldPort {
        _socket: socket,
        port,
    })
}

#[cfg(test)]
mod tests {
    use super::hold_port;

    /// A product's listener — std's, which asks for `SO_REUSEADDR` — binds
    /// the held port exactly while it is held.
    #[test]
    fn a_reusing_listener_binds_the_held_port_exactly() {
        let held = hold_port().expect("a port is held");
        assert_ne!(held.port(), 0, "a real port, not the ask for any");
        assert_eq!(
            held._socket.local_addr().expect("bound"),
            std::net::SocketAddr::from(([127, 0, 0, 1], held.port())),
            "held on the loopback address the product binds"
        );
        let listener =
            std::net::TcpListener::bind(("127.0.0.1", held.port())).expect("the product binds it");
        assert_eq!(
            listener.local_addr().expect("an address").port(),
            held.port()
        );
    }

    /// While held, the port is taken to anything that does not share it:
    /// a plain bind is refused, so nothing else lands on it by accident.
    #[test]
    fn a_held_port_is_refused_to_a_binder_that_does_not_share() {
        let held = hold_port().expect("a port is held");
        let stranger = tokio::net::TcpSocket::new_v4().expect("a socket");
        let refused = stranger
            .bind(std::net::SocketAddr::from(([127, 0, 0, 1], held.port())))
            .expect_err("the port is held");
        assert_eq!(refused.kind(), std::io::ErrorKind::AddrInUse);
    }
}
