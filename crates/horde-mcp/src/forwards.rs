//! The forward verbs' pure half (ADR-0006).
//!
//! What an `expose 5173 as "vite"` or an agent-initiated inject *means* on
//! the wire — the spec that rides the permission raise, and the proposal the
//! chain's rules match on — lifted out of the handlers so the truth tables
//! are assertable without staging an MCP request.

use horde_proto::v1::{ForwardDirection, ForwardSpec};

/// The spec an `expose <port> as <name>` declares: the worker dials `port`
/// per tunneled connection, and each realizing client picks its own local
/// port (the same number when it can bind it, ephemeral with a printed
/// mapping when it cannot).
#[must_use]
pub fn expose_spec(port: u16, name: &str) -> ForwardSpec {
    ForwardSpec {
        direction: ForwardDirection::Expose as i32,
        name: name.to_owned(),
        worker_port: u32::from(port),
        client_port: 0,
    }
}

/// The spec an agent-initiated inject declares: the backer dials
/// `client_port` on its own machine per tunneled connection, and the worker
/// binds `worker_port` on loopback for the agent's processes to reach.
#[must_use]
pub fn inject_spec(client_port: u16, worker_port: u16, name: &str) -> ForwardSpec {
    ForwardSpec {
        direction: ForwardDirection::Inject as i32,
        name: name.to_owned(),
        worker_port: u32::from(worker_port),
        client_port: u32::from(client_port),
    }
}

/// The proposal the permission chain judges a forward by.
///
/// Ports ride as *strings* deliberately: the deterministic rule language
/// constrains named arguments by string prefix, so a numeric port could
/// never satisfy a rule and a mode could only allow all forwards or none —
/// which would defeat ADR-0006's "auto-allows within a named port/name
/// policy".
#[must_use]
pub fn forward_proposal(spec: &ForwardSpec) -> serde_json::Value {
    serde_json::json!({
        "port": spec.worker_port.to_string(),
        "client_port": spec.client_port.to_string(),
        "name": spec.name,
    })
}

/// Whether a port an agent named can be forwarded at all. Zero cannot: the
/// intent is journaled, and the log cannot hold an ephemeral port.
#[must_use]
pub fn forwardable_port(port: u16) -> bool {
    port != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_expose_leaves_the_client_port_to_the_realizing_client() {
        let spec = expose_spec(5173, "vite");
        assert_eq!(spec.direction, ForwardDirection::Expose as i32);
        assert_eq!(spec.name, "vite");
        assert_eq!(spec.worker_port, 5173);
        assert_eq!(
            spec.client_port, 0,
            "each client picks its own local port; the intent does not"
        );
    }

    #[test]
    fn an_inject_names_both_ends_ports() {
        let spec = inject_spec(5432, 15432, "db");
        assert_eq!(spec.direction, ForwardDirection::Inject as i32);
        assert_eq!(spec.client_port, 5432, "the backer dials this");
        assert_eq!(spec.worker_port, 15432, "the worker binds this");
    }

    /// The rule language matches string prefixes on named arguments, so the
    /// proposal must carry ports as strings or no mode could ever constrain
    /// them.
    #[test]
    fn the_proposal_carries_ports_as_strings_for_the_rule_language() {
        let proposal = forward_proposal(&expose_spec(5173, "vite"));
        assert_eq!(
            proposal,
            serde_json::json!({"port": "5173", "client_port": "0", "name": "vite"})
        );
    }

    #[test]
    fn only_a_nonzero_port_is_forwardable() {
        assert!(forwardable_port(5173));
        assert!(!forwardable_port(0));
    }
}
