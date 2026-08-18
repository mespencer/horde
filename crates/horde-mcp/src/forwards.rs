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

/// Whether a spec is complete enough to raise: a forwardable worker port
/// *and* a name — forwards are named, not numbered. The compound is named
/// because its mixed rows are what an `||` typo would get wrong, and the
/// handler wrapping it is reachable only through a whole MCP request.
#[must_use]
pub fn declarable(spec: &ForwardSpec) -> bool {
    let port = u16::try_from(spec.worker_port).unwrap_or(0);
    forwardable_port(port) && !spec.name.is_empty()
}

/// [`declarable`] as the gate the handlers apply: `Err` carries the words
/// the agent reads. A gate rather than an `if !…` in the handler, because
/// the handler is reachable only through a whole MCP request and an
/// inverted refusal there would pass everything else this crate tests.
///
/// # Errors
///
/// The refusal, in the agent's vocabulary.
pub fn declaration_gate(spec: &ForwardSpec) -> Result<(), &'static str> {
    if declarable(spec) {
        Ok(())
    } else {
        Err("a forward needs a nonzero port and a name: forwards are named, not numbered")
    }
}

/// The worker-loopback port an inject binds: the explicit one, defaulted to
/// the target port — refused as unforwardable when zero, because the intent
/// is journaled and the log cannot hold an ephemeral port.
///
/// # Errors
///
/// The refusal, in the agent's vocabulary.
pub fn inject_worker_port(port: u16, worker_port: Option<u16>) -> Result<u16, &'static str> {
    let chosen = worker_port.unwrap_or(port);
    if forwardable_port(chosen) {
        Ok(chosen)
    } else {
        Err("worker_port must be nonzero: the intent is journaled, and the log cannot hold an \
             ephemeral port")
    }
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

    /// All four corners, plus the port that fits no u16: a spec is raisable
    /// only with a real port and a real name.
    #[test]
    fn only_a_named_spec_with_a_real_port_is_declarable() {
        let spec = |port: u32, name: &str| ForwardSpec {
            direction: ForwardDirection::Expose as i32,
            name: name.to_owned(),
            worker_port: port,
            client_port: 0,
        };
        assert!(declarable(&spec(5173, "vite")));
        assert!(!declarable(&spec(0, "vite")));
        assert!(!declarable(&spec(5173, "")));
        assert!(!declarable(&spec(0, "")));
        assert!(
            !declarable(&spec(70_000, "vite")),
            "a port that fits no u16 is not a port"
        );

        // The gate the handlers apply: same verdicts, plus the words.
        assert!(declaration_gate(&spec(5173, "vite")).is_ok());
        let refused = declaration_gate(&spec(0, "vite")).expect_err("must refuse");
        assert!(refused.contains("named, not numbered"), "{refused}");
    }

    /// The inject port choice, both directions: explicit outranks default,
    /// the default is the target port, and zero refuses either way.
    #[test]
    fn the_inject_worker_port_defaults_and_refuses_zero() {
        assert_eq!(inject_worker_port(5432, None), Ok(5432));
        assert_eq!(inject_worker_port(5432, Some(15432)), Ok(15432));
        assert!(inject_worker_port(0, None).is_err());
        let refused = inject_worker_port(5432, Some(0)).expect_err("zero is no port");
        assert!(refused.contains("nonzero"), "{refused}");
    }
}
