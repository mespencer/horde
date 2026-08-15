//! The namespace discipline for proxied tools.
//!
//! One endpoint serves many servers' tools, so every proxied tool's name
//! carries its server: `{server}__{tool}`. The server half is constrained at
//! configuration time to `[a-z0-9-]` — no underscore — which is what makes
//! splitting on the *first* `__` unambiguous even when the tool's own name
//! contains `__`.
//!
//! Pure functions, deliberately: a namespacing bug misroutes tool calls with
//! no error anywhere, so the whole truth table has to be assertable without a
//! wire.

/// The separator between a server's name and its tool's name.
pub const SEPARATOR: &str = "__";

/// The endpoint-facing name for `tool` proxied from `server`.
#[must_use]
pub fn namespaced(server: &str, tool: &str) -> String {
    format!("{server}{SEPARATOR}{tool}")
}

/// Splits a namespaced name back into `(server, tool)`.
///
/// `None` for a name with no separator — an unprefixed name is one of the
/// endpoint's own tools, never a proxied one. The split is on the first
/// separator, and the config-time charset (`[a-z0-9-]`, no `_`) is what makes
/// that the only possible split point inside the server half.
///
/// Both halves must be non-empty: `__tool` names no server and `server__`
/// names no tool, and either would otherwise route somewhere surprising.
#[must_use]
pub fn split_namespaced(name: &str) -> Option<(&str, &str)> {
    let (server, tool) = name.split_once(SEPARATOR)?;
    if server.is_empty() || tool.is_empty() {
        return None;
    }
    Some((server, tool))
}

#[cfg(test)]
mod tests {
    use super::{namespaced, split_namespaced};

    #[test]
    fn a_namespaced_name_round_trips() {
        assert_eq!(namespaced("linear", "create_issue"), "linear__create_issue");
        assert_eq!(
            split_namespaced("linear__create_issue"),
            Some(("linear", "create_issue"))
        );
    }

    /// The case the charset rule exists for: a tool whose own name contains
    /// the separator must come back intact, not truncated at its second `__`.
    #[test]
    fn a_tool_name_containing_the_separator_survives() {
        let name = namespaced("linear", "issues__create");
        assert_eq!(name, "linear__issues__create");
        assert_eq!(split_namespaced(&name), Some(("linear", "issues__create")));
    }

    #[test]
    fn unprefixed_names_are_not_proxied_names() {
        assert_eq!(split_namespaced("spawn_agent"), None);
        assert_eq!(split_namespaced(""), None);
        assert_eq!(
            split_namespaced("single_underscore"),
            None,
            "one underscore is not the separator"
        );
    }

    #[test]
    fn an_empty_half_is_refused_rather_than_routed() {
        assert_eq!(split_namespaced("__tool"), None, "no server half");
        assert_eq!(split_namespaced("server__"), None, "no tool half");
        assert_eq!(split_namespaced("__"), None);
    }
}
