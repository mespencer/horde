//! Ergonomics for the wrapper identifier types.
//!
//! The identifiers are separate messages so that no language's generated code
//! will pass an `AgentId` where a `SessionId` belongs. That costs a `.value`
//! hop everywhere, which these impls pay back.

/// Gives each single-field identifier message a constructor and a `Display`
/// that renders the bare value.
macro_rules! impl_id {
    ($($ty:ident),+ $(,)?) => {
        $(
            impl crate::v1::$ty {
                /// Wraps an identifier string that already exists.
                ///
                /// This does not mint a new identifier: identifiers are
                /// assigned by the Server.
                pub fn new(value: impl Into<String>) -> Self {
                    Self { value: value.into() }
                }
            }

            impl core::fmt::Display for crate::v1::$ty {
                fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                    f.write_str(&self.value)
                }
            }
        )+
    };
}

impl_id!(
    AgentId,
    AssignmentId,
    ClientId,
    ForwardId,
    PermissionRequestId,
    SessionId,
    WorkerId,
    WorkspaceDefinitionId,
);

#[cfg(test)]
mod tests {
    use crate::v1::{AgentId, SessionId};

    #[test]
    fn display_renders_the_bare_identifier_value() {
        assert_eq!(SessionId::new("ci-fix").to_string(), "ci-fix");
        assert_eq!(AgentId::new("agent-7").to_string(), "agent-7");
    }

    #[test]
    fn new_accepts_both_borrowed_and_owned_strings() {
        let owned = String::from("agent-7");
        assert_eq!(AgentId::new(owned), AgentId::new("agent-7"));
    }
}
