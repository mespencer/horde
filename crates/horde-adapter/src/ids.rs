//! Identifiers that cross the adapter seam.

use core::fmt;

/// Wraps a newtype over `String` with the constructor, `Display`, and borrow
/// conversions every identifier here wants, without inviting them to be mixed
/// up with each other.
macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_id! {
    /// The provider harness's own conversation identifier.
    ///
    /// This is what revival is built on: Horde hands it back to the harness to
    /// resume a conversation whose process is long dead. It is opaque and
    /// provider-scoped — for Claude it is additionally scoped to the provider
    /// account's config directory, so it is only meaningful alongside the
    /// Agent's [`ProviderAccount`].
    ProviderSessionId
}

string_id! {
    /// Names an isolated credential/state bundle on a Worker.
    ///
    /// Part of an Agent's revival identity, not just a spawn-time choice: a
    /// provider session recorded under one account cannot be resumed under
    /// another.
    ProviderAccount
}

string_id! {
    /// Groups the output chunks and usage of a single turn.
    ///
    /// Minted by the adapter, because only the adapter can see where the
    /// harness's turn boundaries actually fall.
    TurnId
}

string_id! {
    /// The harness's identifier for one proposed tool call.
    ///
    /// Correlates a proposal, its permission decision, and its result.
    ToolCallId
}

#[cfg(test)]
mod tests {
    use super::{ProviderSessionId, ToolCallId, TurnId};

    #[test]
    fn display_renders_the_bare_value() {
        assert_eq!(TurnId::new("turn-1").to_string(), "turn-1");
    }

    #[test]
    fn new_accepts_borrowed_and_owned_strings() {
        let owned = String::from("toolu_01");
        assert_eq!(ToolCallId::new(owned), ToolCallId::new("toolu_01"));
    }

    #[test]
    fn into_string_returns_the_wrapped_value() {
        let id = ProviderSessionId::new("abc-123");
        assert_eq!(id.into_string(), "abc-123");
    }
}
