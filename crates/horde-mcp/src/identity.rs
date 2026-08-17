//! Per-Agent identity, validated on every request.
//!
//! # Why headers, and why every request
//!
//! The dual-era spike (`docs/spikes/rmcp-dual-era.md` §3) measured two things
//! that between them rule out every other place identity could live:
//!
//! * **`clientInfo` is not the client's.** Under rmcp's default
//!   `legacy_session_mode = false` the handler sees rmcp's own placeholder
//!   `Implementation` (`rmcp` / `3.1.2`) rather than `claude-code` / `2.1.228`.
//!   Nothing may be built on `clientInfo` for legacy clients.
//! * **There is no handshake to gate on.** A legacy request is served without
//!   `initialize` ever happening, so "the session is authenticated" is not a
//!   state this endpoint can have. Authentication has to be enforced by Horde's
//!   own middleware on each request or it is not enforced at all.
//!
//! The 2026-07-28 era removes protocol sessions outright, so per-request
//! identity is also the shape the modern era wants. One mechanism covers both
//! eras, which is the whole reason it is a header pair rather than session
//! state.
//!
//! Both harnesses can carry it. Claude Code takes `headers` in its
//! `--mcp-config` entry; Codex takes `bearer_token_env_var` plus arbitrary
//! `http_headers` in `config.toml`. The spike confirmed both on the wire on
//! *every* request, including `initialize` and the GET stream.

use std::collections::HashMap;

/// A caller this endpoint has authenticated.
///
/// Constructed only by [`AgentRegistry::authenticate`], so holding one is proof
/// the token matched — there is no way to assemble an unverified identity and
/// pass it to something that trusts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentIdentity {
    agent_id: String,
}

impl AgentIdentity {
    /// The Agent this request speaks for.
    #[must_use]
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }
}

/// Why a request carried no usable identity.
///
/// Every variant is a refusal. None of them is a "degraded but continue" state:
/// an endpoint that served an unidentified caller would be serving one Agent's
/// orchestrator verbs to another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    /// No `Authorization` header at all.
    MissingAuthorization,
    /// `Authorization` was present but not `Bearer <token>`.
    ///
    /// Distinguished from [`Self::MissingAuthorization`] because the two send an
    /// operator to different places: one is an unconfigured client, the other a
    /// misconfigured one.
    MalformedAuthorization,
    /// No `X-Horde-Agent` header.
    MissingAgentHeader,
    /// A header held bytes that are not a valid header string.
    NotAscii(&'static str),
    /// The agent is unknown, or the token does not match the one minted for it.
    ///
    /// Deliberately one variant rather than two. Telling a caller "that agent
    /// exists but your token is wrong" hands it the agent half of a credential
    /// pair it has already failed to prove it holds.
    Unauthenticated,
}

impl core::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingAuthorization => write!(f, "no Authorization header"),
            Self::MalformedAuthorization => {
                write!(f, "Authorization must be `Bearer <token>`")
            }
            Self::MissingAgentHeader => write!(f, "no {AGENT_HEADER} header"),
            Self::NotAscii(which) => write!(f, "{which} is not a valid header value"),
            Self::Unauthenticated => write!(f, "unknown agent or wrong token"),
        }
    }
}

impl core::error::Error for IdentityError {}

/// The header naming the Agent a request speaks for.
pub const AGENT_HEADER: &str = "x-horde-agent";

/// The token header. Standard `Authorization: Bearer <token>`.
pub const AUTHORIZATION_HEADER: &str = "authorization";

/// The per-Agent tokens this endpoint will accept.
///
/// One endpoint serves many Agents on one port, so the token is what separates
/// them; the `X-Horde-Agent` header alone is an unauthenticated claim and is
/// never trusted on its own.
///
/// Interior-mutable on purpose: the serving loop holds this behind an `Arc`
/// for the life of the endpoint, and the Worker mints a token for each Agent
/// it spawns *after* the endpoint is already serving. A registry that could
/// only be populated before serving would freeze the toolset to whichever
/// agents existed at bind time.
#[derive(Debug, Default)]
pub struct AgentRegistry {
    tokens: std::sync::RwLock<HashMap<String, String>>,
}

impl AgentRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the token minted for an Agent, replacing any previous one.
    pub fn insert(&self, agent_id: impl Into<String>, token: impl Into<String>) {
        self.write().insert(agent_id.into(), token.into());
    }

    /// Forgets an Agent, so its token stops working immediately.
    pub fn remove(&self, agent_id: &str) {
        self.write().remove(agent_id);
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, String>> {
        // A poisoned lock means a panic mid-insert; the map itself is still
        // coherent (single-key operations), and refusing every later request
        // would turn one panic into a dead endpoint.
        self.tokens
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Validates a request's headers.
    ///
    /// # Errors
    ///
    /// Any [`IdentityError`]. The caller turns all of them into a refusal
    /// before the request reaches rmcp.
    pub fn authenticate(&self, headers: &http::HeaderMap) -> Result<AgentIdentity, IdentityError> {
        let authorization = headers
            .get(AUTHORIZATION_HEADER)
            .ok_or(IdentityError::MissingAuthorization)?
            .to_str()
            .map_err(|_| IdentityError::NotAscii("Authorization"))?;

        let presented = authorization
            .strip_prefix("Bearer ")
            .or_else(|| authorization.strip_prefix("bearer "))
            .ok_or(IdentityError::MalformedAuthorization)?
            .trim();

        let agent_id = headers
            .get(AGENT_HEADER)
            .ok_or(IdentityError::MissingAgentHeader)?
            .to_str()
            .map_err(|_| IdentityError::NotAscii(AGENT_HEADER))?;

        let expected = {
            let tokens = self
                .tokens
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            tokens
                .get(agent_id)
                .cloned()
                .ok_or(IdentityError::Unauthenticated)?
        };

        // Length-independent comparison is not the point here — the tokens are
        // server-minted and equal length — but a mismatch must not reveal
        // *which* half failed, which is why both misses land on one variant.
        if !constant_time_eq(presented.as_bytes(), expected.as_bytes()) {
            return Err(IdentityError::Unauthenticated);
        }

        Ok(AgentIdentity {
            agent_id: agent_id.to_owned(),
        })
    }
}

/// Compares two byte strings without an early return on the first difference.
///
/// The endpoint is loopback-only, so this is defence in depth rather than a
/// response to a measured threat — but a token comparison that returns early is
/// the kind of thing that stops being harmless the moment the bind address
/// changes, and DESIGN §3 has this endpoint growing real remote exposure.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |differences, (a, b)| differences | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::{AGENT_HEADER, AgentRegistry, IdentityError};

    fn registry() -> AgentRegistry {
        let registry = AgentRegistry::new();
        registry.insert("agent-42", "horde-agent-42-token");
        registry
    }

    fn headers(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut map = http::HeaderMap::new();
        for (name, value) in pairs {
            let name: http::HeaderName = name.parse().expect("test header names must be valid");
            let value: http::HeaderValue = value.parse().expect("test header values must be valid");
            map.insert(name, value);
        }
        map
    }

    /// The fold accumulates differences with `|`. An accumulator that
    /// cancelled — xor — would read two positions differing by the same bits
    /// as equal, and `"ab"` against `"ba"` is exactly that pair: both
    /// positions differ by `'a' ^ 'b'`, so a cancelling fold comes out zero.
    #[test]
    fn differences_must_not_cancel_each_other_out() {
        assert!(
            !super::constant_time_eq(b"ab", b"ba"),
            "two differences that xor to nothing are still two differences"
        );
        assert!(
            super::constant_time_eq(b"ab", b"ab"),
            "the positive control: identical bytes still compare equal"
        );
    }

    /// The shape the spike recorded off the wire from Claude Code 2.1.228.
    #[test]
    fn the_recorded_header_pair_authenticates() {
        let identity = registry()
            .authenticate(&headers(&[
                ("authorization", "Bearer horde-agent-42-token"),
                (AGENT_HEADER, "agent-42"),
            ]))
            .expect("the recorded pair must authenticate");

        assert_eq!(identity.agent_id(), "agent-42");
    }

    /// Codex lowercases its header names on the wire; `HeaderMap` is
    /// case-insensitive and the scheme token is matched both ways, so neither
    /// harness's spelling is privileged.
    #[test]
    fn the_bearer_scheme_is_matched_case_insensitively() {
        let identity = registry()
            .authenticate(&headers(&[
                ("Authorization", "bearer horde-agent-42-token"),
                ("X-Horde-Agent", "agent-42"),
            ]))
            .expect("lowercase `bearer` is the same scheme");

        assert_eq!(identity.agent_id(), "agent-42");
    }

    #[test]
    fn a_request_with_no_identity_is_refused() {
        assert_eq!(
            registry().authenticate(&headers(&[])),
            Err(IdentityError::MissingAuthorization)
        );
    }

    #[test]
    fn a_token_without_the_bearer_scheme_is_refused() {
        assert_eq!(
            registry().authenticate(&headers(&[
                ("authorization", "horde-agent-42-token"),
                (AGENT_HEADER, "agent-42"),
            ])),
            Err(IdentityError::MalformedAuthorization)
        );
    }

    #[test]
    fn a_token_without_an_agent_header_is_refused() {
        // The token alone does not say who is calling, and this endpoint serves
        // many Agents on one port.
        assert_eq!(
            registry().authenticate(&headers(&[(
                "authorization",
                "Bearer horde-agent-42-token"
            )])),
            Err(IdentityError::MissingAgentHeader)
        );
    }

    #[test]
    fn an_unknown_agent_is_refused() {
        assert_eq!(
            registry().authenticate(&headers(&[
                ("authorization", "Bearer horde-agent-42-token"),
                (AGENT_HEADER, "agent-99"),
            ])),
            Err(IdentityError::Unauthenticated)
        );
    }

    /// The impersonation attempt this whole module exists to stop: a real agent
    /// id presented with a token minted for somebody else.
    #[test]
    fn one_agents_token_does_not_authenticate_another_agent() {
        let registry = registry();
        registry.insert("agent-77", "horde-agent-77-token");

        assert_eq!(
            registry.authenticate(&headers(&[
                ("authorization", "Bearer horde-agent-77-token"),
                (AGENT_HEADER, "agent-42"),
            ])),
            Err(IdentityError::Unauthenticated),
            "a valid token for another agent must not authenticate this one"
        );
    }

    #[test]
    fn a_wrong_token_and_an_unknown_agent_are_indistinguishable() {
        // Two different mistakes, one answer: telling a caller that an agent
        // exists is itself a disclosure once it has failed to prove it holds
        // that agent's token.
        let wrong_token = registry().authenticate(&headers(&[
            ("authorization", "Bearer not-the-token"),
            (AGENT_HEADER, "agent-42"),
        ]));
        let unknown_agent = registry().authenticate(&headers(&[
            ("authorization", "Bearer not-the-token"),
            (AGENT_HEADER, "nobody"),
        ]));

        assert_eq!(wrong_token, Err(IdentityError::Unauthenticated));
        assert_eq!(wrong_token, unknown_agent);
    }

    /// The property interior mutability exists for: an agent registered
    /// *after* the registry is already shared (the serving loop holds an
    /// `Arc`) authenticates through that same shared handle. Without it, a
    /// Worker could never mint a token for an agent spawned after bind time.
    #[test]
    fn an_agent_registered_after_sharing_authenticates_through_the_shared_handle() {
        let shared = std::sync::Arc::new(AgentRegistry::new());
        let serving_side = std::sync::Arc::clone(&shared);

        assert_eq!(
            serving_side.authenticate(&headers(&[
                ("authorization", "Bearer late-token"),
                (AGENT_HEADER, "late-agent"),
            ])),
            Err(IdentityError::Unauthenticated),
            "the agent does not exist yet"
        );

        shared.insert("late-agent", "late-token");

        let identity = serving_side
            .authenticate(&headers(&[
                ("authorization", "Bearer late-token"),
                (AGENT_HEADER, "late-agent"),
            ]))
            .expect("a late-registered agent must authenticate");
        assert_eq!(identity.agent_id(), "late-agent");
    }

    #[test]
    fn a_removed_agent_stops_authenticating() {
        let registry = registry();
        registry.remove("agent-42");

        assert_eq!(
            registry.authenticate(&headers(&[
                ("authorization", "Bearer horde-agent-42-token"),
                (AGENT_HEADER, "agent-42"),
            ])),
            Err(IdentityError::Unauthenticated)
        );
    }

    /// Each refusal's message is the only thing an operator sees, so each one
    /// has to say which refusal it was.
    #[test]
    fn every_refusal_explains_itself_distinctly() {
        let cases = [
            (
                IdentityError::MissingAuthorization,
                "no Authorization header",
            ),
            (
                IdentityError::MalformedAuthorization,
                "Authorization must be `Bearer <token>`",
            ),
            (IdentityError::MissingAgentHeader, "no x-horde-agent header"),
            (
                IdentityError::NotAscii("Authorization"),
                "Authorization is not a valid header value",
            ),
            (
                IdentityError::Unauthenticated,
                "unknown agent or wrong token",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }
}
