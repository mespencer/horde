//! Where an invocation dials and as whom (ADR-0007), decided before any
//! dial. Its own module, below both the verb table and `horde doctor`,
//! because doctor diagnoses exactly this decision — and a doctor reaching
//! back into the verb table for it would make the two depend on each other.

use crate::{Client, ClientError};

/// One invocation's connection facts, decided before any dial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConnection {
    /// The endpoint this invocation dials.
    pub endpoint: String,
    /// The identity directory in effect: the flag's, or the default.
    pub identity_dir: std::path::PathBuf,
    /// Whether the directory was named explicitly (`--identity`).
    pub explicit_identity: bool,
    /// Whether the directory holds a complete identity.
    pub enrolled: bool,
}

/// Resolves where to dial and as whom (ADR-0007).
///
/// Endpoint precedence: explicit flag, then environment (clap merges those
/// two before this runs), then the endpoint the identity dir stored at
/// join. There is no default: the old plaintext `http://127.0.0.1:7777`
/// could never connect to the secured server it implied (ADR-0007,
/// 2026-10-31 amendment). No endpoint from any source is a refusal — for
/// an enrolled machine, one that says where endpoints come from; for a
/// machine with nothing, one that says how to get a server at all.
///
/// # Errors
///
/// The enrolled-but-endpointless refusal, or the no-server refusal.
pub fn resolve_connection(
    endpoint: Option<&str>,
    identity: Option<&std::path::Path>,
    home: Option<std::path::PathBuf>,
) -> Result<ResolvedConnection, ClientError> {
    let identity_dir = identity.map_or_else(|| default_identity_dir(home), |dir| dir.to_path_buf());
    let enrolled = horde_identity::StoredIdentity::exists(&identity_dir);
    let endpoint = match endpoint {
        Some(endpoint) => endpoint.to_owned(),
        None => match horde_identity::store::load_endpoint(&identity_dir) {
            Some(stored) => stored,
            None if enrolled => {
                return Err(ClientError::NoEndpoint {
                    directory: identity_dir,
                });
            }
            None => {
                return Err(ClientError::NoServer {
                    directory: identity_dir,
                });
            }
        },
    };
    Ok(ResolvedConnection {
        endpoint,
        identity_dir,
        explicit_identity: identity.is_some(),
        enrolled,
    })
}

/// How one resolved invocation authenticates — a decision, as data, so the
/// truth table is testable without sockets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// Load the identity dir and speak mTLS.
    Mtls,
    /// Dial plaintext — an `http://` endpoint asked for exactly that.
    Plaintext,
    /// An `https://` endpoint with no identity anywhere: refused with the
    /// fix, because dialing would fail with a TLS mystery instead.
    RefusedUnenrolled,
}

/// The transport a resolution implies.
///
/// An explicit `--identity` always speaks mTLS (refusing to load is louder
/// and better than a plaintext connection wearing an enrolled flag); the
/// *defaulted* dir is used only against an `https://` endpoint, so an
/// explicit plaintext endpoint stays plaintext whatever `~/.horde/id`
/// happens to hold.
#[must_use]
pub fn transport_for(resolved: &ResolvedConnection) -> Transport {
    if resolved.explicit_identity {
        return Transport::Mtls;
    }
    if !resolved.endpoint.starts_with("https://") {
        return Transport::Plaintext;
    }
    if resolved.enrolled {
        Transport::Mtls
    } else {
        Transport::RefusedUnenrolled
    }
}

/// Connects as the resolution says.
pub(crate) async fn connect(resolved: &ResolvedConnection) -> Result<Client, ClientError> {
    match transport_for(resolved) {
        Transport::Mtls => {
            let identity =
                horde_identity::StoredIdentity::load(&resolved.identity_dir).map_err(|source| {
                    ClientError::Identity {
                        directory: resolved.identity_dir.clone(),
                        source,
                    }
                })?;
            Client::connect_with_identity(&resolved.endpoint, &identity).await
        }
        Transport::Plaintext => Client::connect(&resolved.endpoint).await,
        Transport::RefusedUnenrolled => Err(ClientError::Unenrolled {
            endpoint: resolved.endpoint.clone(),
            directory: resolved.identity_dir.clone(),
        }),
    }
}

/// Where this machine's identity lives when `--identity` does not say:
/// `~/.horde/id` (ADR-0007).
#[must_use]
pub fn default_identity_dir(home: Option<std::path::PathBuf>) -> std::path::PathBuf {
    home.map_or_else(
        || std::path::PathBuf::from(".horde-identity"),
        |home| home.join(".horde").join("id"),
    )
}
