//! The joining side of enrollment (DESIGN §3).
//!
//! `join` is what both `horde-worker join` and the Client's enroll verb run:
//! generate a keypair locally, dial the Server's enrollment listener trusting
//! only the fingerprint the code embedded, redeem the code with a CSR, and
//! come back with a signed identity. The private key is generated here and
//! never leaves the process except into the identity directory.

use horde_proto::v1::enrollment_service_client::EnrollmentServiceClient;
use horde_proto::v1::{Hello, JoinRequest, join_response};

use crate::error::IdentityError;
use crate::fingerprint::JoinCode;
use crate::principal::{Principal, PrincipalRole};
use crate::store::StoredIdentity;
use crate::tls::pinned_verifier;

/// A fresh keypair and the CSR that proves possession of it.
///
/// The CSR carries no subject worth speaking of — the Server overwrites it
/// at signing — so nothing here asks who the key is for.
///
/// # Errors
///
/// Key generation or CSR serialization failing.
pub fn generate_csr() -> Result<(String, String), IdentityError> {
    let key = rcgen::KeyPair::generate()?;
    let csr = rcgen::CertificateParams::default().serialize_request(&key)?;
    Ok((key.serialize_pem(), csr.pem()?))
}

/// What a successful join hands back: the identity to persist, and who the
/// Server says the peer now is.
#[derive(Debug, Clone)]
pub struct EnrolledIdentity {
    pub identity: StoredIdentity,
    pub principal: Principal,
    /// The Server's main (mTLS) listener port, when the answer named it
    /// (ADR-0007): with the token's host beside it, this is the
    /// control-plane endpoint the joining machine stores.
    pub control_plane_port: Option<u16>,
}

/// What can go wrong joining.
#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    #[error(transparent)]
    Identity(#[from] IdentityError),

    /// The listener could not be reached — including the pinned-fingerprint
    /// verifier refusing the server, which surfaces as a connect failure.
    #[error("could not reach the enrollment listener at {endpoint}: {source}")]
    Transport {
        endpoint: String,
        #[source]
        source: tonic::transport::Error,
    },

    /// The Server answered, and said no: a wrong, expired, or spent code.
    #[error("the server refused enrollment: {0}")]
    Refused(#[source] tonic::Status),

    /// A success response missing what the protocol promises.
    #[error("the server's answer was missing {0}")]
    Incomplete(&'static str),
}

/// Redeems a join code at `endpoint` (an `https://` URI) and returns the
/// enrolled identity.
///
/// # Errors
///
/// [`JoinError`], which distinguishes "could not reach" from "was refused"
/// because the two need different reactions: one is retried, the other means
/// a new code.
pub async fn join(
    endpoint: &str,
    code: &JoinCode,
    hello: Hello,
) -> Result<EnrolledIdentity, JoinError> {
    let transport = |source| JoinError::Transport {
        endpoint: endpoint.to_owned(),
        source,
    };

    let (key_pem, csr_pem) = generate_csr()?;

    let channel = tonic::transport::Endpoint::from_shared(endpoint.to_owned())
        .map_err(transport)?
        .tls_config_with_verifier(
            tonic::transport::ClientTlsConfig::new(),
            pinned_verifier(&code.server_fingerprint),
        )
        .map_err(transport)?
        .connect()
        .await
        .map_err(transport)?;

    let response = EnrollmentServiceClient::new(channel)
        .join(JoinRequest {
            enrollment_code: code.secret.clone(),
            csr_pem,
            hello: Some(hello),
            // The principal's name was fixed when the code was minted; there
            // is nothing useful for the joining machine to claim here.
            name: String::new(),
        })
        .await
        .map_err(JoinError::Refused)?
        .into_inner();

    if response.certificate_pem.is_empty() {
        return Err(JoinError::Incomplete("a certificate"));
    }
    if response.ca_bundle_pem.is_empty() {
        return Err(JoinError::Incomplete("the CA bundle"));
    }
    let principal = match response.principal {
        Some(join_response::Principal::WorkerId(id)) => {
            Principal::new(PrincipalRole::Worker, id.to_string())
        }
        Some(join_response::Principal::ClientId(id)) => {
            Principal::new(PrincipalRole::Client, id.to_string())
        }
        None => return Err(JoinError::Incomplete("a principal")),
    }
    .map_err(|_| JoinError::Incomplete("a usable principal name"))?;

    Ok(EnrolledIdentity {
        identity: StoredIdentity {
            certificate_pem: response.certificate_pem,
            key_pem,
            ca_pem: response.ca_bundle_pem,
        },
        principal,
        // Zero is proto3's absent, and a listener has no port 0.
        control_plane_port: u16::try_from(response.control_plane_port)
            .ok()
            .filter(|port| *port != 0),
    })
}

#[cfg(test)]
mod tests {
    use super::generate_csr;

    /// The CSR is checked here for shape only; that a CA can actually sign
    /// it — the property that matters — is held by `ca::tests`, and the
    /// whole flow against a live enrollment listener by `horde-server`'s
    /// suite.
    #[test]
    fn a_generated_csr_is_pem_and_the_key_stays_private_shaped() {
        let (key_pem, csr_pem) = generate_csr().expect("generation succeeds");
        assert!(key_pem.contains("BEGIN PRIVATE KEY"), "{key_pem:?}");
        assert!(csr_pem.contains("BEGIN CERTIFICATE REQUEST"), "{csr_pem:?}");
        let (again, _) = generate_csr().expect("generation succeeds");
        assert_ne!(key_pem, again, "every join generates a fresh key");
    }
}
