//! The gRPC door: every RPC on an mTLS channel carries an enrolled,
//! unrevoked principal, or it does not happen.
//!
//! tonic owns the handshake on gRPC listeners, so chain verification against
//! the CA has already happened by the time a request exists. What remains is
//! interpretation and policy — who does this certificate name, and is it
//! still welcome — which is [`admit`], a pure decision over the presented
//! DER, wrapped in a tonic interceptor that stamps the answer into the
//! request's extensions for the services behind it.

use tonic::Status;

use crate::principal::{PeerIdentity, peer_identity};
use crate::revocation::RevocationList;

/// Why a presented (or absent) certificate was turned away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmitRefusal {
    /// The channel carried no client certificate at all.
    NoCertificate,
    /// A certificate that chains correctly but names no principal.
    NoPrincipal,
    /// An enrolled certificate whose serial has been revoked.
    Revoked { identity: PeerIdentity },
}

impl From<AdmitRefusal> for Status {
    fn from(refusal: AdmitRefusal) -> Self {
        match refusal {
            // Unauthenticated: the caller may retry with credentials.
            AdmitRefusal::NoCertificate => {
                Status::unauthenticated("this channel requires an enrolled client certificate")
            }
            AdmitRefusal::NoPrincipal => {
                Status::unauthenticated("the client certificate names no principal")
            }
            // PermissionDenied: the caller is known, and the answer is no —
            // retrying with the same certificate can never succeed.
            AdmitRefusal::Revoked { identity } => Status::permission_denied(format!(
                "certificate {} for {} has been revoked",
                identity.serial_hex, identity.principal
            )),
        }
    }
}

/// Decides whether a presented end-entity certificate is admitted.
///
/// Pure over its inputs so the whole truth table is assertable with real
/// issued certificates and no transport: `None` (no certificate), a
/// certificate with no principal, a revoked serial, and the happy path.
///
/// # Errors
///
/// The [`AdmitRefusal`] naming which of the three doors closed.
pub fn admit(
    end_entity_der: Option<&[u8]>,
    deny: &RevocationList,
) -> Result<PeerIdentity, AdmitRefusal> {
    let der = end_entity_der.ok_or(AdmitRefusal::NoCertificate)?;
    let identity = peer_identity(der).map_err(|_| AdmitRefusal::NoPrincipal)?;
    if deny.is_revoked(&identity.serial_hex) {
        return Err(AdmitRefusal::Revoked { identity });
    }
    Ok(identity)
}

/// A tonic interceptor requiring an enrolled, unrevoked peer on every RPC.
///
/// On success the [`PeerIdentity`] is inserted into the request extensions —
/// how a service behind this door learns who is calling without touching the
/// transport. Revocation is re-checked here on *every* RPC, so a revoked
/// principal's already-open connection stops working at its next call rather
/// than at its next reconnect.
pub fn require_enrolled(
    deny: RevocationList,
) -> impl FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, Status> + Clone {
    move |mut request: tonic::Request<()>| {
        let certificates = request.peer_certs();
        let end_entity = certificates
            .as_deref()
            .and_then(|chain| chain.first())
            .map(AsRef::as_ref);
        let identity = admit(end_entity, &deny).map_err(Status::from)?;
        request.extensions_mut().insert(identity);
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::{AdmitRefusal, admit, require_enrolled};
    use crate::ca::CertificateAuthority;
    use crate::enroll::generate_csr;
    use crate::principal::{Principal, PrincipalRole};
    use crate::revocation::RevocationList;

    const NOW_MS: i64 = 1_754_956_800_000;

    /// A certificate off the production issuance path, as the door sees it.
    fn issued_der() -> (Vec<u8>, String) {
        use rustls_pki_types::{CertificateDer, pem::PemObject};

        let ca = CertificateAuthority::generate(NOW_MS).expect("a CA generates");
        let principal = Principal::new(PrincipalRole::Worker, "mac-studio").expect("a name");
        let (_key, csr) = generate_csr().expect("a CSR generates");
        let issued = ca.sign_join(&csr, &principal, NOW_MS).expect("signs");
        let der = CertificateDer::from_pem_slice(issued.certificate_pem.as_bytes())
            .expect("issued PEM parses")
            .to_vec();
        (der, issued.serial_hex)
    }

    #[test]
    fn the_admission_truth_table() {
        let (der, serial_hex) = issued_der();
        let deny = RevocationList::new();

        // No certificate.
        assert_eq!(admit(None, &deny), Err(AdmitRefusal::NoCertificate));

        // A certificate that is not one of ours in shape.
        assert_eq!(
            admit(Some(b"garbage"), &deny),
            Err(AdmitRefusal::NoPrincipal)
        );

        // Enrolled and welcome.
        let identity = admit(Some(&der), &deny).expect("an enrolled peer is admitted");
        assert_eq!(identity.principal.to_string(), "worker:mac-studio");
        assert_eq!(identity.serial_hex, serial_hex);

        // Enrolled and revoked.
        deny.revoke(serial_hex);
        let refused = admit(Some(&der), &deny);
        assert!(
            matches!(refused, Err(AdmitRefusal::Revoked { ref identity })
                if identity.principal.to_string() == "worker:mac-studio"),
            "got {refused:?}"
        );
    }

    #[test]
    fn refusals_map_to_the_grpc_codes_callers_react_to() {
        let (der, serial_hex) = issued_der();
        let deny = RevocationList::new();
        deny.revoke(&serial_hex);

        let status = tonic::Status::from(AdmitRefusal::NoCertificate);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);

        let status = tonic::Status::from(AdmitRefusal::NoPrincipal);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);

        let Err(revoked) = admit(Some(&der), &deny) else {
            panic!("a revoked serial must refuse");
        };
        let status = tonic::Status::from(revoked);
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(
            status.message().contains(&serial_hex)
                && status.message().contains("worker:mac-studio"),
            "the refusal names who and which certificate: {}",
            status.message()
        );
    }

    /// A request with no TLS connect-info — a plaintext channel — must be
    /// refused. The with-certificate half of the interceptor rides real mTLS
    /// channels in `horde-server`'s suites; tonic offers no way to forge
    /// connect-info here, which is rather the point.
    #[test]
    fn a_request_without_transport_identity_is_refused() {
        let mut interceptor = require_enrolled(RevocationList::new());
        let refused = interceptor(tonic::Request::new(()));
        assert_eq!(
            refused.map(|_| ()).map_err(|status| status.code()),
            Err(tonic::Code::Unauthenticated)
        );
    }
}
