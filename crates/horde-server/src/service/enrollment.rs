//! The enrollment authority, and the one service reachable without a client
//! certificate.
//!
//! [`EnrollmentAuthority`] is the side-effecting half of enrollment: it holds
//! the CA and the log, folds the reserved enrollment stream
//! ([`crate::enrollment`]) to decide, and appends the events that record what
//! it did — a mint, an issuance, a revocation — so a Server restart
//! reconstructs enrollment state the way it reconstructs everything else
//! (ADR-0001). [`Enrollment`] is the tonic service over its redeem half,
//! served on its own TLS-only listener because it is a distinct trust tier
//! (`enrollment.proto`), while mint and revoke are Control Plane RPCs behind
//! mTLS.
//!
//! Operations serialize on one gate. Enrollment is human-paced — the gate
//! costs nothing — and it makes read-fold-decide-append atomic, which is what
//! keeps one code from redeeming twice in a race.

use std::sync::Arc;

use horde_identity::{
    CertificateAuthority, IssuedCertificate, Principal, PrincipalRole, RevocationList,
    ServerCredential,
};
use horde_proto::v1::enrollment_service_server::EnrollmentService;
use horde_proto::v1::{
    EnrollmentCodeMinted, EnrollmentRevoked, Event, IdentityEnrolled, JoinRequest, JoinResponse,
    event::Kind, join_response,
};
use tonic::{Request, Response, Status};

use crate::enrollment::{
    ENROLLMENT_STREAM, EnrollmentRoster, RedeemRefusal, ms_to_timestamp, role_to_wire,
};
use crate::error::ServerError;
use crate::log::EventLog;
use crate::service::shared::append_server_event;

/// A minted enrollment code, ready to be carried to the joining machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintedCode {
    /// The one-time secret. This is the only moment it exists in cleartext
    /// server-side; the log records its hash.
    pub secret: String,
    /// The Server certificate fingerprint the code embeds (DESIGN §3).
    pub server_fingerprint: String,
    pub expires_at_unix_ms: i64,
}

impl MintedCode {
    /// The `secret.fingerprint` token a human carries (one copy-pasteable
    /// string, per DESIGN §3's "code embedding the server cert fingerprint").
    #[must_use]
    pub fn join_code(&self) -> String {
        format!("{}.{}", self.secret, self.server_fingerprint)
    }
}

/// The Server's enrollment side: the CA, the deny-list, and the log.
pub struct EnrollmentAuthority {
    log: EventLog,
    ca: CertificateAuthority,
    credential: ServerCredential,
    deny: RevocationList,
    /// Serializes read-fold-decide-append so a code cannot redeem twice.
    gate: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for EnrollmentAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrollmentAuthority")
            .field("deny", &self.deny)
            .finish_non_exhaustive()
    }
}

impl EnrollmentAuthority {
    /// Builds the authority and seeds the deny-list from the log, so
    /// revocations survive a Server restart.
    ///
    /// # Errors
    ///
    /// Reading the enrollment stream failing.
    pub async fn new(
        log: EventLog,
        ca: CertificateAuthority,
        credential: ServerCredential,
    ) -> Result<Self, ServerError> {
        let deny = RevocationList::new();
        let roster = Self::roster_of(&log).await?;
        for serial in roster.revoked_serials() {
            deny.revoke(serial);
        }
        Ok(Self {
            log,
            ca,
            credential,
            deny,
            gate: tokio::sync::Mutex::new(()),
        })
    }

    async fn roster_of(log: &EventLog) -> Result<EnrollmentRoster, ServerError> {
        EnrollmentRoster::project(&log.read_all(ENROLLMENT_STREAM).await?)
    }

    /// The deny-list handle every channel door shares.
    #[must_use]
    pub fn deny_list(&self) -> RevocationList {
        self.deny.clone()
    }

    /// The trust root every peer receives at join.
    #[must_use]
    pub fn ca_pem(&self) -> &str {
        self.ca.certificate_pem()
    }

    /// The credential every listener presents.
    #[must_use]
    pub fn credential(&self) -> &ServerCredential {
        &self.credential
    }

    /// Mints a one-time code for `principal` (DESIGN §3).
    ///
    /// # Errors
    ///
    /// Randomness or fingerprinting failing, or the log refusing the record —
    /// a code whose mint was not journaled was never minted.
    pub async fn mint(
        &self,
        principal: &Principal,
        requested_ttl_ms: i64,
        now_unix_ms: i64,
    ) -> Result<MintedCode, ServerError> {
        let _held = self.gate.lock().await;

        let secret = horde_identity::code::mint_secret().map_err(identity_internal)?;
        let code_hash = horde_identity::code::hash_secret(&secret);
        let expires_at_unix_ms =
            now_unix_ms.saturating_add(horde_identity::code::clamp_ttl_ms(requested_ttl_ms));

        append_server_event(
            &self.log,
            ENROLLMENT_STREAM,
            &format!("enroll-mint-{code_hash}"),
            Event {
                agent_id: None,
                occurred_at: Some(ms_to_timestamp(now_unix_ms)),
                idempotency_key: format!("enroll-mint-{code_hash}"),
                kind: Some(Kind::EnrollmentCodeMinted(EnrollmentCodeMinted {
                    role: role_to_wire(principal.role()).into(),
                    name: principal.name().to_owned(),
                    code_hash,
                    expires_at: Some(ms_to_timestamp(expires_at_unix_ms)),
                })),
            },
        )
        .await?;

        Ok(MintedCode {
            secret,
            server_fingerprint: self.credential.fingerprint().map_err(identity_internal)?,
            expires_at_unix_ms,
        })
    }

    /// Redeems a code against a CSR: the join, server side.
    ///
    /// # Errors
    ///
    /// [`ServerError::EnrollmentRefused`] for a wrong, expired, or spent
    /// code; [`ServerError::Invalid`] for an unreadable CSR; log failures
    /// otherwise.
    pub async fn redeem(
        &self,
        secret: &str,
        csr_pem: &str,
        now_unix_ms: i64,
    ) -> Result<(Principal, IssuedCertificate), ServerError> {
        let _held = self.gate.lock().await;

        let roster = Self::roster_of(&self.log).await?;
        let code =
            roster
                .redeem_decision(secret, now_unix_ms)
                .map_err(|refusal| match refusal {
                    RedeemRefusal::Unknown => ServerError::EnrollmentRefused("no such code"),
                    RedeemRefusal::Expired => {
                        ServerError::EnrollmentRefused("the code has expired")
                    }
                    RedeemRefusal::Spent => {
                        ServerError::EnrollmentRefused("the code was already used")
                    }
                })?;
        let principal = Principal::new(code.role, code.name.clone())
            .map_err(|error| ServerError::Invalid(error.to_string()))?;

        let issued = self
            .ca
            .sign_join(csr_pem, &principal, now_unix_ms)
            .map_err(|error| match error {
                horde_identity::IdentityError::UnreadablePem { .. } => {
                    ServerError::Invalid("the CSR is not readable PEM".to_owned())
                }
                other => identity_internal(other),
            })?;

        append_server_event(
            &self.log,
            ENROLLMENT_STREAM,
            &format!("enroll-issue-{}", issued.serial_hex),
            Event {
                agent_id: None,
                occurred_at: Some(ms_to_timestamp(now_unix_ms)),
                idempotency_key: format!("enroll-issue-{}", issued.serial_hex),
                kind: Some(Kind::IdentityEnrolled(IdentityEnrolled {
                    role: role_to_wire(principal.role()).into(),
                    name: principal.name().to_owned(),
                    certificate_serial: issued.serial_hex.clone(),
                    code_hash: horde_identity::code::hash_secret(secret),
                })),
            },
        )
        .await?;

        Ok((principal, issued))
    }

    /// Revokes every active certificate a principal holds (DESIGN §3:
    /// per-cert denylist by serial; all of them, because the oldest key
    /// still opens every door).
    ///
    /// Returns the serials revoked. Empty is not an error: revoking a
    /// principal with nothing active is a no-op that is already true.
    ///
    /// # Errors
    ///
    /// Log failures.
    pub async fn revoke(
        &self,
        principal: &Principal,
        reason: &str,
        now_unix_ms: i64,
    ) -> Result<Vec<String>, ServerError> {
        let _held = self.gate.lock().await;

        let roster = Self::roster_of(&self.log).await?;
        let serials = roster.active_serials(principal);
        for serial in &serials {
            append_server_event(
                &self.log,
                ENROLLMENT_STREAM,
                &format!("enroll-revoke-{serial}"),
                Event {
                    agent_id: None,
                    occurred_at: Some(ms_to_timestamp(now_unix_ms)),
                    idempotency_key: format!("enroll-revoke-{serial}"),
                    kind: Some(Kind::EnrollmentRevoked(EnrollmentRevoked {
                        role: role_to_wire(principal.role()).into(),
                        name: principal.name().to_owned(),
                        certificate_serial: serial.clone(),
                        reason: reason.to_owned(),
                    })),
                },
            )
            .await?;
            // Only after the event is durably journaled: a door that refuses
            // on an unjournaled revocation would un-refuse at restart.
            self.deny.revoke(serial.clone());
        }
        Ok(serials)
    }

    /// Whether any client holds an active certificate — the bootstrap
    /// question (see `EnrollmentRoster::any_client_enrolled`).
    ///
    /// # Errors
    ///
    /// Reading the enrollment stream failing.
    pub async fn any_client_enrolled(&self) -> Result<bool, ServerError> {
        Ok(Self::roster_of(&self.log).await?.any_client_enrolled())
    }
}

/// Identity-crate failures that reach a caller are internal: they mean the
/// CA or the randomness source is broken, not that the request was wrong.
fn identity_internal(error: horde_identity::IdentityError) -> ServerError {
    ServerError::Internal(error.to_string())
}

/// The current wall clock, for the RPC edges. Core methods take `now`
/// explicitly so every decision stays testable at any instant.
#[must_use]
pub fn wall_clock_ms() -> i64 {
    horde_identity::clock::unix_now_ms()
}

/// The tonic service on the enrollment listener: `Join`, and nothing else.
#[derive(Debug, Clone)]
pub struct Enrollment {
    authority: Arc<EnrollmentAuthority>,
}

impl Enrollment {
    #[must_use]
    pub fn new(authority: Arc<EnrollmentAuthority>) -> Self {
        Self { authority }
    }
}

#[tonic::async_trait]
impl EnrollmentService for Enrollment {
    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let request = request.into_inner();
        if request.hello.is_none() {
            return Err(Status::invalid_argument(
                "a join must introduce itself with a Hello",
            ));
        }

        let (principal, issued) = self
            .authority
            .redeem(&request.enrollment_code, &request.csr_pem, wall_clock_ms())
            .await?;

        let principal_id = match principal.role() {
            PrincipalRole::Worker => {
                join_response::Principal::WorkerId(horde_proto::v1::WorkerId::new(principal.name()))
            }
            PrincipalRole::Client => {
                join_response::Principal::ClientId(horde_proto::v1::ClientId::new(principal.name()))
            }
        };

        Ok(Response::new(JoinResponse {
            certificate_pem: issued.certificate_pem,
            ca_bundle_pem: self.authority.ca_pem().to_owned(),
            principal: Some(principal_id),
        }))
    }
}

#[cfg(test)]
mod tests {
    // The authority's decisions (mint → join → refused re-join, expiry,
    // revocation reaching the deny-list) are exercised over real TLS
    // listeners in `tests/enrollment.rs`, where the same certificates then
    // open — or fail to open — the mTLS control plane. What is pure lives in
    // `crate::enrollment`'s fold tests. This module keeps only what neither
    // covers: the one-string form of a minted code.

    use super::MintedCode;

    /// The authority's Debug reaches logs; it must name itself and hold
    /// everything secret back (it holds the CA's signing key).
    #[tokio::test]
    async fn the_authority_debug_names_itself_and_no_material() {
        let now = super::wall_clock_ms();
        let ca = horde_identity::CertificateAuthority::generate(now).expect("a CA generates");
        let credential = ca.issue_localhost_server(now).expect("issues");
        let authority = super::EnrollmentAuthority::new(
            crate::EventLog::in_memory().expect("a log"),
            ca,
            credential,
        )
        .await
        .expect("the authority builds");

        let debugged = format!("{authority:?}");
        assert!(debugged.contains("EnrollmentAuthority"), "{debugged}");
        assert!(!debugged.contains("PRIVATE KEY"), "{debugged}");
    }

    #[test]
    fn the_join_code_is_secret_dot_fingerprint() {
        let minted = MintedCode {
            secret: "00ff".to_owned(),
            server_fingerprint: "a".repeat(64),
            expires_at_unix_ms: 0,
        };
        assert_eq!(minted.join_code(), format!("00ff.{}", "a".repeat(64)));
    }
}
