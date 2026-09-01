//! Identity and mTLS for every Horde channel (DESIGN.md §3).
//!
//! The Server is remote-code-execution-as-a-service, so the trust boundary is
//! real: every principal — Workers *and* Clients — authenticates with a
//! certificate issued by the Server's internal CA, over an enrollment flow in
//! which private keys never travel. This crate owns all of it:
//!
//! * the CA itself ([`ca`]): generation, persistence, and the two things it
//!   signs — the Server's own listener certificate and enrolled principals'
//!   certificates, from CSRs;
//! * the one-time enrollment code primitives ([`code`]) and the joining side
//!   of the flow ([`enroll`]), shared by the Worker and the CLI so both redeem
//!   codes through the same path;
//! * what a certificate *means* ([`principal`]): the principal encoding in the
//!   subject, and its extraction from a peer certificate at the door;
//! * the deny-list revocation state ([`revocation`]) and the tonic
//!   interceptor ([`interceptor`]) that turns "presented a certificate" into
//!   "is an enrolled, unrevoked principal" on every RPC;
//! * every rustls configuration Horde uses ([`tls`]): the tonic server and
//!   channel configs, the acceptor the hyper-served channels (Relay, MCP
//!   endpoint) terminate with, and the fingerprint-pinned verifier the join
//!   flow trusts before it trusts the CA.
//!
//! One crate rather than per-channel copies, because "what a Horde listener
//! accepts" must have exactly one owner: four private copies of a TLS
//! configuration drift, and a drifted one fails open.

pub mod ca;
pub mod clock;
pub mod code;
pub mod enroll;
pub mod error;
pub mod fingerprint;
pub mod interceptor;
pub mod principal;
pub mod revocation;
pub mod store;
pub mod tls;
pub mod token;

pub use ca::{CertificateAuthority, IssuedCertificate, ServerCredential};
pub use enroll::{EnrolledIdentity, JoinError, join};
pub use error::IdentityError;
pub use fingerprint::{JoinCode, certificate_fingerprint, fingerprint_of_pem};
pub use interceptor::require_enrolled;
pub use principal::{PeerIdentity, Principal, PrincipalRole, principal_of_pem};
pub use revocation::RevocationList;
pub use store::StoredIdentity;
pub use token::{JoinToken, TokenError};
