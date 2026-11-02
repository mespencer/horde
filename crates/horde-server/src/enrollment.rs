//! Enrollment state: a fold over the reserved enrollment stream.
//!
//! ADR-0001 admits no store for server state but the event log, and DESIGN §3
//! makes enrollment server state — so codes, issued identities, and
//! revocations are events in a reserved stream (like the Workspace Definition
//! catalog), and everything the Server needs to *decide* — is this code
//! redeemable, which serials does this principal hold, what is revoked — is
//! this pure fold over them. The side-effecting half (signing, appending)
//! lives in `service::enrollment`, exactly as the catalog's lives in the
//! control plane.

use std::collections::{BTreeMap, BTreeSet};

use horde_identity::{Principal, PrincipalRole};
use horde_proto::v1::{Event, Role, event::Kind};
use prost::Message;

use crate::error::ServerError;
use crate::log::StoredEvent;

/// The reserved stream enrollment lives in.
///
/// Server-assigned session ids are `session-…`, so nothing user-created can
/// collide with it.
pub const ENROLLMENT_STREAM: &str = "horde-enrollment";

/// Whether a log stream is the enrollment stream rather than a session.
#[must_use]
pub fn is_enrollment_stream(session_id: &str) -> bool {
    session_id == ENROLLMENT_STREAM
}

/// Whether a log stream is reserved — not a session at all.
///
/// The one predicate every "for each session" loop filters with. Session
/// listing, agent lookup, and placement each fold `session_ids()`, and a
/// reserved stream leaking into any of them becomes a phantom session in a
/// UI; two spellings of this rule is how that happens to exactly one of
/// them.
#[must_use]
pub fn is_reserved_stream(session_id: &str) -> bool {
    crate::workspace::is_catalog_stream(session_id)
        || is_enrollment_stream(session_id)
        || crate::mcp::is_mcp_catalog_stream(session_id)
        || crate::login::is_login_stream(session_id)
        || crate::service::ingest::is_dead_letter_stream(session_id)
        || crate::liveness::is_worker_journal_stream(session_id)
}

/// Whether a stream is a session — any stream that is not reserved. The
/// admission the session-wide projection walks use (ADR-0013).
#[must_use]
pub fn is_session_stream(session_id: &str) -> bool {
    !is_reserved_stream(session_id)
}

/// The wire form of a principal role.
#[must_use]
pub fn role_to_wire(role: PrincipalRole) -> Role {
    match role {
        PrincipalRole::Worker => Role::Worker,
        PrincipalRole::Client => Role::Client,
    }
}

/// The domain form of a wire role. `None` for `ROLE_UNSPECIFIED` and
/// anything newer than this build: an identity must never be minted under a
/// role this build cannot name.
#[must_use]
pub fn role_from_wire(role: i32) -> Option<PrincipalRole> {
    match Role::try_from(role) {
        Ok(Role::Worker) => Some(PrincipalRole::Worker),
        Ok(Role::Client) => Some(PrincipalRole::Client),
        Ok(Role::Unspecified) | Err(_) => None,
    }
}

/// A `google.protobuf.Timestamp` as unix milliseconds.
#[must_use]
pub fn timestamp_to_ms(timestamp: &prost_types::Timestamp) -> i64 {
    timestamp
        .seconds
        .saturating_mul(1_000)
        .saturating_add(i64::from(timestamp.nanos) / 1_000_000)
}

/// Unix milliseconds as a `google.protobuf.Timestamp`.
#[must_use]
pub fn ms_to_timestamp(unix_ms: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: unix_ms.div_euclid(1_000),
        nanos: i32::try_from(unix_ms.rem_euclid(1_000)).unwrap_or(0) * 1_000_000,
    }
}

/// A `google.protobuf.Duration` as milliseconds.
#[must_use]
pub fn duration_to_ms(duration: &prost_types::Duration) -> i64 {
    duration
        .seconds
        .saturating_mul(1_000)
        .saturating_add(i64::from(duration.nanos) / 1_000_000)
}

/// One minted code, as the fold sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeRecord {
    pub role: PrincipalRole,
    pub name: String,
    pub expires_at_unix_ms: i64,
    /// A redeemed code is spent; one code, one identity.
    pub spent: bool,
}

/// Why a presented code cannot be redeemed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedeemRefusal {
    /// No code with this secret was ever minted.
    Unknown,
    /// The code existed but its lifetime has passed.
    Expired,
    /// The code was already redeemed once.
    Spent,
}

/// The enrollment roster, folded from the enrollment stream.
///
/// Pure: same events in, same roster out, so a Server restart reconstructs
/// exactly the enrollment state it shut down with.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnrollmentRoster {
    /// Minted codes by the hash of their secret.
    codes: BTreeMap<String, CodeRecord>,
    /// Serials issued per principal common name, in issuance order.
    serials: BTreeMap<String, Vec<String>>,
    /// Every revoked serial.
    revoked: BTreeSet<String>,
}

impl EnrollmentRoster {
    /// Folds the enrollment stream into a roster.
    ///
    /// Events that are not enrollment kinds, or that carry an unusable role
    /// or name, are ignored rather than fatal — one malformed event must not
    /// take down every enrollment operation. Nothing writes such events, so
    /// ignoring them loses nothing that was ever valid.
    pub fn project(events: &[StoredEvent]) -> Result<Self, ServerError> {
        let mut roster = Self::default();
        for stored in events {
            roster.apply(stored)?;
        }
        Ok(roster)
    }

    fn apply(&mut self, stored: &StoredEvent) -> Result<(), ServerError> {
        let event = Event::decode(stored.payload.as_slice())?;
        match event.kind {
            Some(Kind::EnrollmentCodeMinted(minted)) => {
                let Some(role) = role_from_wire(minted.role) else {
                    return Ok(());
                };
                self.codes.insert(
                    minted.code_hash,
                    CodeRecord {
                        role,
                        name: minted.name,
                        expires_at_unix_ms: minted
                            .expires_at
                            .as_ref()
                            .map(timestamp_to_ms)
                            .unwrap_or_default(),
                        spent: false,
                    },
                );
            }
            Some(Kind::IdentityEnrolled(enrolled)) => {
                if let Some(code) = self.codes.get_mut(&enrolled.code_hash) {
                    code.spent = true;
                }
                let Some(role) = role_from_wire(enrolled.role) else {
                    return Ok(());
                };
                let Ok(principal) = Principal::new(role, enrolled.name) else {
                    return Ok(());
                };
                self.serials
                    .entry(principal.common_name())
                    .or_default()
                    .push(enrolled.certificate_serial);
            }
            Some(Kind::EnrollmentRevoked(revoked)) => {
                self.revoked.insert(revoked.certificate_serial);
            }
            _ => {}
        }
        Ok(())
    }

    /// Decides whether a presented secret redeems, and for whom.
    ///
    /// The refusal order is deliberate: a spent code reports [`Spent`] even
    /// after it also expired, because "someone already used this" and "you
    /// were too slow" call for different reactions — the first is a possible
    /// compromise, the second a new code.
    ///
    /// [`Spent`]: RedeemRefusal::Spent
    ///
    /// # Errors
    ///
    /// The [`RedeemRefusal`] naming why not.
    pub fn redeem_decision(
        &self,
        secret: &str,
        now_unix_ms: i64,
    ) -> Result<&CodeRecord, RedeemRefusal> {
        let code = self
            .codes
            .get(&horde_identity::code::hash_secret(secret))
            .ok_or(RedeemRefusal::Unknown)?;
        if code.spent {
            return Err(RedeemRefusal::Spent);
        }
        if horde_identity::code::is_expired(code.expires_at_unix_ms, now_unix_ms) {
            return Err(RedeemRefusal::Expired);
        }
        Ok(code)
    }

    /// The serials a principal currently holds: issued and not revoked.
    ///
    /// Every serial ever issued to the name, because re-enrollment issues a
    /// new certificate without invalidating the old one — revoking a
    /// principal must revoke all of them or the oldest key still opens every
    /// door.
    #[must_use]
    pub fn active_serials(&self, principal: &Principal) -> Vec<String> {
        self.serials
            .get(&principal.common_name())
            .map(|serials| {
                serials
                    .iter()
                    .filter(|serial| !self.revoked.contains(*serial))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every revoked serial, for seeding the deny-list at startup.
    pub fn revoked_serials(&self) -> impl Iterator<Item = &str> {
        self.revoked.iter().map(String::as_str)
    }

    /// Whether any client principal holds an active certificate.
    ///
    /// The bootstrap question (DESIGN §3 gap, decided here): the first
    /// startup has no enrolled client to mint codes over the control plane,
    /// so the Server mints one locally exactly when this is false.
    #[must_use]
    pub fn any_client_enrolled(&self) -> bool {
        self.serials
            .iter()
            .filter(|(common_name, _)| common_name.starts_with("client:"))
            .any(|(common_name, _)| {
                Principal::parse_common_name(common_name)
                    .is_some_and(|principal| !self.active_serials(&principal).is_empty())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EnrollmentRoster, RedeemRefusal, is_enrollment_stream, ms_to_timestamp, role_from_wire,
        role_to_wire, timestamp_to_ms,
    };
    use crate::log::StoredEvent;
    use horde_identity::code::hash_secret;
    use horde_identity::{Principal, PrincipalRole};
    use horde_proto::v1::{
        EnrollmentCodeMinted, EnrollmentRevoked, Event, IdentityEnrolled, Role, event::Kind,
    };
    use prost::Message;

    fn stored(sequence: u64, kind: Kind) -> StoredEvent {
        StoredEvent {
            session_id: super::ENROLLMENT_STREAM.to_owned(),
            sequence,
            recorded_at_unix_ms: 0,
            payload: Event {
                agent_id: None,
                occurred_at: None,
                idempotency_key: format!("key-{sequence}"),
                kind: Some(kind),
            }
            .encode_to_vec(),
        }
    }

    fn minted(secret: &str, role: Role, name: &str, expires_at_ms: i64) -> Kind {
        Kind::EnrollmentCodeMinted(EnrollmentCodeMinted {
            role: role.into(),
            name: name.to_owned(),
            code_hash: hash_secret(secret),
            expires_at: Some(ms_to_timestamp(expires_at_ms)),
        })
    }

    fn enrolled(secret: &str, role: Role, name: &str, serial: &str) -> Kind {
        Kind::IdentityEnrolled(IdentityEnrolled {
            role: role.into(),
            name: name.to_owned(),
            certificate_serial: serial.to_owned(),
            code_hash: hash_secret(secret),
        })
    }

    fn revoked(serial: &str) -> Kind {
        Kind::EnrollmentRevoked(EnrollmentRevoked {
            role: Role::Worker.into(),
            name: "mac-studio".to_owned(),
            certificate_serial: serial.to_owned(),
            reason: "test".to_owned(),
        })
    }

    fn worker(name: &str) -> Principal {
        Principal::new(PrincipalRole::Worker, name).expect("a valid name")
    }

    #[test]
    fn only_the_enrollment_stream_is_the_enrollment_stream() {
        assert!(is_enrollment_stream(super::ENROLLMENT_STREAM));
        assert!(!is_enrollment_stream("session-1"));
        assert!(!is_enrollment_stream(""));
    }

    #[test]
    fn roles_round_trip_and_the_unspeakable_are_refused() {
        for role in [PrincipalRole::Worker, PrincipalRole::Client] {
            assert_eq!(role_from_wire(role_to_wire(role) as i32), Some(role));
        }
        assert_eq!(role_from_wire(Role::Unspecified as i32), None);
        assert_eq!(
            role_from_wire(999),
            None,
            "future roles must not be guessed at"
        );
    }

    #[test]
    fn timestamps_round_trip_through_milliseconds() {
        for ms in [0_i64, 1, 999, 1_000, 1_754_956_800_123, -1] {
            assert_eq!(timestamp_to_ms(&ms_to_timestamp(ms)), ms);
        }
    }

    /// Wire durations to milliseconds, exactly — a mint ttl misread here
    /// mints codes with the wrong lifetime.
    #[test]
    fn durations_read_as_the_milliseconds_they_are() {
        let of = |seconds, nanos| super::duration_to_ms(&prost_types::Duration { seconds, nanos });
        assert_eq!(of(0, 0), 0);
        assert_eq!(of(300, 0), 300_000, "five minutes");
        assert_eq!(of(1, 500_000_000), 1_500, "nanos contribute their millis");
        assert_eq!(of(0, 999_999), 0, "sub-millisecond nanos truncate");
        assert_eq!(of(i64::MAX, 0), i64::MAX, "saturates rather than wraps");
    }

    #[test]
    fn a_minted_code_redeems_exactly_once_within_its_lifetime() {
        let roster = EnrollmentRoster::project(&[stored(
            1,
            minted("s3cr3t", Role::Worker, "mac-studio", 10_000),
        )])
        .expect("the fold succeeds");

        let code = roster
            .redeem_decision("s3cr3t", 9_999)
            .expect("an unexpired, unspent code redeems");
        assert_eq!(code.role, PrincipalRole::Worker);
        assert_eq!(code.name, "mac-studio");

        assert_eq!(
            roster.redeem_decision("wrong", 9_999),
            Err(RedeemRefusal::Unknown),
            "an unminted secret must never redeem"
        );
        assert_eq!(
            roster.redeem_decision("s3cr3t", 10_000),
            Err(RedeemRefusal::Expired),
            "expiry is inclusive at the boundary"
        );
    }

    #[test]
    fn redemption_spends_the_code_and_spent_outranks_expired() {
        let roster = EnrollmentRoster::project(&[
            stored(1, minted("s3cr3t", Role::Worker, "mac-studio", 10_000)),
            stored(2, enrolled("s3cr3t", Role::Worker, "mac-studio", "0a")),
        ])
        .expect("the fold succeeds");

        assert_eq!(
            roster.redeem_decision("s3cr3t", 5_000),
            Err(RedeemRefusal::Spent),
            "one code, one identity"
        );
        assert_eq!(
            roster.redeem_decision("s3cr3t", 20_000),
            Err(RedeemRefusal::Spent),
            "a spent code reports Spent even after it also expired — \
             'someone used this' and 'too slow' need different reactions"
        );
    }

    #[test]
    fn a_principal_accumulates_serials_and_revocation_subtracts_them() {
        let roster = EnrollmentRoster::project(&[
            stored(1, minted("one", Role::Worker, "mac-studio", 10_000)),
            stored(2, enrolled("one", Role::Worker, "mac-studio", "0a")),
            stored(3, minted("two", Role::Worker, "mac-studio", 10_000)),
            stored(4, enrolled("two", Role::Worker, "mac-studio", "0b")),
            stored(5, revoked("0a")),
        ])
        .expect("the fold succeeds");

        assert_eq!(
            roster.active_serials(&worker("mac-studio")),
            vec!["0b".to_owned()],
            "re-enrollment accumulates serials; revocation removes exactly one"
        );
        assert_eq!(
            roster.active_serials(&worker("elsewhere")),
            Vec::<String>::new()
        );
        assert_eq!(roster.revoked_serials().collect::<Vec<_>>(), vec!["0a"]);
    }

    #[test]
    fn the_bootstrap_question_answers_over_live_clients_only() {
        let nobody = EnrollmentRoster::default();
        assert!(
            !nobody.any_client_enrolled(),
            "a fresh server has no clients"
        );

        let only_workers = EnrollmentRoster::project(&[
            stored(1, minted("w", Role::Worker, "mac-studio", 10_000)),
            stored(2, enrolled("w", Role::Worker, "mac-studio", "0a")),
        ])
        .expect("the fold succeeds");
        assert!(
            !only_workers.any_client_enrolled(),
            "workers cannot mint codes; only a client counts"
        );

        let with_client = EnrollmentRoster::project(&[
            stored(1, minted("c", Role::Client, "laptop", 10_000)),
            stored(2, enrolled("c", Role::Client, "laptop", "0c")),
        ])
        .expect("the fold succeeds");
        assert!(with_client.any_client_enrolled());

        let client_revoked = EnrollmentRoster::project(&[
            stored(1, minted("c", Role::Client, "laptop", 10_000)),
            stored(2, enrolled("c", Role::Client, "laptop", "0c")),
            stored(3, revoked("0c")),
        ])
        .expect("the fold succeeds");
        assert!(
            !client_revoked.any_client_enrolled(),
            "a revoked client is not a way back in, so bootstrap reopens"
        );
    }

    #[test]
    fn unusable_events_are_ignored_rather_than_fatal() {
        let roster = EnrollmentRoster::project(&[
            stored(1, minted("s", Role::Unspecified, "mac-studio", 10_000)),
            stored(2, enrolled("s", Role::Worker, "not a name", "0a")),
            // A non-enrollment kind in the stream folds to nothing.
            stored(
                3,
                Kind::TurnInterrupted(horde_proto::v1::TurnInterrupted::default()),
            ),
        ])
        .expect("the fold must not die on a malformed event");

        assert_eq!(
            roster.redeem_decision("s", 0),
            Err(RedeemRefusal::Unknown),
            "an unspeakable role must not become a mintable code"
        );
        assert_eq!(
            roster.active_serials(&worker("mac-studio")),
            Vec::<String>::new()
        );
    }
}
