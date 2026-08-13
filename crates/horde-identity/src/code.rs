//! One-time enrollment code primitives.
//!
//! A code is a random secret; the Server records only its hash, an expiry,
//! and the principal it will create. What "pending", "expired" and "used"
//! mean is the Server's enrollment fold's business — this module owns the
//! pieces that must be right in exactly one place: how a secret is minted,
//! how it is hashed, and how its lifetime is clamped.

use sha2::{Digest, Sha256};

use crate::error::IdentityError;

/// How long a code lives when the mint does not say: 10 minutes.
///
/// Codes are carried by a human between two machines; minutes is the honest
/// unit (DESIGN §3: "short-lived").
pub const DEFAULT_CODE_TTL_MS: i64 = 10 * 60 * 1_000;

/// The longest lifetime a mint may ask for: one hour. Server-clamped, per
/// `control_plane.proto`.
pub const MAX_CODE_TTL_MS: i64 = 60 * 60 * 1_000;

/// Mints a fresh one-time secret: 16 random bytes as 32 hex digits.
///
/// # Errors
///
/// [`IdentityError::Randomness`] when the OS randomness source fails —
/// nothing may be minted from a bad source.
pub fn mint_secret() -> Result<String, IdentityError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| IdentityError::Randomness(error.to_string()))?;
    Ok(hex::encode(bytes))
}

/// The hash under which a secret is recorded and later matched.
///
/// The event log keeps this, never the secret: an event log is readable by
/// every attached client, and a readable secret is a minted identity for
/// whoever reads fastest.
#[must_use]
pub fn hash_secret(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

/// Whether a presented secret is the one a hash was minted from.
#[must_use]
pub fn secret_matches(presented: &str, recorded_hash: &str) -> bool {
    hash_secret(presented) == recorded_hash
}

/// Clamps a requested lifetime into the allowed window.
///
/// Zero or negative means "the caller did not say" and gets the default —
/// a request for a non-positive lifetime is an unusable code, and minting an
/// already-expired code would report success while enrolling nobody.
#[must_use]
pub fn clamp_ttl_ms(requested_ms: i64) -> i64 {
    if requested_ms <= 0 {
        DEFAULT_CODE_TTL_MS
    } else {
        requested_ms.min(MAX_CODE_TTL_MS)
    }
}

/// Whether a code is expired at `now`.
///
/// Expiry is inclusive: a code presented in the same millisecond it expires
/// is refused, so the boundary case errs toward refusal.
#[must_use]
pub fn is_expired(expires_at_unix_ms: i64, now_unix_ms: i64) -> bool {
    now_unix_ms >= expires_at_unix_ms
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_CODE_TTL_MS, MAX_CODE_TTL_MS, clamp_ttl_ms, hash_secret, is_expired, mint_secret,
        secret_matches,
    };

    #[test]
    fn minted_secrets_are_hex_and_unique() {
        let a = mint_secret().expect("randomness");
        let b = mint_secret().expect("randomness");
        assert_eq!(a.len(), 32);
        assert!(a.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(a, b, "two mints must not collide");
    }

    #[test]
    fn the_hash_is_stable_and_is_not_the_secret() {
        let secret = "00112233445566778899aabbccddeeff";
        let hash = hash_secret(secret);
        assert_eq!(hash, hash_secret(secret), "same secret, same hash");
        assert_ne!(hash, secret);
        assert_eq!(hash.len(), 64);
        assert!(secret_matches(secret, &hash));
        assert!(!secret_matches("not-the-secret", &hash));
    }

    #[test]
    fn ttl_clamping_defaults_the_unspoken_and_caps_the_greedy() {
        assert_eq!(clamp_ttl_ms(0), DEFAULT_CODE_TTL_MS, "unset means default");
        assert_eq!(
            clamp_ttl_ms(-5),
            DEFAULT_CODE_TTL_MS,
            "nonsense means default"
        );
        assert_eq!(clamp_ttl_ms(1), 1, "a short explicit ttl is honored");
        assert_eq!(clamp_ttl_ms(MAX_CODE_TTL_MS), MAX_CODE_TTL_MS);
        assert_eq!(
            clamp_ttl_ms(MAX_CODE_TTL_MS + 1),
            MAX_CODE_TTL_MS,
            "nothing outlives the cap"
        );
    }

    #[test]
    fn expiry_is_inclusive_at_the_boundary() {
        assert!(!is_expired(1_000, 999), "before the deadline is alive");
        assert!(is_expired(1_000, 1_000), "the deadline itself is expired");
        assert!(is_expired(1_000, 1_001), "after the deadline is expired");
    }
}
