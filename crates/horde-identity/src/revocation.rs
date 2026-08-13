//! The per-certificate deny-list (DESIGN §3: revocation by serial).
//!
//! A shared set of revoked serials, cheap to consult on every request. The
//! *record* of revocation is an event in the log like everything else; this
//! is the folded, in-memory form the channel doors check against. Sharing is
//! by clone: every listener holds the same list, so one `revoke` is
//! immediately visible at every door.

use std::collections::HashSet;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// The set of certificate serials no longer accepted anywhere.
#[derive(Debug, Clone, Default)]
pub struct RevocationList {
    revoked: Arc<RwLock<HashSet<String>>>,
}

impl RevocationList {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks one serial revoked. Idempotent, like the event it folds.
    pub fn revoke(&self, serial_hex: impl Into<String>) {
        write_or_recover(&self.revoked).insert(serial_hex.into());
    }

    /// Whether a presented certificate's serial has been revoked.
    #[must_use]
    pub fn is_revoked(&self, serial_hex: &str) -> bool {
        read_or_recover(&self.revoked).contains(serial_hex)
    }
}

/// Locks for reading, absorbing poison.
///
/// A poisoned lock means a panic elsewhere, and this set must keep refusing
/// revoked serials even then — failing open at the door because an unrelated
/// thread died is the worst available behavior. The set itself is always
/// internally consistent (single inserts), so the recovered view is sound.
fn read_or_recover<'a>(lock: &'a RwLock<HashSet<String>>) -> RwLockReadGuard<'a, HashSet<String>> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Locks for writing, absorbing poison for the same reason.
fn write_or_recover<'a>(
    lock: &'a RwLock<HashSet<String>>,
) -> RwLockWriteGuard<'a, HashSet<String>> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::RevocationList;

    #[test]
    fn a_serial_is_only_revoked_once_someone_revokes_it() {
        let list = RevocationList::new();
        assert!(!list.is_revoked("0a"), "fresh lists refuse nobody");
        list.revoke("0a");
        assert!(list.is_revoked("0a"));
        assert!(
            !list.is_revoked("0b"),
            "revocation is per-serial, not global"
        );
    }

    #[test]
    fn clones_share_one_set() {
        let list = RevocationList::new();
        let door = list.clone();
        list.revoke("0a");
        assert!(
            door.is_revoked("0a"),
            "every door must see a revocation immediately"
        );
    }
}
