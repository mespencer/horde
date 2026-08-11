//! What the binary does, in a form that can be tested.
//!
//! `bin/horde_server.rs` is left as subscriber setup and an exit code. Anything
//! with a decision in it lives here, because a binary's functions are reachable
//! only by running the binary — which coverage cannot see and mutation testing
//! therefore reports as untested however carefully it was written.

use crate::EventLog;
use crate::error::ServerError;

/// Where the log lives when the operator does not say.
pub const DEFAULT_DATABASE: &str = "horde.db";

/// Chooses the database path from the command line.
///
/// Separated from opening it so the choice is testable without touching a
/// filesystem: an empty argument is not a path, and treating it as one would
/// create a file named after nothing.
#[must_use]
pub fn database_path(arguments: impl IntoIterator<Item = String>) -> String {
    arguments
        .into_iter()
        .find(|argument| !argument.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_DATABASE.to_owned())
}

/// Opens the log the server will serve from.
///
/// DESIGN.md §3 forbids binding beyond localhost before auth exists, so the
/// networked entrypoint lands with enrollment in wave 5. Until then starting up
/// means proving the log opens, which is a real check: a corrupt or
/// unwritable database fails here rather than on the first append.
pub fn open_log(path: &str) -> Result<EventLog, ServerError> {
    let log = EventLog::open(path)?;
    tracing::info!(%path, "event log opened");
    Ok(log)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_DATABASE, database_path, open_log};

    #[test]
    fn the_first_argument_names_the_database() {
        assert_eq!(
            database_path(vec!["/tmp/horde-test.db".to_owned()]),
            "/tmp/horde-test.db"
        );
    }

    #[test]
    fn no_argument_falls_back_to_the_default() {
        assert_eq!(database_path(Vec::new()), DEFAULT_DATABASE);
    }

    /// An empty argument is not a path. Taking it literally would create a file
    /// named after nothing, which is worse than falling back.
    #[test]
    fn a_blank_argument_falls_back_rather_than_naming_a_file_after_nothing() {
        assert_eq!(database_path(vec![String::new()]), DEFAULT_DATABASE);
        assert_eq!(database_path(vec!["   ".to_owned()]), DEFAULT_DATABASE);
    }

    #[test]
    fn opening_creates_a_usable_log() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let path = directory.path().join("horde.db");
        let path = path.to_string_lossy().into_owned();

        let log = open_log(&path).expect("opening must succeed");
        assert!(
            std::path::Path::new(&path).exists(),
            "opening must create the database"
        );
        drop(log);

        // Re-opening an existing database is the same code path as creating one.
        open_log(&path).expect("re-opening must succeed");
    }

    /// A path that cannot be opened must fail at startup rather than on the
    /// first append, when an agent is already mid-turn.
    #[test]
    fn an_unopenable_path_fails_at_startup() {
        let directory = tempfile::tempdir().expect("a temp dir");
        // A directory is not a database file.
        let path = directory.path().to_string_lossy().into_owned();

        open_log(&path).expect_err("a directory is not a database");
    }
}
