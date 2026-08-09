//! The Horde control plane.
//!
//! One append-only event log per session is the source of truth (ADR-0001);
//! everything else here is either a projection of it or a service that reads
//! and writes it. Reconnect, attach, scrollback, crash recovery, and usage
//! attribution are consequences of that single mechanism rather than five
//! separate features.
//!
//! This crate is a library with a thin binary on top: the binary owns runtime
//! and subscriber policy, and nothing in the library installs a global
//! subscriber or decides how logs are formatted.

pub mod error;
pub mod log;

pub use error::ServerError;
pub use log::EventLog;
