//! Configuration layering (DESIGN §13, ADR-0016).
//!
//! Each role — server, worker, client — reads one TOML file of its own and
//! layers it between built-in defaults and its flags; the server adds the
//! workspace and session layers for per-agent keys. This crate is the
//! machinery every role shares: the strict reader that refuses unknown keys
//! by path and explains type errors, the origin each effective value came
//! from, the value spellings, and the `logs.*` keys ADR-0015 defined. Each
//! role's own key table lives in that role's crate, next to the types it
//! fills.
//!
//! This module declares and re-exports; it does not define.

pub mod file;
pub mod logs;
pub mod origin;
pub mod provenance;
pub mod reader;
pub mod values;

pub use file::{Document, Role, load};
pub use logs::{AgentLogKeys, AgentReadKeys, LogKeys};
pub use origin::Origin;
pub use provenance::{Entry, Provenance};
pub use reader::Section;
