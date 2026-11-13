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

pub mod blob;
pub mod config;
pub mod enrollment;
pub mod error;
pub mod forward;
pub mod hub;
pub mod lineage;
pub mod liveness;
pub mod log;
pub mod login;
pub mod mcp;
pub mod permission;
pub mod placement;
pub mod projection;
pub mod relay;
pub mod remint;
pub mod scheduler;
pub mod service;
pub mod startup;
pub mod state;
pub mod tiers;
pub mod transport;
pub mod trust;
pub mod usage;
mod windows;
pub mod workspace;
pub mod workspace_policy;

pub use blob::{BlobError, BlobStore};
pub use enrollment::EnrollmentRoster;
pub use error::ServerError;
pub use log::EventLog;
pub use scheduler::{CapabilityScheduler, Placement, Scheduler, WorkerRegistry};
pub use service::ServerCore;
pub use state::{AgentState, SessionState};
pub use workspace::{InstanceCache, WorkspaceCatalog};
