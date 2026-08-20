//! The gRPC service spines.
//!
//! Both services are thin: they validate, translate, and then read or write the
//! log. No state lives here that is not derived from the log — [`commands`] was
//! the last exception, and is now a fold over it like everything else.
//!
//! This module declares and re-exports; it does not define. What the two spines
//! share lives in [`shared`], so that the submodules can depend on it without
//! depending back on their own parent — a parent that both re-exports a child
//! and defines what that child imports is a module cycle.

pub mod blob;
pub mod commands;
pub mod control_plane;
pub mod enrollment;
pub mod forwards;
pub mod logins;
pub mod shared;
pub mod sync;
pub mod worker;

pub use blob::Blobs;
pub use commands::{CommandProjection, CommandStream};
pub use control_plane::ControlPlane;
pub use enrollment::{Enrollment, EnrollmentAuthority, MintedCode};
pub use shared::{MIN_SUPPORTED_SEMVER, SERVER_SEMVER, ServerCore};
pub use worker::WorkerFacing;
