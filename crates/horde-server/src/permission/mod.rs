//! The permission chain (DESIGN §10).
//!
//! Layered, evaluated in order: deterministic deny, deterministic allow (which
//! remember-for-session widens), the Policy Judge, and finally a human. The
//! chain is server-side policy evaluated per request: [`driver`] follows the
//! log, [`chain::evaluate`] runs the deterministic layers as a pure function
//! over [`rules`] grouped into named [`modes`], and everything the chain
//! concludes is journaled as an ordinary `PermissionDecided` — which is what
//! makes its decisions indistinguishable, to every existing reader, from a
//! human's.
//!
//! This module declares and re-exports; it does not define.

pub mod chain;
pub mod driver;
pub mod modes;
pub mod orchestrator;
pub mod rules;

pub use chain::{ChainRuling, Consultation, JudgeRuling, PolicyJudge, Requester, evaluate};
pub use driver::{ChainDriver, spawn};
pub use modes::{PermissionMode, PermissionModes};
pub use rules::{DenyRule, FieldPrefix, Rule};
