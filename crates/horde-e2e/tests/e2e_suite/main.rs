//! Every end-to-end scenario, in one test binary.
//!
//! One binary for the same reason `worker_suite` is one: the runner still
//! parallelizes within it, and anything that ever mutates this crate relinks
//! one target instead of several.
//!
//! Every test here follows the same discipline: real processes, bounded
//! waits (a hang is a worse finding than any assertion), and assertions on
//! what crossed the wire — never on a process's internal state.

#[cfg(test)]
mod support;

#[cfg(test)]
mod forwards;
#[cfg(test)]
mod lifecycle;
#[cfg(test)]
mod logins;
#[cfg(test)]
mod relay_payload;
#[cfg(test)]
mod resilience;
