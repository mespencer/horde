//! Every multi-node scenario on the fake adapter, in one test binary.
//!
//! Each scenario brings up its own compose project (docker/multinode): a
//! server, three workers, two clients and a git origin, each a container with
//! its own hostname and address. Trust is bootstrapped through the real flows
//! and every connection is mTLS over the bridge network. Assertions read the
//! server's event log through the `horde` CLI's JSON output, run inside a
//! client container — never a process's internal state.
//!
//! Every test is `#[ignore]`d because it needs Docker; run them with
//! `cargo nextest run -p horde-multinode --run-ignored only`. A missing
//! daemon fails them rather than skipping them.

#[cfg(test)]
mod common;

#[cfg(test)]
mod attach;
#[cfg(test)]
mod liveness;
#[cfg(test)]
mod partition;
#[cfg(test)]
mod placement;
#[cfg(test)]
mod relay;
