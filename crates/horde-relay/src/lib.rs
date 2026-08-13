//! The git backbone: the Relay and share resolution (ADR-0002, DESIGN §8).
//!
//! Workspaces are made of git repos, the unit of cross-agent sharing is the
//! commit, and syncing dirty working trees is permanently out of scope —
//! "commit, then share". This crate is both halves of how a committed ref
//! actually moves:
//!
//! * **The Relay** ([`mirror`], [`http`]): the Server's bare mirrors, one per
//!   registered repo, lazily created on first use and served over git
//!   smart-HTTP via `git http-backend`. It is the cross-Worker transport, the
//!   clone cache, and — end state — the sole holder of external git
//!   credentials.
//! * **Share resolution** ([`resolve`], [`transport`]): given where the sender
//!   and receiver checkouts live, decide how a commit travels — same
//!   clone/worktrees need a ref checkout only, same Worker fetches from the
//!   peer's path, cross-Worker goes push/fetch through the Relay — and then
//!   execute that route with real git. Transport is never the Agent's concern
//!   (DESIGN §5): a Payload names commits, and this crate is what makes them
//!   appear.
//!
//! # The seam is the crate boundary
//!
//! Every real git operation lives here, behind functions the rest of Horde
//! calls with paths and commit ids. The tests exercise them against real
//! repositories in temp directories — clones, worktrees, a real `git` client
//! pushing through a real served Relay — because the failure modes worth
//! testing (pack negotiation, ref advertisement, worktree object sharing) only
//! exist in real git.

pub mod git;
pub mod http;
pub mod mirror;
pub mod resolve;
pub mod transport;

pub use git::GitError;
pub use http::RelayServer;
pub use mirror::{MirrorError, MirrorStore};
pub use resolve::{CheckoutView, ShareRoute, resolve_route};
pub use transport::{
    ObservedHead, RelayLocation, ShareError, observe_head, obtain_share, require_committed,
    share_ref, stage_share,
};
