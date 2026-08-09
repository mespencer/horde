# Git as the workspace backbone, with a server-side relay

Workspaces are made of git repos, the unit of cross-agent sharing is the commit, and isolation is worktrees/clones. Cross-worker transport is a server-hosted bare-mirror relay (git smart-HTTP behind the existing authenticated channel) that doubles as clone cache and, eventually, the sole holder of external git credentials.

## Considered options

- **File-level sync (rsync/mutagen-style)** — rejected: reinvents VCS hard parts (conflicts, atomicity, provenance), loses attributable commits, and abandons the one workflow every agent arrives already trained on.
- **Git bundles over gRPC** — rejected entirely (not even as fallback): point-to-point, no cache, incremental basis tracking; the relay does everything bundles do with less code once it exists.
- **Push through real remotes (GitHub)** — rejected: temp-branch litter, external credentials on every worker, rate/hammering concerns.

## Consequences

- Syncing dirty working trees across workers is permanently out of scope: commit, then share. Tools enforce this with helpful errors.
- Non-git data lives in `unmanaged` workspace paths (never synced) or travels as blob-store attachments — not in the commit graph.
