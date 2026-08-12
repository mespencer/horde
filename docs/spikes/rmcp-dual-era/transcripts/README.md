# rmcp dual-era spike — wire transcripts

> ### ⚠ This directory is empty — the transcripts were not preserved
>
> This README is the only file here. The captures it describes never reached
> disk: they were lost to a session-tooling incident, not deleted deliberately.
>
> The descriptions below are kept, and so are the references to them in
> `docs/spikes/rmcp-dual-era.md`, because they record **which run produced which
> finding** — the provenance is still useful even though the bytes are gone. What
> survives of the wire is the set of excerpts quoted inline in that document.
>
> **To regenerate:** re-run the probes in `spikes/rmcp-dual-era/` per the
> *Reproducing* section below, against `claude` 2.1.228 and `codex-cli` 0.147.0,
> capturing to this directory under the same filenames. Tracked as backlog in
> `docs/spikes/rmcp-dual-era.md` §9; not done.

Each transcript was a byte-for-byte record of TCP traffic between a real MCP
client and one pinned **rmcp 3.1.2** streamable-HTTP endpoint, captured by the
dumb TCP tee in `spikes/rmcp-dual-era/src/tee.rs`. The tee parses no HTTP, so
nothing was reformatted, re-chunked, or summarised — chunk boundaries are real
TCP reads, and the hex lines inside SSE bodies are HTTP chunked-transfer framing.
The rest of this README describes that format, and stands as the specification a
re-capture should meet.

Captured on **2026-08-11** against `claude` 2.1.228 and `codex-cli` 0.147.0.

## Reading a transcript

```
=== +8.103s conn 0 opened from 127.0.0.1:53643 ===   framing note, offset from server start
--- conn 0 C->S +8.113s 628 bytes ---                one TCP read, client -> server
--- conn 0 S->C +8.125s 472 bytes ---                one TCP read, server -> client
```

Connections interleave. Follow one `conn N` at a time.

Each `*.log` has a sibling `*.server.log`: the spike server's own stderr, which
records the resolved rmcp protocol constants, the effective
`StreamableHttpServerConfig`, and a `SERVER-SAW` block per inbound HTTP request
listing every header that survived into the tower service. The tee proves a
header reached the socket; `SERVER-SAW` proves it reached rmcp.

## The transcripts

*None of these files is on disk. The table is the capture manifest — what a
re-run should produce, and what each `NN` reference in the spike document means.*

| File | Client | Server config | What it shows |
|---|---|---|---|
| `01-claude-code-stateless.log` | Claude Code 2.1.228 | rmcp defaults (`legacy_session_mode=false`) | Full legacy round-trip at `2025-11-25`, **but `GET /mcp` is answered `405`** and the handshake `clientInfo` is discarded |
| `02-claude-code-legacy-session.log` | Claude Code 2.1.228 | `legacy_session_mode=true` | Same round-trip with `Mcp-Session-Id` minted, `GET /mcp` answered `200` + SSE priming, `clientInfo` preserved |
| `03-codex-legacy-session.log` | codex-cli 0.147.0 (app-server) | `legacy_session_mode=true` | Legacy round-trip at `2025-06-18`, incl. `tools/call` and the closing `DELETE /mcp` |
| `04-modern-stateless.log` | hand-rolled probe (`probe_modern.sh`) | `legacy_session_mode=true` | The `2026-07-28` stateless flow — `server/discover`, `_meta`-carried version, SEP-2243 routing headers — plus negative probes and a legacy `initialize` on the same instance |
| `05-dual-era-one-instance.log` | Claude Code, then codex, then modern probe | `legacy_session_mode=true` | Three clients at three revisions against a **single** server process |
| `06-codex-mcp-2026-flag.log` | codex-cli 0.147.0 with `--enable mcp_2026_07_28` | `legacy_session_mode=true` | codex driving the endpoint as a genuine **modern-era** client: `server/discover`, no `initialize` |

## Reproducing

The server, the tee, and the tools are one binary:

```
cd spikes/rmcp-dual-era
cargo run -- --transcript /tmp/t.log --listen 127.0.0.1:8931 [--legacy-session-mode]
```

Then drive it with `spikes/rmcp-dual-era/probe_modern.sh` (modern era),
`spikes/rmcp-dual-era/drive_codex.py` (codex via `app-server`), or `claude`
with the `--mcp-config` shown in `docs/spikes/rmcp-dual-era.md`.
