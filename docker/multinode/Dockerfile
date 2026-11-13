# syntax=docker/dockerfile:1.12
#
# Horde's three product binaries in one slim runtime image, for the
# multi-node harness (docs/TESTING.md, "Multi-node over Docker"). Build from
# the workspace root:
#
#   docker build -f docker/multinode/Dockerfile -t horde-multinode:local .
#
# Base images are pinned by digest; bump the tag and digest together
# (`docker buildx imagetools inspect <tag>` prints the index digest).

ARG RUST_IMAGE=rust:1.95.0-slim-trixie@sha256:e14e87345b4d5964ddcc3491d27ee046a0f23820f340c3c1e24da6880141f7c0
ARG RUNTIME_IMAGE=debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

FROM ${RUST_IMAGE} AS build
WORKDIR /src
# rust-toolchain.toml pins 1.95.0 plus the gauntlet's components (clippy,
# llvm-tools); the image already carries 1.95.0, and naming it here keeps
# rustup from downloading components a release build never runs.
ENV RUSTUP_TOOLCHAIN=1.95.0 \
    CARGO_TERM_COLOR=never
COPY . .
# The registry, git checkouts and target directory live in BuildKit cache
# mounts: a source edit recompiles only what changed, and nothing from them
# lands in a layer. The binaries are copied out because the target cache is
# not part of the image.
RUN --mount=type=cache,id=horde-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=horde-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=horde-multinode-target,target=/src/target,sharing=locked \
    cargo build --locked --release -p horde-server -p horde-worker -p horde-cli \
    && install -D -m 0755 -t /out \
        target/release/horde-server target/release/horde-worker target/release/horde

FROM ${RUNTIME_IMAGE} AS runtime
# git: workspaces, the Relay's `git http-backend`, the origin's `git daemon`.
# procps: the worker fingerprints harness process-group leaders with `ps`.
# ca-certificates: real harnesses (the opt-in profile) dial their providers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git procps \
    && rm -rf /var/lib/apt/lists/* \
    && git config --system init.defaultBranch main \
    && git config --system user.name "Horde multinode" \
    && git config --system user.email "multinode@horde.invalid" \
    && groupadd --gid 10001 horde \
    && useradd --uid 10001 --gid horde --home-dir /home/horde --create-home --shell /usr/sbin/nologin horde \
    && install -d -o horde -g horde -m 0700 /state /work /join /home/horde/.horde \
    && install -d -o horde -g horde -m 0755 /srv/git
COPY --from=build /out/ /usr/local/bin/
COPY --chmod=0755 docker/multinode/horde-node /usr/local/bin/horde-node
USER horde
WORKDIR /home/horde
ENV HOME=/home/horde
LABEL org.opencontainers.image.title="horde-multinode" \
      org.opencontainers.image.description="Horde server, worker and CLI for the Docker multi-node test harness"
CMD ["horde", "--help"]
