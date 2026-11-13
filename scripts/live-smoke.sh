#!/usr/bin/env bash
# Every manual live smoke, against the harnesses installed on this machine.
#
#   scripts/live-smoke.sh            # claude and codex
#   scripts/live-smoke.sh claude     # one provider
#
# The automated suite never touches a real harness; these do, and spend a
# few hundred tokens each. Run them when bumping a supported CLI version
# (docs/ADAPTERS.md, "Golden transcripts"). Per provider:
#
#   * the codec against the real CLI, under the operator's own login;
#   * codex only: the protocol-surface drift check against
#     `codex app-server generate-json-schema` (refresh the snapshot with
#     HORDE_UPDATE_CODEX_SURFACE=1 once a diff has been reviewed);
#   * the production adapter through the Worker's supervisor, under a
#     Worker-owned profile — only when HORDE_LIVE_HARNESS_ROOT names one that
#     is signed in, because the operator's login is deliberately not inherited:
#
#       export HORDE_LIVE_HARNESS_ROOT=$HOME/.horde-live-harness
#       CLAUDE_CONFIG_DIR=$HORDE_LIVE_HARNESS_ROOT/claude/default claude login
#       CODEX_HOME=$HORDE_LIVE_HARNESS_ROOT/codex/default codex login
#
# Tests run one at a time: concurrent harnesses on one fresh account
# directory race its first-run initialization.
set -euo pipefail

cd "$(dirname "$0")/.."
which="${1:-all}"
case "$which" in
  all | claude | codex) ;;
  *) echo "usage: $0 [all|claude|codex]" >&2; exit 2 ;;
esac

run() {
  echo "==> $*"
  "$@"
}

if [[ "$which" == all || "$which" == claude ]]; then
  echo "claude: $(claude --version)"
  run cargo test -p horde-adapter-claude --test live_smoke -- --ignored --nocapture --test-threads=1
fi
if [[ "$which" == all || "$which" == codex ]]; then
  echo "codex: $(codex --version)"
  run cargo test -p horde-adapter-codex --test live_smoke --test schema_drift -- --ignored --nocapture --test-threads=1
fi

if [[ -z "${HORDE_LIVE_HARNESS_ROOT:-}" ]]; then
  echo "skipping the Worker-level smokes: set HORDE_LIVE_HARNESS_ROOT to a signed-in profile (see this script's header)"
  exit 0
fi
filter=""
[[ "$which" != all ]] && filter="$which"
run cargo test -p horde-worker --test live_smoke -- --ignored --nocapture --test-threads=1 $filter
