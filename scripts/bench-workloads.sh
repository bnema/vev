#!/usr/bin/env bash
set -euo pipefail

# Demo workload driver for raw/tmux workload generation.
# vev does not currently support `vev new <name> -- command`, so this script
# intentionally does not advertise an automated vev workload path.
# Usage: TARGET=raw|tmux WORKLOAD=yes|seq|cat DURATION=5 ./scripts/bench-workloads.sh
TARGET=${TARGET:-raw}
WORKLOAD=${WORKLOAD:-yes}
DURATION=${DURATION:-5}

case "$WORKLOAD" in
  yes) CMD='yes vev-benchmark' ;;
  seq) CMD='while :; do seq 1 20000; done' ;;
  cat) CMD='cat go.sum go.mod >/dev/null; while :; do cat go.sum; done' ;;
  *) echo "unknown WORKLOAD: $WORKLOAD" >&2; exit 2 ;;
esac

echo "target=$TARGET workload=$WORKLOAD duration=${DURATION}s"
echo "command=$CMD"
case "$TARGET" in
  vev) echo "TARGET=vev is unsupported until vev implements command overrides" >&2; exit 2 ;;
  tmux) timeout "$DURATION" tmux new-session -d -s vev-bench "$CMD" && sleep "$DURATION" && tmux kill-session -t vev-bench ;;
  raw) timeout "$DURATION" sh -lc "$CMD" ;;
  *) echo "unknown TARGET: $TARGET" >&2; exit 2 ;;
esac
