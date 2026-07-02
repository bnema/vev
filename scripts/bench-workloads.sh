#!/usr/bin/env bash
set -euo pipefail

# Demo workload driver for comparing vev and tmux manually.
# Usage: TARGET=vev|tmux WORKLOAD=yes|seq|cat DURATION=5 ./scripts/bench-workloads.sh
TARGET=${TARGET:-vev}
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
  vev) timeout "$DURATION" ./vev new bench -- sh -lc "$CMD" ;;
  tmux) timeout "$DURATION" tmux new-session -d -s vev-bench "$CMD" && sleep "$DURATION" && tmux kill-session -t vev-bench ;;
  raw) timeout "$DURATION" sh -lc "$CMD" ;;
  *) echo "unknown TARGET: $TARGET" >&2; exit 2 ;;
esac
