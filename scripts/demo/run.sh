#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

TAPE="${1:-demo.tape}"
mkdir -p scripts/demo/out

CLAUDE_TMP=$(mktemp -d)
trap 'rm -rf "$CLAUDE_TMP"' EXIT
mkdir -p "$CLAUDE_TMP/.claude"
cp "$HOME/.claude/.credentials.json" "$CLAUDE_TMP/.claude/"
cp scripts/demo/claude-settings.json "$CLAUDE_TMP/.claude/settings.json"
printf '{"hasCompletedOnboarding": true, "theme": "dark"}\n' > "$CLAUDE_TMP/.claude.json"

docker run --rm --hostname vev-demo \
  -v "$PWD/scripts/demo:/tape:ro" \
  -v "$PWD/scripts/demo/out:/home/demo/out" \
  -v "$CLAUDE_TMP/.claude:/home/demo/.claude" \
  -v "$CLAUDE_TMP/.claude.json:/home/demo/.claude.json" \
  vev-demo "/tape/$TAPE"
