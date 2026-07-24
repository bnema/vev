#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

TAPE="${1:-demo.tape}"
install -d -m 0777 scripts/demo/out

CLAUDE_TMP=$(mktemp -d)
# claude writes files into the temp HOME as the container's demo user, which
# rootless docker maps to a subuid the host cannot delete; the rm then fails and
# leaves a dir in /tmp. Accepted (cleared on reboot); `|| true` keeps the script
# exit status clean. A container-based cleanup isn't worth the complexity.
trap 'rm -rf "$CLAUDE_TMP" 2>/dev/null || true' EXIT
mkdir -p "$CLAUDE_TMP/.claude"
cp "$HOME/.claude/.credentials.json" "$CLAUDE_TMP/.claude/"
cp scripts/demo/claude-settings.json "$CLAUDE_TMP/.claude/settings.json"
printf '{"hasCompletedOnboarding": true, "theme": "dark", "projects": {"/home/demo": {"hasTrustDialogAccepted": true, "hasCompletedProjectOnboarding": true, "projectOnboardingSeenCount": 1}}}\n' > "$CLAUDE_TMP/.claude.json"

# Rootless docker maps host-owned files (uid 1000) to container root, but the
# container runs as demo (uid 1000 -> a subuid), so demo cannot read the
# credentials or write its config dir. Make the throwaway home world-accessible
# so claude can authenticate and write transcripts.
chmod -R a+rwX "$CLAUDE_TMP"

docker run --rm --hostname vev-demo \
  -v "$PWD/scripts/demo:/tape:ro" \
  -v "$PWD/scripts/demo/out:/home/demo/out" \
  -v "$CLAUDE_TMP/.claude:/home/demo/.claude" \
  -v "$CLAUDE_TMP/.claude.json:/home/demo/.claude.json" \
  vev-demo "/tape/$TAPE"
