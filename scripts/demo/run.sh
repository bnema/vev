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
# credentials or write its config dir. Open up the throwaway home's contents so
# claude can authenticate and write transcripts, then restore the mktemp
# parent to 0700: bind mounts hand the container the inner paths directly, but
# other host users would have to traverse the parent, which now blocks them.
chmod -R a+rwX "$CLAUDE_TMP"
chmod 0700 "$CLAUDE_TMP"

docker run --rm --hostname vev-demo \
  -v "$PWD/scripts/demo:/tape:ro" \
  -v "$PWD/scripts/demo/out:/home/demo/out" \
  -v "$CLAUDE_TMP/.claude:/home/demo/.claude" \
  -v "$CLAUDE_TMP/.claude.json:/home/demo/.claude.json" \
  vev-demo "/tape/$TAPE"
