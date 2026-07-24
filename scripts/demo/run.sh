#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

TAPE="${1:-demo.tape}"
install -d -m 0777 scripts/demo/out

CLAUDE_TMP=$(mktemp -d)
# claude writes files into the temp HOME as the container's demo user, which
# rootless docker maps to a subuid the host cannot delete directly. Clean up
# through the container first, where the creating uid can unlink its own
# files, then remove the (now-empty) temp dir from the host. This never
# suppresses a leftover: if anything still remains, it's reported so a
# credentials-bearing directory doesn't silently linger in /tmp.
cleanup() {
	# Every step here is best-effort: under `set -e`, an unguarded failure in an
	# EXIT trap would abort the trap early and replace the script's real exit
	# status with the failing cleanup command's. Guard each step with `|| true`
	# so cleanup always runs to completion and never masks the vhs run's result;
	# the final check below is what surfaces an unrecovered leftover.
	rm -rf "$CLAUDE_TMP" 2>/dev/null || true
	if [ -d "$CLAUDE_TMP" ]; then
		docker run --rm -v "$CLAUDE_TMP:/cleanup" --entrypoint sh vev-demo \
			-c 'rm -rf /cleanup/.claude /cleanup/.claude.json' 2>/dev/null || true
		rmdir "$CLAUDE_TMP" 2>/dev/null || true
	fi
	if [ -d "$CLAUDE_TMP" ]; then
		echo "warning: could not remove claude temp home, left at $CLAUDE_TMP" >&2
	fi
	return 0
}
trap cleanup EXIT
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
