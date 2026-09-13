#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
help="$("$repo_root/scripts/debug-remote-attach.sh" --help 2>&1)"
grep -Fq -- '--mode stdio|quic|default' <<<"$help"
if grep -Fiq -- 'udp' <<<"$help"; then
  echo 'udp mode still advertised' >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin" "$work/state/vev"
printf 'peer=192.0.2.1 home=%s/local\n' "$HOME" >"$work/state/vev/vev-test.log"
cat > "$work/bin/ssh" <<'SSH'
#!/usr/bin/env bash
printf 'peer=198.51.100.2 key=ssh-ed25519\n'
SSH
chmod +x "$work/bin/ssh"

TMPDIR="$work" XDG_STATE_HOME="$work/state" PATH="$work/bin:$PATH" \
  "$repo_root/scripts/debug-remote-attach.sh" --mode quic --duration 0s --vev-bin /bin/true alice@mobile.example.com >"$work/run.out" 2>&1
bundle="$(find "$work" -maxdepth 1 -type d -name 'vev-remote-attach-*' -print -quit)"
test -n "$bundle"
test -f "$bundle/local-after/vev-test.log"
if grep -R --exclude='*.raw' -F -e 'alice@mobile.example.com' -e '192.0.2.1' -e '198.51.100.2' -e 'ssh-ed25519' -e "$HOME/local" "$bundle"; then
  echo 'unredacted diagnostic value found' >&2
  exit 1
fi

echo 'debug-remote-attach redaction: ok'
