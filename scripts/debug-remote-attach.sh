#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE' >&2
Usage: scripts/debug-remote-attach.sh [--mode stdio|udp|default] [--session NAME] [--duration 8s] [--vev-bin PATH] user@host

Runs a real remote attach smoke/debug attempt under a PTY, captures local and
remote vev versions, and stores local/remote log tails around the attempt.

Examples:
  scripts/debug-remote-attach.sh brice@arch
  scripts/debug-remote-attach.sh --session work brice@arch
  scripts/debug-remote-attach.sh --mode udp brice@arch
  go build -o /tmp/vev-debug . && scripts/debug-remote-attach.sh --vev-bin /tmp/vev-debug brice@arch

Notes:
  - The command times out intentionally if attach succeeds and stays interactive.
  - Exit 124 from timeout is treated as a likely successful attach if no early
    vev/ssh error is captured; inspect the bundle for confirmation.
  - The remote host must be reachable by SSH for log collection.
  - The attach attempt unsets VEV so the smoke test can run from inside vev.
USAGE
}

mode="default"
session=""
duration="8s"
vev_bin="${VEV_BIN:-vev}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      mode="${2:-}"; shift 2 ;;
    --session)
      session="${2:-}"; shift 2 ;;
    --duration)
      duration="${2:-}"; shift 2 ;;
    --vev-bin)
      vev_bin="${2:-}"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    --)
      shift; break ;;
    -*)
      echo "unknown option: $1" >&2; usage; exit 2 ;;
    *)
      break ;;
  esac
done

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi
case "$mode" in
  default|stdio|udp) ;;
  *) echo "invalid --mode $mode (want default, stdio, or udp)" >&2; exit 2 ;;
esac

target="$1"
attach_target="$target"
if [[ -n "$session" ]]; then
  attach_target="$target:$session"
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out_dir="${TMPDIR:-/tmp}/vev-remote-attach-$target-$(date +%Y%m%d-%H%M%S)"
out_dir="${out_dir//@/_}"
out_dir="${out_dir//:/_}"
mkdir -p "$out_dir"

local_state="${XDG_STATE_HOME:-$HOME/.local/state}/vev"

run_capture() {
  local name="$1"; shift
  {
    printf '$'
    printf ' %q' "$@"
    printf '\n'
    "$@"
  } >"$out_dir/$name.out" 2>"$out_dir/$name.err" || printf '%s\n' "$?" >"$out_dir/$name.exit"
}

copy_local_logs() {
  local label="$1"
  mkdir -p "$out_dir/local-$label"
  if [[ -d "$local_state" ]]; then
    find "$local_state" -maxdepth 1 -type f -name 'vev*.log' -print0 2>/dev/null | while IFS= read -r -d '' f; do
      tail -n 200 "$f" >"$out_dir/local-$label/$(basename "$f")" || true
    done
  fi
}

copy_remote_logs() {
  local label="$1"
  mkdir -p "$out_dir/remote-$label"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "$target" 'state="${XDG_STATE_HOME:-$HOME/.local/state}/vev"; if [ -d "$state" ]; then for f in "$state"/vev*.log; do [ -e "$f" ] || continue; echo "===== $f ====="; tail -n 200 "$f"; done; fi' \
    >"$out_dir/remote-$label/logs.out" 2>"$out_dir/remote-$label/logs.err" || true
}

printf 'debug bundle: %s\n' "$out_dir"
printf 'target: %s\nattach_target: %s\nmode: %s\nduration: %s\n' "$target" "$attach_target" "$mode" "$duration" >"$out_dir/summary.txt"

(
  cd "$repo_root"
  git rev-parse --show-toplevel
  git rev-parse --short HEAD
  git status --short
) >"$out_dir/git.txt" 2>&1 || true

run_capture local-version "$vev_bin" --version
ssh -o BatchMode=yes -o ConnectTimeout=8 "$target" 'set -e; command -v vev; vev --version; pgrep -a vev || true; state="${XDG_STATE_HOME:-$HOME/.local/state}/vev"; echo "state=$state"' \
  >"$out_dir/remote-version.out" 2>"$out_dir/remote-version.err" || echo "$?" >"$out_dir/remote-version.exit"

copy_local_logs before
copy_remote_logs before

attach_log="$out_dir/attach.typescript"
attach_err="$out_dir/attach.err"
cmd=("$vev_bin" attach "$attach_target")
if [[ "$mode" == "default" ]]; then
  env_cmd=(env -u VEV -u VEV_REMOTE_TRANSPORT)
else
  env_cmd=(env -u VEV "VEV_REMOTE_TRANSPORT=$mode")
fi

set +e
if command -v script >/dev/null 2>&1; then
  printf -v attach_cmd '%q ' "${cmd[@]}"
  "${env_cmd[@]}" timeout "$duration" script -q -e -c "$attach_cmd" "$attach_log" >"$out_dir/attach.stdout" 2>"$attach_err"
else
  "${env_cmd[@]}" timeout "$duration" "${cmd[@]}" >"$out_dir/attach.stdout" 2>"$attach_err"
fi
attach_status=$?
set -e
printf '%s\n' "$attach_status" >"$out_dir/attach.exit"

copy_local_logs after
copy_remote_logs after

{
  echo "debug bundle: $out_dir"
  echo "attach exit: $attach_status"
  echo "local version:"
  sed 's/^/  /' "$out_dir/local-version.out" 2>/dev/null || true
  echo "remote version:"
  sed 's/^/  /' "$out_dir/remote-version.out" 2>/dev/null || true
  if [[ -s "$attach_err" ]]; then
    echo "attach stderr:"
    sed 's/^/  /' "$attach_err" | tail -40
  fi
  echo "recent local after logs:"
  find "$out_dir/local-after" -maxdepth 1 -type f -print0 2>/dev/null | while IFS= read -r -d '' f; do
    echo "  --- $(basename "$f") ---"
    tail -20 "$f" | sed 's/^/    /'
  done
  echo "recent remote after logs:"
  tail -80 "$out_dir/remote-after/logs.out" 2>/dev/null | sed 's/^/  /' || true
} | tee -a "$out_dir/summary.txt"

if [[ "$attach_status" -eq 124 ]]; then
  echo "attach timed out after $duration; this often means the interactive attach stayed alive. Inspect $out_dir for protocol/log errors."
  exit 0
fi
exit "$attach_status"
