#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE' >&2
Usage: scripts/debug-remote-attach.sh [--mode stdio|udp|default] [--session NAME] [--duration 8s] [--vev-bin PATH] [--udp-health] user@host

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
  - --udp-health collects redacted UDP transport-health log lines and Linux
    ss -u -m socket-memory counters when available.
  - The attach attempt unsets VEV so the smoke test can run from inside vev.
USAGE
}

mode="default"
session=""
duration="8s"
vev_bin="${VEV_BIN:-vev}"
collect_udp_health=false
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
    --udp-health)
      collect_udp_health=true; shift ;;
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
out_dir="${TMPDIR:-/tmp}/vev-remote-attach-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$out_dir"

local_state="${XDG_STATE_HOME:-$HOME/.local/state}/vev"
diagnostic_files=()

# All persisted and displayed command output passes through this filter. Keep
# diagnostic counters while removing the target, local home, network addresses,
# and common key material labels from bundles that may be shared.
redact() {
  sed -E \
    -e "s|${target//\/\\/\\}|[remote]|g" \
    -e "s|${HOME//\/\\/\\}|[home]|g" \
    -e 's/([[:alnum:]_.-]+@)?([[:alnum:]_.-]+\.)+[[:alpha:]]{2,}/[host]/g' \
    -e 's/([0-9]{1,3}\.){3}[0-9]{1,3}/[address]/g' \
    -e 's/([[:xdigit:]]{1,4}:){2,}[[:xdigit:]:]*/[address]/g' \
    -e 's/(private[ _-]?key|ssh-rsa|ssh-ed25519|ecdsa-sha2-[^[:space:]]*)/[redacted-key]/Ig'
}

redact_file() {
  local raw="$1" dest="$2"
  redact <"$raw" >"$dest" || true
  rm -f "$raw"
}

run_capture() {
  local name="$1" raw_out raw_err status
  shift
  raw_out="$out_dir/.$name.out.raw"
  raw_err="$out_dir/.$name.err.raw"
  status=0
  {
    printf '$'
    printf ' %q' "$@"
    printf '\n'
    "$@"
  } >"$raw_out" 2>"$raw_err" || status=$?
  redact_file "$raw_out" "$out_dir/$name.out"
  redact_file "$raw_err" "$out_dir/$name.err"
  if [[ "$status" -ne 0 ]]; then printf '%s\n' "$status" >"$out_dir/$name.exit"; fi
}

copy_local_logs() {
  local label="$1" raw
  mkdir -p "$out_dir/local-$label"
  if [[ -d "$local_state" ]]; then
    find "$local_state" -maxdepth 1 -type f -name 'vev*.log' -print0 2>/dev/null | while IFS= read -r -d '' f; do
      raw="$out_dir/local-$label/.$(basename "$f").raw"
      tail -n 200 "$f" >"$raw" || true
      redact_file "$raw" "$out_dir/local-$label/$(basename "$f")"
    done
  fi
}

copy_remote_logs() {
  local label="$1" raw_out="$out_dir/remote-$1/.logs.out.raw" raw_err="$out_dir/remote-$1/.logs.err.raw"
  mkdir -p "$out_dir/remote-$label"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "$target" 'state="${XDG_STATE_HOME:-$HOME/.local/state}/vev"; if [ -d "$state" ]; then for f in "$state"/vev*.log; do [ -e "$f" ] || continue; echo "===== $f ====="; tail -n 200 "$f"; done; fi' \
    >"$raw_out" 2>"$raw_err" || true
  redact_file "$raw_out" "$out_dir/remote-$label/logs.out"
  redact_file "$raw_err" "$out_dir/remote-$label/logs.err"
}

collect_udp_diagnostics() {
  local label="$1" local_dir="$out_dir/local-$1" remote_dir="$out_dir/remote-$1"
  if ! "$collect_udp_health"; then return; fi

  grep -hE 'udp( transport)? health' "$local_dir"/*.log 2>/dev/null | redact >"$local_dir/udp-transport-health.out" || true
  (ss -u -m 2>/dev/null || true) | awk '/skmem:/' | redact >"$local_dir/udp-socket-memory.out"
  diagnostic_files+=("$local_dir/udp-transport-health.out" "$local_dir/udp-socket-memory.out")

  ssh -o BatchMode=yes -o ConnectTimeout=8 "$target" 'state="${XDG_STATE_HOME:-$HOME/.local/state}/vev"; [ -d "$state" ] && grep -hE "udp( transport)? health" "$state"/vev*.log 2>/dev/null || true' \
    2>&1 | redact >"$remote_dir/udp-transport-health.out" || true
  ssh -o BatchMode=yes -o ConnectTimeout=8 "$target" 'ss -u -m 2>/dev/null | awk "/skmem:/" || true' \
    2>&1 | redact >"$remote_dir/udp-socket-memory.out" || true
  diagnostic_files+=("$remote_dir/udp-transport-health.out" "$remote_dir/udp-socket-memory.out")
}

printf 'debug bundle: %s\n' "$out_dir"
printf 'mode: %s\nduration: %s\n' "$mode" "$duration" >"$out_dir/summary.txt"

raw_git="$out_dir/.git.raw"
(
  cd "$repo_root"
  git rev-parse --show-toplevel
  git rev-parse --short HEAD
  git status --short
) >"$raw_git" 2>&1 || true
redact_file "$raw_git" "$out_dir/git.txt"

run_capture local-version "$vev_bin" --version
raw_remote_version_out="$out_dir/.remote-version.out.raw"
raw_remote_version_err="$out_dir/.remote-version.err.raw"
ssh -o BatchMode=yes -o ConnectTimeout=8 "$target" 'set -e; command -v vev; vev --version; pgrep -a vev || true; state="${XDG_STATE_HOME:-$HOME/.local/state}/vev"; echo "state=$state"' \
  >"$raw_remote_version_out" 2>"$raw_remote_version_err" || echo "$?" >"$out_dir/remote-version.exit"
redact_file "$raw_remote_version_out" "$out_dir/remote-version.out"
redact_file "$raw_remote_version_err" "$out_dir/remote-version.err"

copy_local_logs before
copy_remote_logs before

attach_log_raw="$out_dir/.attach.typescript.raw"
attach_err_raw="$out_dir/.attach.err.raw"
attach_stdout_raw="$out_dir/.attach.stdout.raw"
cmd=("$vev_bin" attach "$attach_target")
if [[ "$mode" == "default" ]]; then
  env_cmd=(env -u VEV -u VEV_REMOTE_TRANSPORT)
else
  env_cmd=(env -u VEV "VEV_REMOTE_TRANSPORT=$mode")
fi

set +e
if command -v script >/dev/null 2>&1; then
  printf -v attach_cmd '%q ' "${cmd[@]}"
  "${env_cmd[@]}" timeout "$duration" script -q -e -c "$attach_cmd" "$attach_log_raw" >"$attach_stdout_raw" 2>"$attach_err_raw"
else
  "${env_cmd[@]}" timeout "$duration" "${cmd[@]}" >"$attach_stdout_raw" 2>"$attach_err_raw"
fi
attach_status=$?
set -e
redact_file "$attach_stdout_raw" "$out_dir/attach.stdout"
redact_file "$attach_err_raw" "$out_dir/attach.err"
if [[ -e "$attach_log_raw" ]]; then redact_file "$attach_log_raw" "$out_dir/attach.typescript"; fi
printf '%s\n' "$attach_status" >"$out_dir/attach.exit"

copy_local_logs after
copy_remote_logs after
collect_udp_diagnostics after

{
  echo "debug bundle: $out_dir"
  echo "attach exit: $attach_status"
  echo "local version:"
  sed 's/^/  /' "$out_dir/local-version.out" 2>/dev/null || true
  echo "remote version:"
  sed 's/^/  /' "$out_dir/remote-version.out" 2>/dev/null || true
  if [[ -s "$out_dir/attach.err" ]]; then
    echo "attach stderr:"
    sed 's/^/  /' "$out_dir/attach.err" | tail -40
  fi
  if ((${#diagnostic_files[@]})); then
    echo "collected diagnostic files:"
    printf '  %s\n' "${diagnostic_files[@]}"
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
