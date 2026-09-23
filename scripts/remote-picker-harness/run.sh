#!/usr/bin/env bash
# Disposable navigation acceptance with the real client, daemons and UI driver.
set -euo pipefail
case "${1:-}" in
  -h|--help) printf 'Usage: scripts/remote-picker-harness/run.sh\nRequires Docker, Python 3 and ssh-keygen.\nSet VEV_ACCEPTANCE_ARTIFACTS_DIR to keep logs elsewhere than /tmp.\nSet VEV_ACCEPTANCE_SCENARIOS to a space-separated subset, e.g.\n  VEV_ACCEPTANCE_SCENARIOS="warm-reuse@quic warm-reuse@stdio"\nNames: navigation-repro local-palette-cycle direct-remote-named@quic|@stdio\n  warm-reuse@quic|@stdio hybrid-exact-return@quic|@stdio\n  client-picker-navigate-local@quic client-picker-navigate-direct@quic|@stdio\n'; exit 0 ;;
  '') ;;
  *) exit 2 ;;
esac
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
for tool in docker python3 ssh-keygen; do command -v "$tool" >/dev/null; done
docker info >/dev/null
run_id="$(date +%s)-$$"
image="vev-acceptance:${run_id}"
network="vev-acceptance-${run_id}"
local_container="${network}-local"
remote_container="${network}-remote"
context_dir="$(mktemp -d)"
# Sanitized, per-run evidence outside the worktree. Container state logs carry
# structured vev events only: no host state, credentials, sockets or keys.
artifacts_dir="${VEV_ACCEPTANCE_ARTIFACTS_DIR:-$(mktemp -d "/tmp/vev-acceptance-artifacts-${run_id}.XXXXXX")}"
export VEV_ACCEPTANCE_ARTIFACTS_DIR="$artifacts_dir"

collect_artifacts() {
  mkdir -p "$artifacts_dir"
  for role in local remote; do
    container="${network}-${role}"
    for file in vev-daemon.log vev-client.log vev-stdio.log \
                vev-daemon-crash.log vev-client-crash.log vev-stdio-crash.log; do
      docker cp "$container:/home/demo/.local/state/vev/$file" \
        "$artifacts_dir/${role}-${file}" >/dev/null 2>&1 || true
    done
    docker cp "$container:/home/demo/.local/state/vev/broker/log/vev-daemon.log" \
      "$artifacts_dir/${role}-vev-broker.log" >/dev/null 2>&1 || true
  done
}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  set +e
  collect_artifacts
  if [ "$status" -ne 0 ]; then
    for file in "$artifacts_dir"/*.log; do
      [ -f "$file" ] || continue
      printf '===== %s =====\n' "$file" >&2
      tail -n 60 "$file" >&2
    done
  fi
  docker rm -f "$local_container" "$remote_container" >/dev/null 2>&1
  docker network rm "$network" >/dev/null 2>&1
  docker image rm "$image" >/dev/null 2>&1
  rm -rf "$context_dir"
  printf 'acceptance artifacts: %s\n' "$artifacts_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker build -f "$repo_root/scripts/demo/Dockerfile" -t "$image" "$repo_root" >/dev/null
docker network create "$network" >/dev/null
# Client and daemon debug logging is enabled container-wide; docker exec and
# the daemon child both inherit it. Remote proxy children receive it through
# ssh SendEnv/AcceptEnv below.
for role in local remote; do
  docker run -d --name "${network}-${role}" --network "$network" \
    --network-alias "$role" -e VEV_LOG=debug \
    --entrypoint sleep "$image" infinity >/dev/null
done

# Only generated fixture credentials enter these containers; no host HOME or
# sockets are mounted. Pin the fresh remote key instead of trusting keyscan.
ssh-keygen -q -t ed25519 -N '' -C acceptance-client -f "$context_dir/client"
ssh-keygen -q -t ed25519 -N '' -C acceptance-host -f "$context_dir/host"
docker exec --user root "$remote_container" sh -c 'rm -f /etc/ssh/ssh_host_* /home/demo/.ssh/id_ed25519*; mkdir -p /run/sshd'
docker cp "$context_dir/host" "$remote_container:/etc/ssh/ssh_host_ed25519_key"
docker cp "$context_dir/host.pub" "$remote_container:/etc/ssh/ssh_host_ed25519_key.pub"
docker cp "$context_dir/client.pub" "$remote_container:/home/demo/.ssh/authorized_keys"
# Accept the debug level the client forwards so remote _stdio/_quic-proxy and
# the daemon they spawn log at debug too.
docker exec --user root "$remote_container" sh -c 'printf "\nAcceptEnv VEV_LOG\n" >> /etc/ssh/sshd_config; chmod 600 /etc/ssh/ssh_host_ed25519_key /home/demo/.ssh/authorized_keys; chown demo:demo /home/demo/.ssh/authorized_keys; /usr/sbin/sshd'
docker cp "$context_dir/client" "$local_container:/home/demo/.ssh/id_ed25519"
docker cp "$context_dir/client.pub" "$local_container:/home/demo/.ssh/id_ed25519.pub"
{ printf 'remote '; cut -d' ' -f1-2 "$context_dir/host.pub"; } > "$context_dir/known_hosts"
docker cp "$context_dir/known_hosts" "$local_container:/home/demo/.ssh/known_hosts"
docker exec --user root "$local_container" sh -c 'chown demo:demo /home/demo/.ssh/id_ed25519* /home/demo/.ssh/known_hosts; chmod 600 /home/demo/.ssh/id_ed25519 /home/demo/.ssh/known_hosts'
# Forward VEV_LOG on every remote SSH child (global option before the first
# Host block); the fixture ssh config itself is otherwise untouched.
docker exec "$local_container" sh -c 'p="$HOME/.ssh/config"; { printf "SendEnv VEV_LOG\n"; cat "$p"; } > "$p.new" && mv "$p.new" "$p" && chmod 600 "$p"'
docker exec "$local_container" ssh -o BatchMode=yes -o ConnectTimeout=5 remote true
docker exec "$local_container" vev host add remote
# Baseline matrix: local IPC, direct named-remote (QUIC + explicit stdio, no
# local daemon), warm remote reuse (QUIC + stdio), and hybrid exact-return
# (QUIC + stdio). Each scenario asserts committed outcomes, not sent keys.
# navigation_repro.py keeps the original hybrid transport regression.
acceptance_py="$repo_root/scripts/remote-picker-harness/acceptance.py"
scenarios_env="${VEV_ACCEPTANCE_SCENARIOS:-}"
known_scenarios="navigation-repro local-palette-cycle direct-remote-named@quic direct-remote-named@stdio warm-reuse@quic warm-reuse@stdio hybrid-exact-return@quic hybrid-exact-return@stdio client-picker-navigate-local@quic client-picker-navigate-direct@quic client-picker-navigate-direct@stdio"
for requested in $scenarios_env; do
  case " $known_scenarios " in
    *" $requested "*) ;;
    *) printf 'unknown acceptance scenario: %s\n' "$requested" >&2; exit 2 ;;
  esac
done
if [ -n "$scenarios_env" ] && [ -z "${scenarios_env//[[:space:]]/}" ]; then
  printf 'VEV_ACCEPTANCE_SCENARIOS selects no scenarios\n' >&2
  exit 2
fi
wanted_scenario() {
  [ -z "$scenarios_env" ] && return 0
  for want in $scenarios_env; do [ "$want" = "$1" ] && return 0; done
  return 1
}
run_scenario() { # name spec [topology]
  wanted_scenario "$1" || return 0
  if [ -n "${3:-}" ]; then
    VEV_ACCEPTANCE_TOPOLOGY="$3" python3 "$acceptance_py" "$local_container" "$remote_container" "$2"
  else
    python3 "$acceptance_py" "$local_container" "$remote_container" "$2"
  fi
}
if wanted_scenario navigation-repro; then
  python3 "$repo_root/scripts/remote-picker-harness/navigation_repro.py" "$local_container" "$remote_container"
fi
run_scenario local-palette-cycle local-palette-cycle
run_scenario direct-remote-named@quic direct-remote-named@quic
run_scenario direct-remote-named@stdio direct-remote-named@stdio
run_scenario warm-reuse@quic warm-reuse@quic
run_scenario warm-reuse@stdio warm-reuse@stdio
run_scenario hybrid-exact-return@quic hybrid-exact-return@quic
run_scenario hybrid-exact-return@stdio hybrid-exact-return@stdio
# Client-owned navigation picker where the serving attachment answers the
# palette session-picker command itself: local IPC and direct remote (QUIC and
# explicit stdio). A hybrid client advertises the home-picker capability, so
# its session-picker command delegates to the home route overlay (an R1
# behavior pinned by hybrid-exact-return) instead of this interaction.
run_scenario client-picker-navigate-local@quic client-picker-navigate@quic local
run_scenario client-picker-navigate-direct@quic client-picker-navigate@quic direct
run_scenario client-picker-navigate-direct@stdio client-picker-navigate@stdio direct
