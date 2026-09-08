#!/usr/bin/env bash
# Acceptance scaffold for coherent session navigation (plan P0.2/E1).
#
# Builds the real Arch image (scripts/demo/Dockerfile) from the execution
# worktree and starts three disposable roles on a private network:
#   local     interactive vev client role (optional local daemon per mode)
#   remote-a  first SSH daemon endpoint (aliases: remote-a, remote)
#   remote-b  second SSH daemon endpoint (alias: remote-b)
#
# Fixture-only SSH credentials and distinct A/B host keys are generated per
# run; the demo image's baked fixture keys are replaced inside the owned
# containers at startup. No host HOME, credentials, session state, notes, or
# sockets are mounted. The runner owns only its prefixed containers, network,
# image, and workdir, and cleans them on success, failure, and cancellation
# after exporting artifacts.
#
# This scaffold verifies the Arch/SSH/binary prerequisites and records run
# identity (image ID, source HEAD, binary hash, protocol version) into
# matrix.json. The E3 case matrix and E4 vision review (plan P5) are not
# implemented yet: matrix.json marks every case pending and this script
# exits 0 once the prerequisites hold. It must not be mistaken for V6.
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage: scripts/remote-picker-harness/run.sh

Builds scripts/demo/Dockerfile from the current worktree into a disposable
three-container Arch environment (local, remote-a, remote-b) and verifies
the SSH/binary prerequisites for the coherent-navigation acceptance matrix.
Docker is invoked normally, so DOCKER_HOST and the active Docker context
select the daemon; no socket path is assumed.

Environment:
  DOCKER_HOST              Docker endpoint, when the active context does not
                           already select the rootless daemon.
  VEV_HARNESS_ARTIFACT_DIR Host directory for run artifacts. matrix.json is
                           written there before container cleanup; no report
                           is written when unset.
USAGE
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
  "") ;;
  *)
    usage
    exit 2
    ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
artifact_dir="${VEV_HARNESS_ARTIFACT_DIR:-}"
if [ -n "$artifact_dir" ]; then
  umask 077
  mkdir -p "$artifact_dir"
  chmod 0700 "$artifact_dir"
fi
run_id="$(date +%s)-$$"
image="vev-acceptance:${run_id}"
network="vev-acceptance-${run_id}"
local_container="${network}-local"
remote_a_container="${network}-remote-a"
remote_b_container="${network}-remote-b"
context_dir="$(mktemp -d "${TMPDIR:-/tmp}/vev-acceptance.XXXXXX")"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  set +e
  if [ "$status" -ne 0 ]; then
    printf 'acceptance: local container logs:\n' >&2
    docker logs "$local_container" >&2
    printf 'acceptance: remote-a container logs:\n' >&2
    docker logs "$remote_a_container" >&2
    printf 'acceptance: remote-b container logs:\n' >&2
    docker logs "$remote_b_container" >&2
  fi
  docker rm -f "$local_container" "$remote_a_container" "$remote_b_container" >/dev/null 2>&1
  docker network rm "$network" >/dev/null 2>&1
  docker image rm "$image" >/dev/null 2>&1
  rm -rf "$context_dir"
  exit "$status"
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null || { echo 'acceptance: docker is required' >&2; exit 127; }
command -v ssh-keygen >/dev/null || { echo 'acceptance: ssh-keygen is required' >&2; exit 127; }

if ! docker info >/dev/null 2>&1; then
  echo 'acceptance: Docker is unavailable through the active DOCKER_HOST/context' >&2
  exit 1
fi

printf 'acceptance: building %s from %s\n' "$image" "$repo_root"
docker build -f scripts/demo/Dockerfile -t "$image" "$repo_root" >/dev/null
image_id="$(docker image inspect "$image" --format '{{.Id}}')"
source_head="$(git -C "$repo_root" rev-parse HEAD)"
binary_hash="$(docker run --rm --entrypoint sha256sum "$image" /usr/local/bin/vev | cut -d' ' -f1)"
protocol_version="$(sed -n 's/^const Version uint16 = //p' "$repo_root/internal/protocol/session.go" | tr -d ' ')"
binary_version="$(docker run --rm --entrypoint /usr/local/bin/vev "$image" --version | head -n 1)"

docker network create "$network" >/dev/null

# Fresh fixture-only credentials: one client key and distinct A/B host keys.
# The demo image's baked keys are replaced inside the owned containers below.
ssh-keygen -q -t ed25519 -N '' -C vev-acceptance-client -f "$context_dir/client"
ssh-keygen -q -t ed25519 -N '' -C vev-acceptance-remote-a -f "$context_dir/ssh_host_ed25519_key_a"
ssh-keygen -q -t ed25519 -N '' -C vev-acceptance-remote-b -f "$context_dir/ssh_host_ed25519_key_b"
chmod 0600 "$context_dir/client"

start_role() {
  local name="$1" udp_range="$2"
  shift 2
  docker run -d --name "$name" --entrypoint sleep \
    --network "$network" "$@" \
    -e "XDG_STATE_HOME=/tmp/vev-state-${name##*-}" \
    -e "XDG_RUNTIME_DIR=/tmp/vev-runtime-${name##*-}" \
    -e "VEV_UDP_PORT_RANGE=${udp_range}" \
    "$image" infinity >/dev/null
}

start_role "$remote_a_container" 61000 --network-alias remote-a --network-alias remote
start_role "$remote_b_container" 61001 --network-alias remote-b
start_role "$local_container" 61002 --network-alias local

configure_sshd() {
  local container="$1" host_key="$2" role="$3"
  docker exec --user root "$container" bash -c '
    set -e
    rm -f /etc/ssh/ssh_host_*key* /etc/ssh/ssh_host_*key*.pub
    rm -f /home/demo/.ssh/id_ed25519 /home/demo/.ssh/id_ed25519.pub /home/demo/.ssh/known_hosts
    mkdir -p /run/sshd
  ' >/dev/null
  docker cp "$host_key" "$container:/etc/ssh/ssh_host_ed25519_key"
  docker cp "$host_key.pub" "$container:/etc/ssh/ssh_host_ed25519_key.pub"
  docker cp "$context_dir/client.pub" "$container:/home/demo/.ssh/authorized_keys"
  docker exec --user root "$container" bash -c '
    set -e
    chmod 0600 /etc/ssh/ssh_host_ed25519_key
    chmod 0644 /etc/ssh/ssh_host_ed25519_key.pub
    chown demo:demo /home/demo/.ssh/authorized_keys
    chmod 0600 /home/demo/.ssh/authorized_keys
    /usr/sbin/sshd
  ' >/dev/null
  printf 'acceptance: %s sshd ready\n' "$role"
}

configure_sshd "$remote_a_container" "$context_dir/ssh_host_ed25519_key_a" "remote-a"
configure_sshd "$remote_b_container" "$context_dir/ssh_host_ed25519_key_b" "remote-b"

# Local role: client key plus pinned known-hosts for both aliases.
docker cp "$context_dir/client" "$local_container:/home/demo/.ssh/id_ed25519"
docker exec --user root "$local_container" bash -c '
  set -e
  chown demo:demo /home/demo/.ssh/id_ed25519
  chmod 0600 /home/demo/.ssh/id_ed25519
  rm -f /home/demo/.ssh/known_hosts /home/demo/.ssh/config
  # Drop the baked public half: ssh offers the adjacent .pub before
  # signing, so a stale copy would present the replaced fixture key.
  rm -f /home/demo/.ssh/id_ed25519.pub
  ssh-keygen -y -f /home/demo/.ssh/id_ed25519 > /home/demo/.ssh/id_ed25519.pub
  chown demo:demo /home/demo/.ssh/id_ed25519.pub
  chmod 0644 /home/demo/.ssh/id_ed25519.pub
' >/dev/null
{
  printf 'remote-a '
  cut -d' ' -f1-2 "$context_dir/ssh_host_ed25519_key_a.pub"
  printf 'remote-b '
  cut -d' ' -f1-2 "$context_dir/ssh_host_ed25519_key_b.pub"
  printf 'remote '
  cut -d' ' -f1-2 "$context_dir/ssh_host_ed25519_key_a.pub"
} > "$context_dir/known_hosts"
docker cp "$context_dir/known_hosts" "$local_container:/home/demo/.ssh/known_hosts"
docker exec --user root "$local_container" bash -c '
  chown demo:demo /home/demo/.ssh/known_hosts
  chmod 0600 /home/demo/.ssh/known_hosts
' >/dev/null

check_arch() {
  local container="$1" role="$2"
  if ! docker exec --user demo "$container" bash -c 'grep -q "^ID=arch" /etc/os-release'; then
    echo "acceptance: ${role} is not Arch (/etc/os-release):" >&2
    docker exec "$container" cat /etc/os-release >&2
    exit 1
  fi
  printf 'acceptance: %s Arch verified\n' "$role"
}

check_arch "$local_container" "local"
check_arch "$remote_a_container" "remote-a"
check_arch "$remote_b_container" "remote-b"

printf 'acceptance: waiting for SSH services\n'
for target in remote-a remote-b; do
  ready=0
  for _ in $(seq 1 60); do
    if docker exec --user demo -e HOME=/home/demo "$local_container" ssh -o BatchMode=yes -o ConnectTimeout=1 "demo@${target}" true >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.2
  done
  if [ "$ready" -ne 1 ]; then
    echo "acceptance: SSH service did not become ready on ${target}" >&2
    exit 1
  fi
  printf 'acceptance: SSH demo@%s ready\n' "$target"
done

if [ -n "$artifact_dir" ]; then
  cat > "$artifact_dir/matrix.json" <<EOF
{
  "image": "$image_id",
  "source_head": "$source_head",
  "binary_sha256": "$binary_hash",
  "binary_version": "$binary_version",
  "protocol_version": "$protocol_version",
  "roles": ["local", "remote-a", "remote-b"],
  "transports": ["udp", "stdio"],
  "geometries": ["100x30", "60x24"],
  "cases": "pending: E3 matrix and E4 vision review are not implemented (plan P5)",
  "v6": false
}
EOF
  chmod 0600 "$artifact_dir/matrix.json"
  printf 'acceptance: wrote %s\n' "$artifact_dir/matrix.json"
fi

printf 'acceptance: prerequisites hold (image %s, HEAD %s, protocol %s)\n' "${image_id:0:19}" "${source_head:0:12}" "$protocol_version"
printf 'acceptance: TODO(P5): E3 case matrix and E4 vision review are not implemented; this is not V6\n'
