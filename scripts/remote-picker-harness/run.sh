#!/usr/bin/env bash
# Disposable navigation acceptance with the real client, daemons and UI driver.
set -euo pipefail
case "${1:-}" in
  -h|--help) printf 'Usage: scripts/remote-picker-harness/run.sh\nRequires Docker, Python 3 and ssh-keygen.\n'; exit 0 ;;
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
cleanup() {
  status=$?
  trap - EXIT INT TERM
  set +e
  if [ "$status" -ne 0 ]; then
    for container in "$local_container" "$remote_container"; do
      docker logs "$container" >&2
      docker exec "$container" sh -c 'tail -n 40 ~/.local/state/vev/vev-daemon.log' >&2
    done
  fi
  docker rm -f "$local_container" "$remote_container" >/dev/null 2>&1
  docker network rm "$network" >/dev/null 2>&1
  docker image rm "$image" >/dev/null 2>&1
  rm -rf "$context_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker build -f "$repo_root/scripts/demo/Dockerfile" -t "$image" "$repo_root" >/dev/null
docker network create "$network" >/dev/null
for role in local remote; do
  docker run -d --name "${network}-${role}" --network "$network" \
    --network-alias "$role" --entrypoint sleep "$image" infinity >/dev/null
done

# Only generated fixture credentials enter these containers; no host HOME or
# sockets are mounted. Pin the fresh remote key instead of trusting keyscan.
ssh-keygen -q -t ed25519 -N '' -C acceptance-client -f "$context_dir/client"
ssh-keygen -q -t ed25519 -N '' -C acceptance-host -f "$context_dir/host"
docker exec --user root "$remote_container" sh -c 'rm -f /etc/ssh/ssh_host_* /home/demo/.ssh/id_ed25519*; mkdir -p /run/sshd'
docker cp "$context_dir/host" "$remote_container:/etc/ssh/ssh_host_ed25519_key"
docker cp "$context_dir/host.pub" "$remote_container:/etc/ssh/ssh_host_ed25519_key.pub"
docker cp "$context_dir/client.pub" "$remote_container:/home/demo/.ssh/authorized_keys"
docker exec --user root "$remote_container" sh -c 'chmod 600 /etc/ssh/ssh_host_ed25519_key /home/demo/.ssh/authorized_keys; chown demo:demo /home/demo/.ssh/authorized_keys; /usr/sbin/sshd'
docker cp "$context_dir/client" "$local_container:/home/demo/.ssh/id_ed25519"
docker cp "$context_dir/client.pub" "$local_container:/home/demo/.ssh/id_ed25519.pub"
{ printf 'remote '; cut -d' ' -f1-2 "$context_dir/host.pub"; } > "$context_dir/known_hosts"
docker cp "$context_dir/known_hosts" "$local_container:/home/demo/.ssh/known_hosts"
docker exec --user root "$local_container" sh -c 'chown demo:demo /home/demo/.ssh/id_ed25519* /home/demo/.ssh/known_hosts; chmod 600 /home/demo/.ssh/id_ed25519 /home/demo/.ssh/known_hosts'
docker exec "$local_container" ssh -o BatchMode=yes -o ConnectTimeout=5 remote true
docker exec "$local_container" vev host add remote
python3 "$repo_root/scripts/remote-picker-harness/navigation_repro.py" "$local_container" "$remote_container"
