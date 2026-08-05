#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage: scripts/remote-picker-harness/run.sh

Builds the current vev binary into a disposable two-container environment and
runs the SSH-stdio, UDP, catalog, preview, lifecycle-fence, and environment
acceptance checks. Docker is invoked normally, so DOCKER_HOST and the active
Docker context select the daemon; no socket path is assumed.

Environment:
  DOCKER_HOST             Docker endpoint, when the active context does not
                          already select the rootless daemon.
  VEV_HARNESS_BASE_IMAGE  Base image for the disposable containers
                          (default: ubuntu:24.04).
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
base_image="${VEV_HARNESS_BASE_IMAGE:-ubuntu:24.04}"
run_id="$(date +%s)-$$"
image="vev-remote-picker-harness:${run_id}"
network="vev-remote-picker-harness-${run_id}"
local_container="${network}-local"
remote_container="${network}-remote"
context_dir="$(mktemp -d "${TMPDIR:-/tmp}/vev-remote-picker-harness.XXXXXX")"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  set +e
  if [ "$status" -ne 0 ]; then
    printf 'remote picker harness: local container logs:\n' >&2
    docker logs "$local_container" >&2
    printf 'remote picker harness: remote container logs:\n' >&2
    docker logs "$remote_container" >&2
  fi
  docker rm -f "$local_container" "$remote_container" >/dev/null 2>&1
  docker network rm "$network" >/dev/null 2>&1
  docker image rm "$image" >/dev/null 2>&1
  rm -rf "$context_dir"
  exit "$status"
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null || { echo 'remote picker harness: docker is required' >&2; exit 127; }
command -v ssh-keygen >/dev/null || { echo 'remote picker harness: ssh-keygen is required' >&2; exit 127; }

if ! docker info >/dev/null 2>&1; then
  echo 'remote picker harness: Docker is unavailable through the active DOCKER_HOST/context' >&2
  exit 1
fi

printf 'remote picker harness: building %s\n' "$image"
docker pull "$base_image" >/dev/null
image_arch="$(docker image inspect "$base_image" --format '{{.Architecture}}')"
GOOS=linux GOARCH="$image_arch" CGO_ENABLED=0 go build -o "$context_dir/vev" "$repo_root"
GOOS=linux GOARCH="$image_arch" CGO_ENABLED=0 go build -o "$context_dir/harness" "$repo_root/scripts/remote-picker-harness"
ssh-keygen -q -t ed25519 -N '' -f "$context_dir/id_ed25519"
cp "$repo_root/scripts/remote-picker-harness/Dockerfile" "$context_dir/Dockerfile"
cp "$repo_root/scripts/remote-picker-harness/vev-wrapper" "$context_dir/vev-wrapper"
printf '%s\n' 'id_ed25519' > "$context_dir/.dockerignore"
chmod 0600 "$context_dir/id_ed25519"

docker build --build-arg "BASE_IMAGE=$base_image" -t "$image" "$context_dir" >/dev/null
docker network create "$network" >/dev/null

docker run -d --name "$remote_container" \
  --network "$network" --network-alias remote \
  -e VEV_UDP_PORT_RANGE=61000 \
  "$image" >/dev/null

docker create --name "$local_container" \
  --network "$network" --network-alias local \
  "$image" sleep infinity >/dev/null
docker cp "$context_dir/id_ed25519" "$local_container:/home/test/.ssh/id_ed25519"
docker start "$local_container" >/dev/null
docker exec "$local_container" chown test:test /home/test/.ssh/id_ed25519
docker exec "$local_container" chmod 0600 /home/test/.ssh/id_ed25519

printf 'remote picker harness: waiting for SSH service\n'
for _ in $(seq 1 60); do
  if docker exec --user test -e HOME=/home/test "$local_container" ssh -o BatchMode=yes -o ConnectTimeout=1 test@remote true >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
if ! docker exec --user test -e HOME=/home/test "$local_container" ssh -o BatchMode=yes -o ConnectTimeout=2 test@remote true >/dev/null 2>&1; then
  echo 'remote picker harness: SSH service did not become ready' >&2
  exit 1
fi

printf 'remote picker harness: running acceptance checks\n'
docker exec --user test -e HOME=/home/test "$local_container" /usr/local/bin/remote-picker-harness
