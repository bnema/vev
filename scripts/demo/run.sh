#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

TAPE="${1:-demo.tape}"
COMPOSE_FILE="scripts/demo/compose.demo.yaml"
install -d -m 0777 scripts/demo/out
# Demo containers use a rootless subuid mapping, so reset generated state
# from the same container identity rather than relying on host ownership.
docker run --rm --entrypoint sh -v "$PWD/scripts/demo/out:/out" vev-demo -c 'rm -rf /out/local && mkdir -m 0777 /out/local'

compose() {
	docker compose -f "$COMPOSE_FILE" "$@"
}

cleanup() {
	compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose up -d remote-a

ready=""
for _ in $(seq 60); do
	if compose run --rm --entrypoint ssh client remote-a true >/dev/null 2>&1; then
		ready=yes
		break
	fi
	sleep 0.5
done
if [ -z "$ready" ]; then
	echo "error: sshd on the remote container never accepted a connection" >&2
	compose logs remote-a >&2 || true
	exit 1
fi

start_remote_session() {
	local name=$1 scene=$2
	# `vev new` needs a sized terminal. Attach briefly inside a pseudo-terminal;
	# the named session survives when the timeout ends the client.
	compose exec -u demo -T remote-a env TERM=xterm-256color VEV_DEMO_SCENE="$scene" SHELL=/bin/bash \
		timeout 3 script -qec "stty cols 120 rows 40; SHELL=/usr/local/bin/demo-shell vev new $name" /dev/null >/dev/null 2>&1 || true
	if ! compose exec -u demo -T remote-a vev ls | grep -q "^$name"; then
		echo "error: remote session '$name' was not retained" >&2
		compose exec -u demo -T remote-a vev ls >&2 || true
		return 1
	fi
}

start_remote_session work remote-deploys
start_remote_session ticker remote-ticker

compose run --rm --entrypoint vev client host add remote-a
compose run --rm client "/tape/$TAPE"
