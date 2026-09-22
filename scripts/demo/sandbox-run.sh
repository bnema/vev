#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

compose=(docker compose -f scripts/demo/compose.yaml)
artifacts=${VEV_SANDBOX_ARTIFACTS:-scripts/demo/out/sandbox-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$artifacts"
cleanup() {
  if [[ -n "${client:-}" ]]; then
    docker exec "$client" sh -c 'find /tmp/vev-state -maxdepth 8 -type f -name "*.log" -print -exec tail -100 {} \;' >"$artifacts/client-state.log" 2>&1 || true
  fi
  "${compose[@]}" logs --no-color >"$artifacts/compose.log" 2>&1 || true
  "${compose[@]}" down -v --remove-orphans --timeout 5 >>"$artifacts/cleanup.log" 2>&1 || true
}
trap cleanup EXIT INT TERM

security=$(docker info --format '{{json .SecurityOptions}}')
case "$security" in *'name=rootless'*) ;; *) echo "error: Docker daemon is not rootless: $security" >&2; exit 1;; esac

go build -o build/vev .
sha256sum build/vev | tee "$artifacts/vev.sha256"
docker build --label vev.sandbox.binary-sha256="$(sha256sum build/vev | cut -d' ' -f1)" \
  -t "vev-sandbox:${VEV_SANDBOX_TAG:-dev}" -f scripts/demo/Dockerfile . \
  >"$artifacts/build.log" 2>&1
"${compose[@]}" up -d --force-recreate remote-a remote-b client >"$artifacts/up.log" 2>&1

client=$("${compose[@]}" ps -q client)
timeout 10 docker exec "$client" sh -c 'mkdir -m 700 -p "$XDG_CONFIG_HOME/vev"; umask 077; printf %s '\''{"marker":"vev.broker.offline/v1","registrations":[]}'\'' > "$XDG_CONFIG_HOME/vev/broker.json"'
for host in remote-a remote-b; do
  echo "readiness: $host" | tee -a "$artifacts/acceptance.log"
  ready=
  for _ in $(seq 1 40); do
    if timeout 3 docker exec "$client" ssh -o ConnectTimeout=2 "$host" true >/dev/null 2>&1; then ready=1; break; fi
    sleep 0.25
  done
  if [[ -z $ready ]]; then echo "error: SSH readiness timeout for $host" >&2; exit 1; fi
  if ! timeout 30 docker exec "$client" vev host add "$host" 2>&1 | tee -a "$artifacts/acceptance.log"; then
    timeout 5 docker exec "$client" vev _broker-production-serve >"$artifacts/direct-broker.log" 2>&1 || true
    exit 1
  fi
done

timeout 180 python3 scripts/demo/sandbox_acceptance.py "$client" | tee -a "$artifacts/acceptance.log"
echo "PASS artifacts=$artifacts"
