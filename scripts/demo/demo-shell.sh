#!/usr/bin/env bash
set -euo pipefail
session=${VEV:-}
unset VEV

case "${VEV_DEMO_SCENE:-}" in
remote-deploys)
	work_marker=/tmp/vev-demo-work-shown
	if [[ $session != session=work,* || -e $work_marker ]]; then
		exec /bin/bash --noprofile --norc
	fi
	: >"$work_marker"
	printf '\033]0;deploys · remote\007'
	printf '\n\033[1;36mDeploy queue · remote\033[0m\n\n'
	printf '  \033[32m✓\033[0m api-gateway     production   3m ago\n'
	printf '  \033[32m✓\033[0m web-console     production   7m ago\n'
	printf '  \033[33m•\033[0m worker-pool     staging      running\n\n'
	printf 'Use the session picker to jump between this host and local work.\n\n'
	exec /bin/bash --noprofile --norc
	;;
remote-ticker)
	exec ticker
	;;
esac

exec /bin/bash --noprofile --norc
