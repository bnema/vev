#!/usr/bin/env bash
# Headless Kitty-graphics repro: NeferWL headless -> kitty -> (optional) isolated vev -> kimg.py
# usage: [PRE=n] [PAD=n] [WHEEL=n] run.sh <label> <vev-binary|none> [kimg case ids...]
#   PRE   blank lines printed before the images (pushes them to the bottom)
#   PAD   lines printed after the images (pushes them into scrollback)
#   WHEEL wheel-up steps for the scrolled shot (default 2)
#   OUTDIR output root (default $TMPDIR/vev-kitty-graphics)
# Requires neferwl, kitty, jq, python3 + Pillow. Never touches the live desktop or vev daemon.
# Shots: $OUTDIR/out/<label>/1-after.png, 2-scrolled.png, 3-back.png
set -u
S=$(cd "$(dirname "$0")" && pwd)
W=${OUTDIR:-${TMPDIR:-/tmp}/vev-kitty-graphics}
usage() { echo "usage: [PRE=n] [PAD=n] [WHEEL=n] $0 <label> <vev-binary|none> [kimg case ids...]" >&2; exit 2; }
[ $# -ge 2 ] || usage
LABEL=$1; VEVBIN=$2; shift 2
# LABEL becomes path components under $W and is removed with rm -rf.
[[ $LABEL =~ ^[A-Za-z0-9._-]+$ ]] && [ "$LABEL" != . ] && [ "$LABEL" != .. ] || usage
# shq single-quotes a value for the generated POSIX sh script.
shq() { printf "'%s'" "$(printf %s "$1" | sed "s/'/'\\\\''/g")"; }
CASES="$*"
OUT=$W/out/$LABEL; rm -rf "$OUT"; mkdir -p "$OUT"
ID=vevimg
neferwl session rm $ID >/dev/null 2>&1
printf 'animations = off\n' >"$OUT/neferwl.conf"
READY=$(neferwl session new --id $ID --config "$OUT/neferwl.conf" --size 1280x800 --ttl 5m -- --no-xwayland --no-terminal) || exit 1
WL=$(printf '%s' "$READY" | jq -r .wayland)

# vev isolation. A long XDG_RUNTIME_DIR makes vev fall back to /tmp/vev-<uid>-<sha256(dir)>: clean it too.
ROOT=$W/env-$LABEL
HASHDIR=/tmp/vev-$(id -u)-$(printf %s "$ROOT/rt/vev" | sha256sum | cut -d" " -f1)
rm -rf "$ROOT" "$HASHDIR"; mkdir -p "$ROOT"/{rt,state,config,data,cache}; chmod 700 "$ROOT"/rt
cat >"$OUT/inner.sh" <<EOF
#!/bin/sh
export XDG_RUNTIME_DIR=$(shq "$ROOT/rt") XDG_STATE_HOME=$(shq "$ROOT/state") XDG_CONFIG_HOME=$(shq "$ROOT/config") XDG_DATA_HOME=$(shq "$ROOT/data") XDG_CACHE_HOME=$(shq "$ROOT/cache")
unset VEV NEFERWL_SESSION
export SHELL=/bin/sh PS1='$ ' ENV=
if [ $(shq "$VEVBIN") = none ]; then exec /bin/sh; fi
exec $(shq "$VEVBIN")
EOF
chmod +x "$OUT/inner.sh"
neferwl -s $ID 'do' spawn --until 'window-mapped app_id=kt' -- kitty --class kt -o font_size=11 -o remember_window_size=no -o confirm_os_window_close=0 "$OUT/inner.sh" >/dev/null
sleep 4
shot() { neferwl -s $ID shot --output HEADLESS-1 --inline | jq -r '.shots[0].png' | base64 -d >"$OUT/$1.png"; }
wheel() { WAYLAND_DISPLAY=$WL neferwl pointer --output HEADLESS-1 abs 0.5 0.5 scroll "$1" "$2" >/dev/null; }
neferwl -s $ID type "clear; PRE=${PRE:-0} PAD=${PAD:-0} python3 $S/kimg.py $CASES" >/dev/null
neferwl -s $ID key Return >/dev/null
sleep 3
shot 1-after
wheel up "${WHEEL:-2}"; sleep 1.5
shot 2-scrolled
wheel down 20; sleep 1.5
shot 3-back
if [ "$VEVBIN" != none ]; then
  XDG_RUNTIME_DIR=$ROOT/rt XDG_STATE_HOME=$ROOT/state XDG_CONFIG_HOME=$ROOT/config "$VEVBIN" kill --all >/dev/null 2>&1
fi
neferwl session rm $ID >/dev/null 2>&1
rm -rf "$HASHDIR"
ls "$OUT"/*.png
