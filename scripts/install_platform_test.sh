#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH='' cd "$(dirname "$0")/.." && pwd)
TEST_DIR=$(mktemp -d)
trap 'rm -rf "$TEST_DIR"' EXIT HUP INT TERM

FAKE_BIN="$TEST_DIR/bin"
mkdir -p "$FAKE_BIN"

cat >"$FAKE_BIN/uname" <<'EOF'
#!/bin/sh
case "${1:-}" in
  -s) printf '%s\n' "$FAKE_OS" ;;
  -m) printf '%s\n' "$FAKE_ARCH" ;;
  *) exit 2 ;;
esac
EOF

cat >"$FAKE_BIN/curl" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$CURL_LOG"
exit 97
EOF

chmod +x "$FAKE_BIN/uname" "$FAKE_BIN/curl"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_mapping() {
  os=$1
  arch=$2
  tarball=$3
  curl_log="$TEST_DIR/curl-$os-$arch.log"
  output="$TEST_DIR/output-$os-$arch.log"

  if FAKE_OS=$os FAKE_ARCH=$arch CURL_LOG=$curl_log HOME="$TEST_DIR/home" \
    PATH="$FAKE_BIN:$PATH" VEV_VERSION=v0.0.0 \
    sh "$ROOT_DIR/install.sh" >"$output" 2>&1; then
    fail "$os/$arch unexpectedly completed installation"
  fi

  expected_url="https://github.com/bnema/vev/releases/download/v0.0.0/$tarball"
  [ -f "$curl_log" ] || fail "$os/$arch did not attempt a download"
  grep -F "$expected_url" "$curl_log" >/dev/null \
    || fail "$os/$arch did not select $tarball"
  [ "$(wc -l <"$curl_log" | tr -d ' ')" = 1 ] \
    || fail "$os/$arch attempted more than the archive download"
}

assert_mapping Linux x86_64 vev_linux_x86_64.tar.gz
assert_mapping Linux amd64 vev_linux_x86_64.tar.gz
assert_mapping Linux aarch64 vev_linux_arm64.tar.gz
assert_mapping Linux arm64 vev_linux_arm64.tar.gz
assert_mapping Darwin arm64 vev_darwin_arm64.tar.gz

curl_log="$TEST_DIR/curl-Linux-armv7l.log"
output="$TEST_DIR/output-Linux-armv7l.log"
if FAKE_OS=Linux FAKE_ARCH=armv7l CURL_LOG=$curl_log HOME="$TEST_DIR/home" \
  PATH="$FAKE_BIN:$PATH" VEV_VERSION=v0.0.0 \
  sh "$ROOT_DIR/install.sh" >"$output" 2>&1; then
  fail "Linux/armv7l unexpectedly succeeded"
fi
grep -F "unsupported platform: linux/armv7l" "$output" >/dev/null \
  || fail "Linux/armv7l did not report an unsupported platform"
grep -F "supported: linux/x86_64, linux/arm64, darwin/arm64" "$output" >/dev/null \
  || fail "Linux/armv7l did not list the canonical supported platforms"
[ ! -e "$curl_log" ] || fail "Linux/armv7l invoked curl before rejection"

printf 'installer platform tests passed\n'
