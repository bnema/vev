#!/bin/sh
# vev installer — Linux x86_64 and macOS arm64.
# Usage: curl -fsSL https://raw.githubusercontent.com/bnema/vev/main/install.sh | sh
# Env: VEV_VERSION=vX.Y.Z to pin a version (default: latest release).
set -eu

REPO="bnema/vev"
VERSION="${VEV_VERSION:-latest}"

info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# --- Detect platform -------------------------------------------------------
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$OS/$ARCH" in
  linux/x86_64|linux/amd64) TARBALL="vev_linux_x86_64.tar.gz" ;;
  darwin/arm64)             TARBALL="vev_darwin_arm64.tar.gz" ;;
  *) err "unsupported platform: $OS/$ARCH (supported: linux/x86_64, darwin/arm64)" ;;
esac

# --- Pick install dir ------------------------------------------------------
if [ -d "$HOME/.local/bin" ]; then
  INSTALL_DIR="$HOME/.local/bin"
else
  INSTALL_DIR="/usr/local/bin"
fi

# --- Resolve version -------------------------------------------------------
if [ "$VERSION" = "latest" ]; then
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name"' | head -1 | cut -d '"' -f 4)"
  [ -n "$VERSION" ] || err "could not resolve the latest release tag"
fi
info "installing vev ${VERSION} (${OS}/${ARCH}) to ${INSTALL_DIR}"

# --- Download + verify -----------------------------------------------------
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
curl -fsSL -o "$TMP_DIR/$TARBALL" "$BASE_URL/$TARBALL" \
  || err "download failed: $BASE_URL/$TARBALL"
curl -fsSL -o "$TMP_DIR/checksums.txt" "$BASE_URL/checksums.txt" \
  || err "download failed: $BASE_URL/checksums.txt"

cd "$TMP_DIR"
EXPECTED="$(grep "  $TARBALL\$" checksums.txt | cut -d ' ' -f 1)"
[ -n "$EXPECTED" ] || err "$TARBALL not found in checksums.txt"
if command -v sha256sum >/dev/null 2>&1; then
  ACTUAL="$(sha256sum "$TARBALL" | cut -d ' ' -f 1)"
else
  ACTUAL="$(shasum -a 256 "$TARBALL" | cut -d ' ' -f 1)"
fi
[ "$EXPECTED" = "$ACTUAL" ] || err "checksum mismatch for $TARBALL (expected $EXPECTED, got $ACTUAL)"
info "checksum verified"

# --- Extract + install -----------------------------------------------------
tar -xzf "$TARBALL"

do_install() {
  # $1 = src, $2 = dst dir
  if [ -w "$2" ]; then
    install -m 755 "$1" "$2/"
  else
    info "escalating with sudo to write into $2"
    sudo install -m 755 "$1" "$2/"
  fi
}

mkdir -p "$INSTALL_DIR" 2>/dev/null || true
do_install vev "$INSTALL_DIR"
# Default status-bar segments; vev's default config references them by name.
do_install scripts/vev-bar-top-right "$INSTALL_DIR"
do_install scripts/vev-bar-bottom-right "$INSTALL_DIR"

info "installed: $INSTALL_DIR/vev"
case ":$PATH:" in
  *":$INSTALL_DIR:"*) "$INSTALL_DIR/vev" --version ;;
  *) info "note: $INSTALL_DIR is not in your PATH" ;;
esac
