#!/bin/sh
# vev installer — Linux x86_64/arm64 and macOS arm64.
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
  linux/x86_64|linux/amd64)  TARBALL="vev_linux_x86_64.tar.gz" ;;
  linux/aarch64|linux/arm64) TARBALL="vev_linux_arm64.tar.gz" ;;
  darwin/arm64)              TARBALL="vev_darwin_arm64.tar.gz" ;;
  *) err "unsupported platform: $OS/$ARCH (supported: linux/x86_64, linux/arm64, darwin/arm64)" ;;
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

# Keep this list in the archive's required installation order.  All payloads
# are checked and staged before a live file is replaced.
PAYLOADS="vev scripts/vev-bar-top-right scripts/vev-bar-bottom-right"
TRANSACTION=".vev-install-$$"

path_exists() {
  [ -e "$1" ] || [ -L "$1" ]
}

install_path() {
  # Run every destination mutation through the same privilege path.
  if [ -w "$INSTALL_DIR" ]; then
    "$@"
  else
    sudo "$@"
  fi
}

ensure_install_dir() {
  [ -d "$INSTALL_DIR" ] && return 0

  if [ -w "$(dirname "$INSTALL_DIR")" ]; then
    mkdir -p "$INSTALL_DIR" || err "cannot create $INSTALL_DIR"
  else
    info "escalating with sudo to create $INSTALL_DIR"
    sudo mkdir -p "$INSTALL_DIR" || err "cannot create $INSTALL_DIR"
  fi
}

stage_path() {
  printf '%s/%s-%s.new\n' "$INSTALL_DIR" "$TRANSACTION" "${1##*/}"
}

backup_path() {
  printf '%s/%s-%s.bak\n' "$INSTALL_DIR" "$TRANSACTION" "${1##*/}"
}

remove_destination_file() {
  install_path rm -f "$1"
}

cleanup_staging() {
  for payload in $PAYLOADS; do
    stage="$(stage_path "$payload")"
    remove_destination_file "$stage" || return 1
  done
}

rollback_install() {
  rollback_ok=true

  for payload in $PAYLOADS; do
    name="${payload##*/}"
    target="$INSTALL_DIR/$name"
    stage="$(stage_path "$payload")"
    backup="$(backup_path "$payload")"

    if path_exists "$backup"; then
      # A prior file was moved aside: remove the new one and put it back.
      remove_destination_file "$target" || rollback_ok=false
      install_path mv "$backup" "$target" || rollback_ok=false
    elif ! path_exists "$stage"; then
      # No backup means the original was absent. A missing stage was moved
      # into place, so restore that original absence.
      remove_destination_file "$target" || rollback_ok=false
    fi
    remove_destination_file "$stage" || rollback_ok=false
  done

  [ "$rollback_ok" = true ]
}

preflight_payloads() {
  for payload in $PAYLOADS; do
    [ -f "$payload" ] && [ -r "$payload" ] \
      || err "archive is missing readable payload: $payload"
  done
}

# Do not create or alter the destination until every archive payload exists.
preflight_payloads
ensure_install_dir

# Stage every payload in the destination first, so each later mv is a
# same-filesystem replacement. A staging failure leaves live files untouched.
for source in $PAYLOADS; do
  stage="$(stage_path "$source")"
  if ! install_path install -m 755 "$source" "$stage"; then
    cleanup_staging || err "failed to stage payloads and clean staging files"
    err "failed to stage payload: $source"
  fi
done

# Replace each live file only after all three staged copies are ready. Existing
# files are retained as backups until the full transaction succeeds.
for source in $PAYLOADS; do
  name="${source##*/}"
  target="$INSTALL_DIR/$name"
  stage="$(stage_path "$source")"
  backup="$(backup_path "$source")"

  if path_exists "$target" && ! install_path mv "$target" "$backup"; then
    rollback_install || err "failed to preserve $target and roll back installation"
    err "failed to preserve existing file: $target"
  fi
  if ! install_path mv "$stage" "$target"; then
    rollback_install || err "failed to replace $target and roll back installation"
    err "failed to install payload: $source"
  fi
done

for source in $PAYLOADS; do
  backup="$(backup_path "$source")"
  remove_destination_file "$backup" \
    || err "installed payloads but failed to clean backup: $backup"
done

info "installed: $INSTALL_DIR/vev"
case ":${PATH:-}:" in
  *":$INSTALL_DIR:"*) "$INSTALL_DIR/vev" --version ;;
  *) info "note: $INSTALL_DIR is not in your PATH" ;;
esac
