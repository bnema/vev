# Releasing vev

Releases are cut by tagging: `git tag -s v0.X.Y -m v0.X.Y && git push origin v0.X.Y`.
`origin` is ermite (git.bnema.dev). Its `release` workflow cross-compiles
linux/amd64, linux/arm64 and darwin/arm64 (`CGO_ENABLED=0`), publishes the
release on git.bnema.dev with `checksums.txt`, and pushes the `vev-bin`
PKGBUILD to AUR (stable tags only: `skip_upload: auto` skips AUR for
`-rc.*`/`-alpha.*` prerelease tags). The GitHub mirror receives the tag but
publishes nothing.

## Checklist

1. main is green on ermite CI (`ermite run list vev`).
2. Tag and push. Watch: `ermite run list vev`, then `ermite run watch vev <n>`.
3. Verify the release: `ermite release view vev v0.X.Y` shows three archives,
   `checksums.txt` and the grouped changelog.
4. On Linux amd64, download the installer from the exact tag, inspect it,
   then run it for that same version:
   ```sh
   installer=$(mktemp)
   curl -fsSL https://git.bnema.dev/vev/raw/v0.X.Y/install.sh -o "$installer"
   less "$installer"
   VEV_VERSION=v0.X.Y sh "$installer"
   rm -f "$installer"
   ```
   Confirm `vev --version` prints `vev 0.X.Y (commit ..., built ...)`.
5. Stable tags only: confirm https://aur.archlinux.org/packages/vev-bin
   picked up the new version.

## If the release publishes but AUR fails

Do **not** delete or re-push the tag. The ermite release is the source of
truth; only the PKGBUILD push failed (usually SSH/key, secret `AUR_KEY`). Fix
the cause, then either:

- Re-run the failed job (`ermite run rerun vev <n> --failed`), which
  re-executes goreleaser idempotently for the same tag, or
- For manual recovery, clone
  `ssh://aur@aur.archlinux.org/vev-bin.git`, update its existing PKGBUILD for
  the exact tag using that release's `checksums.txt`, and regenerate
  `.SRCINFO` with `makepkg --printsrcinfo > .SRCINFO`. Review the diff, then
  commit and push only the AUR repository.

## If AUR publishes but the release fails

Do **not** delete or re-push the tag. Fix the workflow failure and rerun it
(`ermite run rerun vev <n> --failed`); if necessary, repair the release with
`ermite release upload vev v0.X.Y <files> --clobber`, then verify both
publication targets before proceeding.

## Version stamping

`vev --version` reads `internal/app.{version,commit,date}`, stamped via
ldflags in `.goreleaser.yaml`. Local builds report `0.1.0-dev`.
