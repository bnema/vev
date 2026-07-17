# Releasing vev

Releases are cut by tagging: `git tag v0.X.Y && git push origin v0.X.Y`.
The `release` workflow cross-compiles linux/amd64 + darwin/arm64
(`CGO_ENABLED=0`), publishes a GitHub Release with `checksums.txt`, and pushes
the `vev-bin` PKGBUILD to AUR (stable tags only — `skip_upload: auto` skips
AUR for `-rc.*`/`-alpha.*` prerelease tags).

## Checklist

1. main is green on both CI legs (ubuntu, macos-14).
2. Tag and push. Watch: `gh run watch`.
3. Verify the release page: two archives + `checksums.txt` + grouped changelog.
4. `curl -fsSL https://raw.githubusercontent.com/bnema/vev/main/install.sh | sh`
   on a Linux box; confirm `vev --version` prints the tag.
5. Stable tags only: confirm https://aur.archlinux.org/packages/vev-bin
   picked up the new version.

## If GitHub publishes but AUR fails

Do **not** delete or re-push the tag. The GitHub release is the source of truth;
only the PKGBUILD push failed (usually SSH/key). Fix the cause, then either:

- Re-run the failed workflow job (`gh run rerun <run-id> --failed`), which
  re-executes goreleaser idempotently (`replace_existing_draft`, same tag), or
- Push the PKGBUILD by hand: `goreleaser release --clean --skip=validate`
  locally with `AUR_KEY` exported, or clone
  `ssh://aur@aur.archlinux.org/vev-bin.git` and commit the PKGBUILD generated
  under `dist/`.

## If AUR publishes but GitHub fails

Do **not** delete or re-push the tag. Fix the GitHub workflow failure and rerun
it (`gh run rerun <run-id> --failed`); if necessary, repair the GitHub Release
manually with the same tag's archives and `checksums.txt`, then verify both
publication targets before proceeding.

## Version stamping

`vev --version` reads `internal/app.{version,commit,date}`, stamped via
ldflags in `.goreleaser.yaml`. Local builds report `0.1.0-dev`.
