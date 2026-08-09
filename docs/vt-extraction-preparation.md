# VT extraction preparation checkpoint

The extraction boundary is ready for standalone ownership. Compatibility
coverage and the first dependency-inversion step are complete; VT behavior,
renderer output, wire bytes, transport, ACK/rebase, and cursor-tail policy remain
unchanged.

## Evidence

- The feature worktree was created from a clean checkout.
- `make test` passes (`go test ./... -race`).
- `make lint` passes (`goimports` has no changes and `go vet ./...` passes).
- External-package contract tests pass under `-race` for parsing, callbacks,
  borrowing, snapshots, history views, stable chunk identity, and single-owner
  usage.
- VTH3 golden vectors round-trip byte-for-byte across empty, plain,
  continuation, styled/RGB, bounded, and line-bound cases. Truncation,
  corruption, invalid fields, and trailing bytes are rejected.
- The VEVS v4 restore envelope golden decodes through the existing restore path,
  including sealed history, tail history, and recovery transcript blobs.
  Truncated, corrupted, trailing, and malformed nested VT payloads are rejected.
- Representative VT/history/snapshot benchmarks pass; benchmark output is
  retained in the implementation run rather than committed as machine-specific
  data.
- `git diff --check` passes.
- `pkg/vtcore` owns the frontend-neutral Cell, Style, RGB, Frame, Damage, and
  RuneWidth model; `pkg/vt` has no production dependency on the ANSI package.
- `pkg/ansi` owns ANSI output planning and encoding and consumes `vtcore`; the
  old `pkg/renderer` path forwards to it without a second implementation owner.
- A core-only frame consumer test builds ANSI output without importing the VT
  parser.

## Scope guard

The production changes in this checkpoint only move model ownership behind an
explicit core package and adapt ANSI delta commits to the core Frame API. No VT
parser behavior, VTH3/VEVS bytes, or wire/protocol definition changed. The
compatibility import path is a temporary cutover bridge; the standalone release
must not retain it or a local module replacement. Any unrelated behavior, wire,
or dependency change discovered while extracting the standalone module must be
split into a separate plan.
