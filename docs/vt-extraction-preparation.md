# VT extraction preparation checkpoint

The standalone module owns the frontend-neutral VT engine and ANSI renderer;
vev consumes the released `github.com/bnema/vev-vt v0.2.1` module. VT behavior,
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
- The standalone module owns the frontend-neutral Cell, Style, RGB, Frame,
  Damage, RuneWidth, VT parser, history, snapshots, and ANSI output packages.
- The cutover has no local VT, core, ANSI, or renderer implementation owner;
  vev resolves the released module through normal module download semantics.
- A standalone core-only frame consumer test builds ANSI output without
  importing the VT parser.

## Scope guard

The vev cutover changes only module ownership and import paths; no VT parser
behavior, VTH3/VEVS bytes, or wire/protocol definition changed. There is no
committed `replace` directive or compatibility owner in vev. Any unrelated
behavior, wire, or dependency change discovered while maintaining the standalone
module must be split into a separate plan.
