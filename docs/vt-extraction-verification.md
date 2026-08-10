# VT extraction verification evidence

## Release

- Module: `github.com/bnema/vev-vt v0.2.1`
- Module checksum: `h1:sb5XWVVIgiYd82KSYb3gHvkHVt9r5oRoEK0hqO1B+Aw=`
- Go module checksum: `h1:QdV0QI7vUnlAF+7+CiPhThHDpDSkxMEZmCYl/Eo+xCA=`
- Standalone release commit: `7bd1a3c4`
- vev cutover commit: `3fd4a02d`

## Acceptance evidence

- vev: `make test`, `make lint`, `go mod verify`, and clean-checkout module
  resolution pass without a `replace` directive.
- Standalone module: tests, race tests, `go vet`, `golangci-lint`, external
  consumer smoke, import-boundary checks, and signed release tag pass.
- Fuzz smoke passes for VT input, ANSI rendering, VTH3 decoding, and VEVS v4
  restore decoding. Seeds include escape sequences, Unicode, clipboard input,
  malformed history, and the VEVS restore golden.
- VTH3 and VEVS fixtures remain byte-compatible. VEVS framing and session policy
  remain vev-owned.
- History/codec benchmark allocations and bytes are unchanged in the selected
  pre/post comparison. Representative daemon frame benchmarks remained within
  normal run variance; no output-byte regression was observed.

## Diff classification

- **Extraction:** standalone module release, vev module dependency cutover, and
  removal of the in-repository VT/core/ANSI owners.
- **Tests and fixtures:** external ownership contracts, VTH3 golden vectors,
  VEVS restore fixtures, and fuzz smoke coverage.
- **Documentation:** ownership, callback, borrowing, release, and verification
  records.
- **CI:** standalone formatting, lint, race, fuzz, benchmark, and consumer jobs.

## Risks and follow-up

- A broad baseline run including `BenchmarkScreenResizeReflowViewport` still
  exposes the pre-existing `invalid history row ID` benchmark panic. The
  selected extraction comparison excludes that benchmark rather than masking it.
- There is no dedicated standalone `RuneWidth` benchmark; width behavior is
  covered by public contract tests and the screen/parser benchmark suite.
- The outer VEVS codec intentionally remains in vev and must not move into the
  standalone module.
