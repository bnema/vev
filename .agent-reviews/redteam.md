# Remote down-session lifecycle refactor

## Decision

- Keep remote catalogue state as a dedicated string-backed JSON contract with `up`, `down`, and `broken` constants. Do not couple it to the binary `SessionState` enum. Require exact catalogue schema version 3 and cache version 3.
- Keep `RemoteSessionTarget.Stopped` on the wire. It describes the selected down-row resume intent across the transition to a live runtime; it is not a claim that the destination remains stopped.
- For structured remote picker rows, derive picker presentation state from `RemoteSessionTarget`; local rows retain their presentation flag. Remove pre-parity count-only catalogue rows and `ConnectOnly`.
- Rename the daemon's non-live durable registry representation to `inactiveSession`/`inactive`, then add narrow predicates for visibility, resumability, broken/degraded state, purge fencing, and restore waiting rather than one broad state check.
- Route remote down targets through exact inactive-record fencing. Recheck lifecycle and resumability after catalogue I/O before publishing a runtime.
- Support a healthy down record with no retained tabs as an exact default-tab resume target. A missing stopped selector is valid only when the authoritative inactive metadata has at most one default tab; it must not ambiguously select among multiple tabs.

## Resolved objections

- **Lifecycle TOCTOU:** use the expected inactive entry with the fenced creation helper and map stale-fence failures to `ErrNoSuchTarget`.
- **Target state transition:** document and test `down catalogue row -> exact same-lifecycle resume -> live runtime`; preserve the stopped intent through initial attach.
- **Typed-state compatibility:** retain JSON state tokens, bump the changed catalogue and cache contracts to version 3, and preserve ingress validation/error taxonomy.
- **Broken and obsolete rows:** map only exact-schema `down` entries to resumable targets; broken remains unavailable and older peers fail with a catalogue version mismatch.
- **Cross-host identity:** validate endpoint, name, lifecycle, and picker row identity together; never authorize a route from a display label.
- **Protocol layout:** retain the existing target boolean and wire layout, so no protocol bump is required.

## Accepted risks

- Renaming the inactive registry is mechanically broad inside the daemon package. It will be isolated from behavior changes where practical and verified with focused recovery tests plus the race suite.
- Older count-only catalogue peers and cache files are rejected rather than exposed as partial routes. Remote discovery starts empty when an obsolete local cache is encountered.

## Evidence

- `internal/usecase/daemon/remote_attach.go`: exact remote route and initial attach transition.
- `internal/usecase/daemon/session.go`: expected inactive lifecycle fence around catalogue I/O.
- `internal/usecase/daemon/coderabbit_regression_test.go`: existing same-name replacement race shape.
- `internal/usecase/daemon/remote_picker_test.go`: remote catalogue-to-picker-to-route seam.
- Critic and Go review passes: subagent `77a5f587-1e4f-40cb-b90b-44dde1598fbc`.
