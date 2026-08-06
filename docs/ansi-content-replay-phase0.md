# Phase 0: ANSI content replay gate

The test-only prototype in `internal/usecase/daemon/ansi_content_replay_phase0_test.go` records the current feasibility boundary for remote content:

- ANSI produced by `pkg/renderer` is replayed into a private `pkg/vt.Screen`.
- The screen has nil response, bell, notification, progress, and clipboard callbacks, so replay cannot emit remote host-side effects.
- Full and incremental output, styles, wide-cell continuation, cursor state, and split CSI/OSC writes are captured as cells and cursor metadata.
- The captured grid can be handed to a local renderer for composition; the remote ANSI stream is not passed to that renderer or a transport.
- A test-only ordered-frame model requires metadata before the first full content frame, contiguous state-bearing deltas, and base-zero full resets. Gaps and malformed resets are rejected.

This is feasibility evidence only. It does not change the production renderer, protocol, layout, or attach flow. The remaining gate is to specify and integrate a production remote metadata/content ownership contract around this boundary; no compatibility path is implied by Phase 0.
