# Phase 0: ANSI content replay gate

The former test-only prototype in `internal/usecase/daemon/ansi_content_replay_phase0_test.go` recorded the feasibility boundary for remote content. That prototype was removed when direct remote ownership became the production boundary; the constraints it established remain:

- ANSI produced by `pkg/renderer` is replayed into a private `pkg/vt.Screen`.
- The screen has nil response, bell, notification, progress, and clipboard callbacks, so replay cannot emit remote host-side effects.
- Full and incremental output, styles, wide-cell continuation, cursor state, and split CSI/OSC writes are captured as cells and cursor metadata.
- The captured grid can be handed to a local renderer for composition; the remote ANSI stream is not passed to that renderer or a transport.
- A test-only ordered-frame model requires metadata before the first full content frame, contiguous state-bearing deltas, and base-zero full resets. Gaps and malformed resets are rejected.

This remains feasibility evidence only. Direct remote attachments keep the remote daemon as the owner of the renderer, protocol, layout, and attach flow; no ANSI replay or local shadow-session compatibility path is implied.
