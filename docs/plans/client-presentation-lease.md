# Client presentation lease (P5.1, design-only)

Status: proposed. No production edits accompany this document. Do not
implement the lease; stop for design review after this document.

## Anchors (existing behavior, unchanged by this contract)

- Ordered output with bounded ACK window: `internal/usecase/daemon/attachment_output.go`
  (`maxUnackedOutputStates`, cumulative ACKs advance the paint window).
  Client admission gates: `outputApplyState.next` in
  `internal/usecase/client/output_state.go` (epoch/state/view-revision/
  lifecycle checks, ordered side effects cross replay reset gates but never
  advance committed state).
- Parked-route lease as the proven ordering precedent:
  `internal/usecase/daemon/parked_route.go` (`armParkedRoute`,
  `prepareParkedRoute`/`consumeParkedRoute`/`beginParkedRouteSwitch`/
  `rejectParkedRouteSwitch`, `releaseParkedOutputLocked`,
  `MsgParkedRouteRequest` 35 / `MsgParkedRouteResponse` 36). This contract
  reuses its concepts (lease identity + generation + transport match,
  prepare/suspend/resume, expiry), not its capability or expiry semantics
  wholesale.
- UI fence for action-scoped barriers: `renderCoordinator`
  (`internal/usecase/daemon/ui_fence.go`: `registerUIFence`, `captureUIFence`,
  `needsUIFence`, `retireUIFence`, `finishUIFence`/`failUIFence`).
- Receipt semantics: `UI.receipt` (`internal/usecase/client/ui_actions.go`)
  admits only committed output boundaries; ambiguous delivery stays unknown,
  never blind-retried. Handoff completion: `destinationFull`
  (`internal/usecase/client/ui_handoff.go`) runs only after the handoff owner
  validates.
- Modal precedence (input and render): `overlay_runtime.go`
  (`handleOverlayInput` dispatch order prompt → palette → picker → notices →
  resize → copy; `overlayRenderSnapshot` composition). Initially retained.
- Backdrop behavior is preserved: no frozen/cleared background is approved
  by this contract; a visible alternative needs separate approval.

## State table

States: `server-owned`, `acquiring`, `client-owned`, `releasing`, `aborted`.

For every state the table gives: admissible input, output, effects, resize,
control/liveness, disconnect and cancellation events; owner; completion
evidence; timeout; recovery.

| State | Admissible input | Output | Effects | Resize / control / liveness | Disconnect / cancel | Owner | Completion evidence | Timeout | Recovery |
|---|---|---|---|---|---|---|---|---|---|
| `server-owned` | All input to daemon (unchanged current behavior) | Ordinary server paints (unchanged) | All daemon effects (incl. clipboard, OSC 52) | Resize arbitrated by daemon; liveness via existing attach protocol | Existing reconnect semantics | Daemon | n/a (steady state) | None | n/a |
| `acquiring` | Input held: neither daemon modal admission nor client modal admission; held bytes retained with pending-sequence ownership, never dropped, never double-admitted | Ordered barrier: daemon drains admitted output up to the barrier epoch/state, then stops ordinary paints; barrier epoch/state published to client | Cleanup effects only (e.g. parked-output release); clipboard effects held | Resize held and replayed at grant; liveness heartbeats continue; no navigation/switch admitted | Disconnect aborts to `aborted`; explicit cancel aborts | Daemon until barrier acknowledged | Client acknowledges barrier epoch/state AND displays admitted output up to the barrier (`outputApplyState` initialized) | Barrier timeout (proposed 5s, mirroring command result deadline scale); on expiry abort | Return to `server-owned`; held input replayed to daemon in original order |
| `client-owned` | Client modal owns its input tail (see P5.2); ordinary terminal input still forwarded per existing pump | Ordinary server paints suppressed; no unbounded buffering (server drops non-essential paints past a bounded window rather than queueing indefinitely) | Cleanup vs clipboard classified separately: cleanup effects execute; clipboard effects require the modal's foreground generation (P2.1 rule extended) | Resize forwarded as events to the client modal; terminal cursor/mode policy owned by modal while held; Kitty assets: retain uploaded assets, no re-upload, placements frozen at acquisition snapshot | Disconnect aborts; cancel releases | Client (presentation + input for the leased modal only) | Continuous: client holds exact attachment/transport/interaction generations | Liveness: missed client liveness (proposed 2 consecutive intervals) aborts; no indefinite hold | Abort path below |
| `releasing` | New input held (same hold rule as `acquiring`) | Authoritative full paint required: daemon publishes full frame + view revision; normal input resumes only after successful flush of that paint | Held clipboard effects resolve against the restored foreground owner or are discarded with `ErrNoClipboardImage` semantics | Resize applied before the full paint so the paint matches live geometry | Disconnect aborts; cancel completes release early only after the full paint flushes | Daemon | Client ACKs the release full paint (successful flush boundary, same rule as output ACK: ACK follows successful flush only); then release completes | Release timeout (proposed 5s); on expiry abort | Forced return to `server-owned` with authoritative full paint |
| `aborted` | Held input replayed to daemon in original order | Daemon resumes ordinary paints with a fresh full paint | No effect replay: effects admitted during `acquiring`/`client-owned` are never re-executed; ambiguous deliveries stay unknown | Cursor/mode reset to daemon authority; Kitty placements from the last daemon snapshot (no client uploads retained) | Terminal state of the lease; a new acquisition starts a new lease identity | Daemon | Fresh full paint flushed | None | Steady `server-owned` |

## Acquisition binding (no hidden frame ACKs)

Acquisition binds exact `attachment identity + transport snapshot +
interaction generation` in the lease request. The ordered barrier is an
explicit epoch/state pair, not an implicit frame ACK: the client must display
admitted output up to the barrier before the grant, and the output window
never fills indefinitely (bounded exactly like the existing unacked paint
window; saturation behavior specified per mapped test below, including ACK
saturation and failed flush cases).

## Kitty / cursor / terminal policy during `client-owned`

- Cleanup: placements existing at acquisition stay; client uploads during
  the lease are namespaced to the interaction generation and deleted on
  release/abort.
- Retention: no re-upload of daemon assets; no daemon re-upload of
  client-namespaced assets.
- Cursor/mode: the modal owns cursor visibility/shape and pending
  ESC/Alt/mouse/paste state for its input tail; on release the daemon
  reasserts from its authoritative render.

## Local observation and receipts

- Local UI observation context during the lease reports the leased modal
  identity, attachment/transport/interaction generations, and lease state;
  it never claims daemon-modal state.
- Local action receipts describe the leased modal's committed presentation
  boundary; daemon action receipts keep their existing meaning. The two are
  never conflated in one receipt.

## Proposed typed fields (exact, pending approval)

- `PresentationLeaseRequest{LeaseID, AttachmentID, Transport, InteractionGeneration, ModalKind}`
  client→server. New `MsgType` required (next free server-adjacent ID to be
  assigned at implementation time); protocol `Version` bump 43→44 at that
  time. Encoded bounds: fixed-size IDs + small enum, ≤64 bytes payload.
- `PresentationLeaseBarrier{Epoch, State, ViewRevision, ViewPublication}`
  server→client; `PresentationLeaseGrant{LeaseID, Barrier}` server→client
  after drain.
- `PresentationLeaseRelease{LeaseID, Reason}` either direction;
  `PresentationLeaseAbort{LeaseID, Reason, FailedFlush bool}` server→client.
- Failure statuses: `stale-generation`, `unknown-lease`, `barrier-timeout`,
  `release-timeout`, `transport-changed`, `flush-failed`.
- Cancellation: dropping the attachment, navigation/switch, or explicit
  cancel moves `acquiring`/`client-owned`/`releasing` to `aborted` with
  `FailedFlush` set when the release paint did not flush.

## Tests mapped (to be added only after approval)

- `TestLeaseAcquisitionBindsExactGenerations` (sessionwire): mismatched
  attachment/transport/interaction generation rejected with
  `stale-generation`; no barrier published.
- `TestLeaseBarrierDrainsAndDisplays` (client): grant only after barrier
  epoch/state displayed; held input retained in order.
- `TestLeaseAckSaturationDoesNotFillWindow` (daemon + client): full ACK
  window during `client-owned` drops non-essential paints, bounded memory,
  no deadlock.
- `TestLeaseFailedFlushAborts` (daemon + client): failed release-paint flush
  yields `flush-failed` abort, never a success receipt.
- `TestLeaseNoEffectReplay` (daemon): effects admitted during the lease never
  re-execute after abort; ambiguous stays unknown.
- `TestLeaseDisconnectAbortsAndRepaints` (daemon + client): disconnect in any
  transient state ends in `server-owned` with a fresh flushed full paint.
- `TestLeaseKittyNamespaceCleanup` (daemon): client-namespaced assets
  deleted on release/abort; daemon assets untouched.
- Wire tests byte-for-byte for every new message: truncated-prefix and
  trailing-garbage coverage, direction checks, bound checks, version-bump
  test (strict mismatch fails closed).

## Stop condition

P5.1 ends at this document. Every transition above needs its mapped
deterministic test before any implementation. Do not implement the lease.
