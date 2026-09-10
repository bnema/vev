# Client modal approval gate (P5.3, design-only)

Status: awaiting user decision. No implementation is authorized by this
document. This plan does not authorize additional subagent launches.

## Contracts presented

- P5.1 `docs/plans/client-presentation-lease.md`: five-state lease
  (`server-owned`, `acquiring`, `client-owned`, `releasing`, `aborted`) with
  per-state input/output/effect/resize/control/disconnect/cancel rules,
  exact generation-bound acquisition (no hidden frame ACKs), Kitty/cursor
  policy, local observation/receipt rules, exact proposed wire fields with
  protocol bump 43→44, and mapped tests including ACK saturation and failed
  flush.
- P5.2 `docs/plans/client-attached-interaction.md`: bounded
  `AttachedPickerModel` (facts + capabilities + revision, no credentials/
  env/content), actions with source authority + lifecycle pin + continuation
  ID + commit-time revalidation, server-side preview/inline validation,
  M1–M6 role table (transient home attachments explicitly retained),
  input-tail ownership, six walkthroughs (success, rejection, stale move
  source, validation retry, disconnected control, failed destination).

## Behavior deltas if approved

1. One pilot modal (recommended below) presents from the client under lease
   instead of from daemon overlay rendering; all other modals stay
   daemon-owned with unchanged precedence.
2. New wire messages for lease + attached model (exact fields in P5.1/P5.2),
   protocol version 43→44, strict mismatch fails closed.
3. Daemon suppresses ordinary paints during `client-owned` within the
   existing bounded window (no unbounded buffering); authoritative full
   paint + flush on release.
4. No change to: pane VT state, session/tab/pane mutation authority,
   key-router admission, config source of truth, persistence format,
   backdrop behavior, clipboard interception rules (extended, not relaxed).

## Recommended pilot modal

**Navigation picker** (pickerNavigate intent, `SelectNavigationTab` mode):

- Read-mostly: selection resolves to an existing attach target; no pane/tab
  mutation, unlike move selection (`commitMovePickerSelection` atomicity
  would need lease-aware two-phase care).
- Best-pinned preconditions: S3 transaction tests already pin source-held /
  destination-pending / retirement boundaries the pilot must preserve.
- Existing relay precedent: `inventoryRelay` admission/generation discipline
  transfers directly to model-revision discipline.
- Prompt validation stays fully server-side in the pilot (keystroke batches
  forwarded, closures never serialized); move intents stay daemon-rendered
  until the pilot proves the path.

## Tests required with the pilot (same layer as the behavior)

All P5.1 lease tests plus: model-revision admission/discarding, stale-target
rejection with prompt-input preservation, continuation-ID exactly-once
attribution, `unknown-outcome` surfacing without blind retry, transient-home
retention in hybrid UDP + stdio, exact-source restore on failed destination,
M1–M6 walkthroughs, full V1–V10 gates including `make remote-acceptance`.

## Performance implications

- Steady state: zero additional traffic (no lease when no modal open).
- During lease: one model publication per server-side change (same cost as
  today's overlay invalidation path), bounded previews (`ui.FrameView`
  only), no paint buffering past the existing unacked window.
- Resize during lease: single event forward + full paint at release; no
  paint storm (paints suppressed, not queued).
- Benchmark comparison required before approval of any expansion:
  `internal/adapters/ipc` + `internal/usecase/daemon` benches at baseline
  vs pilot tip under identical conditions.

## Unresolved risks

1. Input-tail edge cases across split UTF-8/ESC/Alt/mouse sequences at the
   exact open boundary; the ownership rule is specified but unproven.
2. Liveness tuning (proposed 2 missed intervals; 5s barrier/release
   timeouts): values are proposals, need live-terminal validation.
3. Kitty placement freeze vs live pane output under the lease: snapshot
   semantics specified, physical-rendering proof explicitly out of scope
   for ui-driver E2E.
4. Hybrid SSH close-and-dial interacting with lease abort: specified as
   abort + exact-source restore, unproven.
5. If the pilot needs disproportionate new infrastructure or cannot hold
   the mode invariants, the recommendation flips to **retain daemon
   presentation** (see below).

## Independent challenge

Requested only when the user authorizes it. This plan does not authorize
additional subagent launches; do not self-initiate a review agent.

## Decision

- [ ] Approve navigation-picker pilot: author a separate executor-ready
  pilot plan with exact APIs/files from the P5.1/P5.2 contracts, one modal
  end to end, daemon mutation/source/input authority retained, M1–M6
  applicable coverage before any other modal moves.
- [ ] Approve a different pilot modal (record which and why).
- [ ] Retain daemon ownership: document the decision; the P5.1/P5.2
  contracts remain as design records, no pilot plan follows. Do not force
  migration to satisfy the original ownership suggestion.

Exit: explicit approval of a bounded pilot plan, or a documented retain
decision. P6 expansion plans follow only after pilot evidence + renewed
approval.
