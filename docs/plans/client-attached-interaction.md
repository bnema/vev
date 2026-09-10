# Client attached interaction (P5.2, design-only)

Status: proposed. No production edits accompany this document. This contract
exercises the P5.1 lease with one picker flow covering navigation, move
selection, and prompt validation. It does not authorize migrating all three
at once; the pilot migrates one modal end to end only after approval.

## Anchors (existing behavior, unchanged by this contract)

- Sanitized inventory API stays intact: `internal/protocol/navigation_inventory.go`
  request/response/selection with `Validate*`; client admission via
  `inventoryRelay.preparePublication` + `validateSelection`
  (`internal/usecase/client/navigation_inventory.go`). Sanitized discovery
  rows are not an attached picker/action model.
- Picker model: `picker.Target` (`internal/usecase/picker/model.go:141`:
  session/incarnation/name, remote key/target/host, tab ID/index, stopped,
  `ExpectedCreatedAt` lifecycle pin), `SelectionMode` (navigate / move-pane /
  move-tab), bounded `Preview = ui.FrameView`.
- Daemon move admission: `enterPickerForIntent` (capability gate
  `yieldsMoves`, `errNoMoveDestination`), `commitMovePickerSelection`
  (`internal/usecase/daemon/move_picker.go`), revalidated at commit.
- Prompt validation continuations cannot be serialized:
  `handlePromptInput` (`internal/usecase/daemon/prompt.go`) runs
  `promptSubmit`/`promptTransitionSubmit` closures inline; validation errors
  stay in the modal (`SetError`), non-validation errors report via
  `reportAttachmentError`.
- Precedence preserved: prompt → palette → picker → notices → resize → copy
  (`overlay_runtime.go`). Daemon shortcut admission retained initially
  (`keys.Router` stays the single admission path).
- Pending input tail: ESC/Alt/mouse/paste state and the remainder of the
  input batch that opens a modal belong to the modal that opens; see
  ownership rule below.

## Attached model (bounded, distinct from inventory rows)

`AttachedPickerModel` (name pending approval) contains only what the leased
picker needs to present and act, without endpoint credentials, environment,
or unnecessary pane content:

- Tab/pane/view facts: for each row, session display name + kind
  (local/remote/ephemeral), tab ID + label, focused-pane ID, stopped flag,
  `ExpectedCreatedAt` lifecycle pin where the row came from a snapshot
  outside the daemon.
- Operation capabilities per row: `canSelect` (navigate),
  `canAcceptMove` (mirrors `CannotAcceptMoves`), `canPreview` (bounded frame
  view available), move-source eligibility (`yieldsMoves`).
- Interaction revision: model generation bumped on every server-side change;
  preview generations per row; the client renders only the latest admitted
  revision and discards older ones.
- Explicitly excluded: endpoint credentials, session environment/CWD,
  full pane content (preview is the existing bounded `ui.FrameView` only),
  daemon socket/transport internals.

## Actions

Every action carries: source authority (serving session + attachment
capability, mirroring `movePaneRequest`/`moveTabRequest` fields),
lifecycle/stable object identity (session/incarnation/created-at pin, tab
and pane stable IDs), attachment + interaction revision, permitted operation,
continuation identity, cancellation, and outcome semantics:

- Permitted operations: `navigate`, `move-pane`, `move-tab`,
  `preview-request`, `preview-cancel`, `prompt-submit`, `modal-cancel`.
- Continuation identity: each mutating action carries a client-generated
  continuation ID; the server's receipt echoes it so exactly-once outcome
  attribution holds without blind retry.
- Revalidation immediately before mutation: the server re-resolves source
  authority, lifecycle pin (`ExpectedCreatedAt`), destination capability,
  and interaction revision at commit time. A row that changed since the
  model revision is `stale-target`, never force-committed.
- Validation error: prompt submit failures return inline (`SetError`
  equivalent) with the modal kept open; the validation error text is bounded
  and sanitized like bar output.
- Stale-target: selection against a superseded model revision, or a target
  whose lifecycle pin no longer matches, is rejected; the server publishes a
  fresh model revision and the client re-renders without losing typed prompt
  input.
- Unknown-outcome: if the server cannot prove commit or non-commit (e.g.
  crash between mutation and receipt), it reports `unknown-outcome` with the
  continuation ID; the client surfaces "unknown, verify before retrying"
  and MUST NOT blind-retry the mutation.

## Preview and inline validation

- Preview generation/cancellation: `preview-request` carries row identity +
  model revision; the server answers with a bounded `ui.FrameView` tagged
  with the same revision, or `preview-stale` if the row moved on. The client
  cancels outstanding previews on selection change; late previews for older
  revisions are discarded.
- Prompt inline validation runs server-side (closures cannot be serialized):
  each keystroke batch forwards under the lease; the server responds with
  the updated prompt view (value echo, error text, submit-readiness) tagged
  with the interaction revision. No validation logic moves to the client.

## Roles across M1–M6

Catalog owner, resolver, serving session, and presentation owner stay
independent, mirroring the hybrid invariant:

- Direct remote (M2/M3): zero local dependency. Catalog owner = resolver =
  serving session = the remote daemon; presentation owner = the local client
  under lease. No local control dials, no local attachment.
- Hybrid (M4/M5): catalog source (dial-only local inventory) and serving
  session (remote) stay distinct; the transient home attachment question is
  decided explicitly: **retain transient home attachments** (no change to
  UDP parking vs SSH close-and-dial), because the lease acquisition already
  binds exact attachment/transport generations and the home flow's
  open/cancel-must-not-commit rule (S3) is load-bearing. Changing it needs
  separate approval.
- Cross-endpoint (M6): lifecycle/exact-identity rules from S3 apply
  unchanged (retired identities cannot return through aliases; explicit tab
  selection; environment/CWD authority stays with the serving session).

## Input-tail ownership

- The tail of the input batch that opens the modal (bytes after the
  opening key, pending ESC/Alt/mouse/paste state) is owned by the opening
  modal: forwarded to it under the lease, never replayed to the underlying
  terminal, never double-admitted.
- On modal cancel/close, unclaimed tail bytes are discarded (they were
  consumed as modal input); only bytes the modal explicitly declines
  (e.g. a cancelled prompt with unconsumed paste) return to the normal pump
  in original order.

## Walkthroughs

1. **Success**: lease acquired → model revision N admitted → user navigates
   (preview req/cancel within revision N) → action with continuation C,
   revision N → server revalidates (authority, lifecycle, revision) →
   commits → receipt `{continuation: C, outcome: committed, boundary:
   epoch/state}` → client completes handoff (`destinationFull` semantics) →
   release with authoritative full paint + flush → `server-owned`.
2. **Rejection**: server revalidation fails (e.g. destination lost
   `canAcceptMove`) → receipt `{continuation: C, outcome: rejected, reason}`
   → modal stays open on fresh revision N+1, typed prompt input preserved.
3. **Stale move source**: source pane/tab moved or closed between model
   publication and commit → `stale-target` → fresh model published; no
   partial move (commit is atomic per existing `commitMovePickerSelection`).
4. **Validation retry**: prompt submit → inline validation error → modal
   open with error text, revision bumped, value preserved → corrected submit
   → committed.
5. **Disconnected control source**: transport drops mid-interaction → lease
   aborts (P5.1) → held input replayed to daemon in order → daemon resumes
   with full paint; no action receipt is fabricated; in-flight continuation
   C resolves as `unknown-outcome` on reconnect, surfaced for manual verify.
6. **Failed destination**: commit target unreachable (remote gone, session
   dead) → exact-source restore per S3 boundaries (source input/history
   preserved, no history commit) → modal closed or re-anchored to a live
   source, never stranded on a dead route.

## Matrix gaps

None unexplained: every M1–M6 mode maps to the role table above; the
unsupported-raw-input / physical-rendering limits recorded in P1.E2E still
apply (ui-driver coverage is not claimed for physical Kitty rendering or
real desktop effects). Copy/search/notices/resize input interactions stay in
every expansion test even while those modes remain daemon-owned.

## Stop condition

P5.2 ends at this document. The exact pilot modal (one of navigation picker,
move selection, or prompt validation) is chosen at the P5.3 gate, not here.
