# P6 pilot (revised R1): client-owned navigation picker over inventory commit

Executor-ready plan, revision R1 after senior-engineer review (verdict
REVISE, 9 findings — all addressed below). Pilot scope is the navigation
session picker end to end; move intents, prompt validation, palette
command execution, and shortcut admission stay daemon-owned. No
compatibility layer, no persistence change, no new desktop feature.

Approval: user approved the P5.3 navigation-picker pilot on 2026-09-10
(`docs/plans/client-modal-approval-gate.md`). Scope approval only — the
R0 wire design was rejected in review; this R1 design is what the
executor implements. Independent challenge: not authorized by the plan;
do not launch review subagents.

## R0 review findings and R1 resolutions

1. **Raw Enter cannot commit with `rt.picker == nil`** (`picker.go:325-338`
   early return; `overlay_runtime.go:265-303` dispatch needs
   `pickerActive()`; bytes would fall to the key router). RESOLVED: the
   pilot does NOT route commits through `handlePickerInput`. Commit uses
   the existing inventory-selection path (below): a typed
   client→server selection message, daemon-side resolution, then the
   unchanged `switchToTargetForAttachment` handoff. `handlePickerInput`
   is untouched.
2. **No-version-bump fallback contradicts Hello validation**
   (`navigation.go:65-99` rejects unknown capability bits; a v43 daemon
   rejects a v43 Hello with bit `1<<3`). RESOLVED: protocol `Version`
   bumps 43→44 with ordinary strict-mismatch behavior. No compat layer.
3. **One terminal writer ≠ presentation ownership** (two renderer
   shadows diverge; `Model.Render` draws the inner picker, not the
   composed modal at `render_pipeline.go:291-305`; ACK/window and
   backdrop/cursor/Kitty/resize/release-paint correlation unspecified).
   RESOLVED: section 5 specifies the presentation contract explicitly
   (acquisition barrier, suppression with bounded window, modal
   composition ownership, backdrop/cursor/Kitty/resize rules, release
   full paint correlated to interaction close).
4. **Preview had no selection path or schema** (snapshot had no preview
   fields; `registerPreviewForSelection` reads `rt.picker`). RESOLVED:
   pilot reuses the EXISTING preview paths unchanged — local preview
   stays daemon-composed inside ordinary paints; remote preview stays
   `RemotePreview` (`protocol/preview.go` bounds). The client displays
   preview cells carried in ordinary output; no new preview message.
   Cursor movement is local-only; the daemon needs no per-movement
   notification because commit is key-based (finding 1 resolution).
5. **Interaction vs publication identity conflated.**
   RESOLVED: section 2 defines `PickerInteraction{ID}` (open/close
   namespace) separate from `PickerRevision` (monotonic model version).
6. **Input-tail/precedence lacked an admission mechanism.**
   RESOLVED: section 6 names the byte owner per state with a concrete
   transfer point (capability-gated daemon hold, mirroring the parked
   foreground pattern at `client.go:2649-2661`).
7. **Receipt integration omitted** (`UI.receipt` needs pending record +
   generation + committed boundary; `controlCh` vs `sendCh` ordering).
   RESOLVED: section 7 specifies action completion, queue choice
   (`controlCh`), fence ordering, and unknown-outcome behavior.
8. **Boundary OK, snapshot privacy claim wrong** (`SessionView` also has
   `RemoteKey`/`HideRemoteOrigin`/`RemoteDetail`; `RemoteTarget`/keys
   carry routing identity including endpoints; `picker.Target` is not a
   wire contract). RESOLVED: the attached model uses OPAQUE ROW KEYS
   with daemon-owned target resolution (section 3), mirroring the
   inventory `SourceKey`/`EntryKey` pattern. Daemon `pickerViews` row
   labels/sections keep endpoint-derived display text daemon-side; only
   opaque keys + display strings cross the wire. Bounds + sanitization
   specified for every string.
9. **Tests need failure orientation.** RESOLVED: section 8 is rewritten
   around non-initial-target commits, version/capability matrices,
   refresh/close/reopen races, precedence, fragmentation, disconnect at
   every boundary, and two-attachment independence.

Also corrected: usecase→usecase imports are legal
(`boundary_test.go:44`); section 0 states the full rule. And
`OpenSessionPicker` may send `NavigationOpenHomePicker` instead of
entering the local picker (`palette.go:1175`) — preserved and tested.

## 0. Baseline and work setup (no code yet)

- Worktree `.worktrees/test-client-ownership-e2e`, branch
  `docs/client-modal-contracts` (S5 tip `43c09fdf`), tree clean. Never
  edit `main`; layers via `gh stack add <exact-name>` from clean
  committed predecessors; `git commit -S`; `gh stack submit --auto`;
  verify Draft + base per PR.
- Baseline gates first: `go build -o vev .`, `go test .`, `make lint`,
  `go test ./internal/usecase/palette ./internal/usecase/picker
  ./internal/usecase/keys ./internal/protocol`, `make remote-acceptance`.
- Read first: lock-order notes at top of
  `internal/usecase/daemon/client.go`; `boundary_test.go` full layer map:
  usecase imports usecase/domain/ports/protocol/catalogue (never wire,
  adapters, app, persist, platform).

## 1. What moves and what stays (binding)

- ONLY the navigation picker (daemon intent `pickerNavigate`,
  `move_picker.go:43`; `SelectNavigationTab`, `picker/model.go:80-86`).
  Move intents, prompt, palette, notices, resize, copy stay
  daemon-rendered with unchanged `HandleInput` precedence
  (`overlay_runtime.go:265-303`).
- `internal/usecase/picker` state machine (`Model.Up/Down/Selected/
  Cursor/SelectedIndex/SelectNearestRow/Clone/Render`, `search.go`
  EnterSearch/ExitSearch/SearchActive/Query/InsertSearch/BackspaceSearch/
  ClearSearch/MatchCount/ReplaceFrom`) is reused in the client as-is
  (legal per boundary matrix; picker imports stdlib + vev-vt + domain +
  sibling ui only). No fork.
- Mutation authority stays daemon-side. Commit path is the inventory
  pattern, NOT `handlePickerInput`: client sends typed
  `PickerSelection` (opaque row key + interaction + revision);
  daemon resolves the key to a `picker.Target` under its own locks,
  then calls the UNCHANGED `switchToTargetForAttachment` handoff.
  `handlePickerInput`, `switchToTarget*`, guards unchanged.
- Preview: no new message. Local preview = daemon-composed cells in
  ordinary paints (existing pipeline); remote preview = existing
  `RemotePreview` flow. Client cursor moves never notify the daemon.

## 2. Protocol (Version 43→44; typed interaction messages)

- `internal/protocol/session.go`: `const Version uint16 = 44`.
  Strict equality everywhere (`daemon.go:1315`,
  `navigation_inventory.go:35`, `control.go:25`): old client ↔ new
  daemon and new client ↔ old daemon both fail closed with
  `ErrVersionMismatch` / `NavigationInventoryVersionMismatch`. No
  compat layer. Catalogue `ProtocolVersion` follows (`catalogue.go:248`
  emits `protocol.Version`; `catalogue.go:125` checks it).
- `internal/protocol/navigation.go`: add
  `NavigationCapabilityClientPicker NavigationCapabilities = 1 << 3`;
  extend `validNavigationCapabilities`. Advertising it in Hello under
  v44 is valid; under v43 it rejects as today (covered by matrix
  tests). `Welcome` gains NO acceptance field (same as the existing
  Inventory capability: advertisement-only, daemon gates on Hello
  bits — `inventoryDemandEnabled` precedent,
  `palette_inventory.go:17`).
- New typed messages in `internal/protocol` (new file
  `picker_interaction.go`), all with `Validate*` following the
  inventory validators' style:
  - `PickerOpen{InteractionID uint64}` client→server: request a
    client-picker interaction for the current attachment. Nonzero ID,
    client-generated (mirrors `beginPoll` query identities).
  - `PickerSnapshot{InteractionID, Revision uint64, Title string,
    Rows []PickerRow, Cursor PickerCursor}` server→client: full model
    replacement (no deltas — picker row order is semantically
    significant, never canonicalized). `PickerRow{Key, Display, Detail,
    Stopped, Kind}` with opaque `Key` (daemon-side `SessionView`→key
    map, same role as `navigationInventoryEntryKey`), display strings
    bounded (`NavigationInventoryMaxDisplayBytes` = 256) and sanitized
    (`validInventoryDisplay` rules). `PickerCursor{Key string, Index
    int}`. Bounds: ≤4096 rows (local inventory limit), total encoded
    ≤4 MiB (`navigationInventoryMaxEncodedBytes` precedent).
  - `PickerClose{InteractionID, Revision}` either direction: daemon
    authoritative close (superseded interaction, target vanished);
    client cancel/close notification.
  - `PickerSelection{CauseActionID, InteractionID, Revision, Key
    string}` client→server: commit request for the opaque row key at
    the displayed revision (mirrors `NavigationInventorySelection`
    shape: cause + interaction + publication generations + key).
  - `PickerFailure{CauseActionID, InteractionID, Key, Code}` server→
    client: `PickerStaleRevision`, `PickerUnknownKey`,
    `PickerRetiredTarget`, `PickerNavigationFailed` (mirrors
    `NavigationInventoryFailureCode`).
- `PickerOpen`/`PickerClose`/`PickerSelection` are `clientMessage()`;
  `PickerSnapshot`/`PickerClose`/`PickerFailure` are `serverMessage()`
  (`messages.go` closed sets). `PickerClose` both directions needs two
  entries like `RouteNavigationFailure` precedent (both lists).
- Wire: `picker_snapshot_wire.go` strict codecs with
  `payloadWriter/putString/putUint*`, `r.done()` discipline; version
  peeker NOT needed (Hello already negotiated 44). New server→client
  MsgType: next free ≥49 (14/24 stay reserved); new client→server
  MsgTypes: next free client-adjacent IDs per `frame.go` comment
  discipline. Document the allocation in `frame.go` comments.
  Byte-for-byte tests: round-trip, truncated-prefix,
  trailing-garbage, direction, bounds (mirror
  `navigation_inventory_test.go`).

## 3. Daemon interaction tracking (new file, overlay untouched)

New `internal/usecase/daemon/picker_interaction.go` (imports
domain/protocol/picker only; overlay structs gain two fields under
`pickerMu` mirroring `paletteInventoryOpen/Interaction` in
`overlay_runtime.go:59-63`):

- `pickerClientOpen bool`, `pickerClientInteraction uint64`,
  `pickerClientRevision uint64`, `pickerClientKeys map[string]picker.Target`
  (opaque key → resolved target; keys derived like
  `navigationInventoryEntryKey`: lifecycle hex + "/" + name).
- Open: `enterPicker` (`picker.go:59-62`) checks the Hello capability;
  when set AND navigate intent, sets interaction state and publishes
  `PickerSnapshot{Revision: 1}` via `effect.sendControl` (same guarded
  send boundary as `sendPaletteInventoryDemand`,
  `palette_inventory.go:59-79`) INSTEAD of `publishPicker` overlay
  install. `rt.picker` stays nil — deliberately, since commit no
  longer routes through `handlePickerInput` (finding 1).
- Refresh: any capture change while open → bump revision, republish
  full snapshot (same cost class as `invalidateRender`; no delta
  protocol). Sort toggle arrives as typed `PickerSortToggle`? NO —
  keep it simple: the daemon has no picker input path in this mode,
  so sort toggle is a client-local model operation over the snapshot
  rows (sort is presentation-only for navigate intent; commit uses
  opaque keys, order-independent). Document explicitly.
- Selection: on `PickerSelection`, validate interaction match +
  revision ≤ current + key in map (stale → `PickerFailure`
  StaleRevision/UnknownKey); resolve key → `picker.Target` under
  daemon locks (re-resolve lifecycle pin like `previewTarget`,
  `move_picker.go:121-152`: incarnation + `targetMatchesLifecycle`
  must still hold, else `PickerRetiredTarget`); then the UNCHANGED
  `switchToTargetForAttachment(effect, target,
  sessionHandoffGuard{closePicker:true, allowSamePeer:true}, ...)`
  handoff. Close the interaction after commit (daemon sends
  `PickerClose`), mirroring `closeExecutedPalette`.
- `x` kill: unavailable in client-picker mode (no key path to
  `killPickerTarget`); daemon picker retains it. Documented pilot gap.
- `OpenSessionPicker` home-picker distinction (`palette.go:1175`)
  preserved: `NavigationOpenHomePicker` directive path untouched.

## 4. Client loop (new files, existing seams)

New `internal/usecase/client/picker_interaction.go` (admission,
mirroring `inventoryRelay` open/interaction/publication discipline but
WITHOUT canonicalization — order significant) and
`picker_loop.go` (model ownership):

- `picker.New(snapshotRows→views, SelectionConfig{Mode:
  SelectNavigationTab, Current, Source:{}})` — the snapshot rows map
  1:1 to `picker.SessionView` construction fields the daemon used
  (daemon sends display-ready rows; client builds views with the same
  `New()`; cursor restored from `PickerCursor`).
- ui-driver ops map directly: `keys Up/Down/Enter/Escape` →
  `Up()/Down()/Selected()/ExitSearch()`; `text` → `EnterSearch()` +
  `InsertSearch`; leading `/` enters search (mirror
  `handlePickerInput:351` minimally); literal `s`/`x` in search are
  text, in normal mode `s` sorts locally, `x` yields local feedback
  "not available in client picker".
- Commit: Enter with `Selected() ok` → `PickerSelection{CauseActionID:
  ui action id, InteractionID, Revision: displayed, Key}` on
  `controlCh` (NOT `sendCh` — ordered control per finding 7, mirroring
  inventory publication sends at `client.go:2279`). Daemon resolves +
  commits; receipts flow through existing `UI.receipt`/`follow`/
  `destinationFull`.
- Cancel: Escape (no search) → `PickerClose` on `controlCh`; daemon
  closes interaction + repaints authoritatively (existing post-close
  repaint path).
- Late/foreign snapshots (interaction mismatch, revision ≤ current
  displayed, closed interaction) drop, mirroring `admitPaletteInventory
  Publication` (`palette_inventory.go:86-102`).

## 5. Presentation contract (explicit, per finding 3)

- Acquisition: daemon sends first `PickerSnapshot` (rev 1) only after
  draining admitted output to an explicit barrier epoch/state carried
  IN the snapshot (`BarrierEpoch/BarrierState` fields); client
  displays admitted output through the barrier via existing
  `outputApplyState` before rendering the picker. No hidden frame ACK.
- Suppression: while the interaction is open, the daemon suppresses
  ordinary paints within the EXISTING bounded unacked window
  (`MaxOutputWindow` = 8); no extra buffering, no window change.
  Saturation behavior = today's saturation behavior (drops/coalesces
  per existing pipeline), now with a test pinning it during an open
  picker interaction.
- Modal composition: the CLIENT composes the full modal — geometry
  via `pickerModal.Resolve(size)` equivalent? NO daemon import.
  Client computes placement from its own size using `picker.ChooseGeometry`
  + title/search-title rule mirrored from `render_pipeline.go:291-300`
  (title, else `pickerModal.Title`), themed styles from the client's
  already-known applied theme (client holds `appliedTheme` per
  attachment — reuse, don't invent). Cursor hidden while open, restored
  from daemon paint on close. Kitty placements frozen (no client
  uploads in pilot); backdrop = last daemon frame (preserved behavior,
  no frozen/cleared background).
- Resize: daemon arbitrates geometry as today; client recomputes
  layout from the new size; daemon includes current size epoch in
  refresh snapshots so a resize during interaction forces a revision
  bump (client never renders against a stale size).
- Release: daemon `PickerClose` + authoritative full paint + flush;
  client ACKs per existing output ACK (ACK follows successful flush
  only); normal input resumes after the flush. Failed flush → abort
  path: fresh full paint, held input replayed, no success receipt.

## 6. Input ownership transfer (explicit, per finding 6)

- Closed→open: the opening key is consumed daemon-side (daemon owns
  the shortcut, `keys.Router` unchanged); daemon's `enterPicker`
  decision point is the transfer: bytes AFTER the opening key in the
  same read are held daemon-side and DISCARDED for terminal purposes
  once the capability gate routes to client-picker (they were consumed
  as modal-open input). Daemon shortcut admission is preserved because
  the daemon makes the routing decision.
- Open: picker-bound bytes are consumed by the client loop, never
  forwarded; prompt/palette precedence is preserved because those
  overlays stay daemon-side and the daemon never opened them — but a
  daemon overlay opening MID-interaction (e.g. prompt) preempts: daemon
  sends `PickerClose` (superseded) and the client loop ends before the
  daemon overlay consumes further input. Specified transition, tested.
- Release/abort/disconnect: unadmitted tail bytes (never consumed by
  the picker model) replay to the daemon in order; consumed modal
  bytes never replay (prevents PTY delivery of picker commands);
  uncertain commit (disconnect after `PickerSelection` send, before
  receipt) resolves `unknown-outcome` via existing
  `UIActionOutcomeUnknown`, never blind-retried.
- Fragmentation: ui-driver ops are already tokenized; physical-input
  fragmentation (split ESC/UTF-8/mouse/paste) is handled by the
  EXISTING terminal input pump BEFORE the picker loop sees bytes —
  the loop consumes pump records, not raw reads. Stateful search
  distinctions (literal vs command `s`/`x`, search-exit vs picker-close
  Escape) implemented in the loop with tests.

## 7. Receipts and action completion (explicit, per finding 7)

- Local cursor/search/sort Vrenders complete LOCALLY: the ui-driver
  `keys`/`text` op returns `processed` once the model updates AND the
  composed frame flushes to the terminal (same flush rule as output
  ACK). Daemon evidence is NOT required for local presentation ops —
  and the harness MUST NOT wait for daemon state for them.
- Commit (`PickerSelection`) completes via the daemon receipt path:
  `controlCh` ordering, `Input.ActionID`-style cause attribution
  (`CauseActionID` bound at the single admitted send boundary like
  `sendControl`'s navigation binding,
  `attachment_effect_gate.go:211-228`), `UI.receipt` pending-record +
  generation + committed-boundary checks unchanged,
  `follow`/`destinationFull` for the handoff.
- Fence: commit sends `UIFence{ActionID}` BEFORE `PickerSelection`
  on `controlCh` (mirrors the input-pump batch fence at
  `client.go:3730`), so the daemon orders the commit after prior
  output. Failed flush / unknown outcome → `UIActionUnavailable` /
  `UIActionOutcomeUnknown` per existing codes, surfaced to ui-driver
  as today.

## 8. Tests (failure-oriented, each with the behavior)

- Wire (`picker_snapshot_wire_test.go`): round-trip, truncated-prefix,
  trailing-garbage, direction rejects, bounds (oversize title/rows/
  display strings, >4096 rows, >4 MiB encoded).
- Version/capability matrix: v43↔v44 both directions fail closed;
  v43 Hello + unknown capability bit rejects; v44 Hello without the
  picker bit gets the daemon picker (unchanged behavior pin).
- Daemon: snapshot rows == `newPickerModel` rows for navigate intent;
  `rt.picker == nil` in client mode WITH commit still working via
  typed selection (the finding-1 regression test);
  `TestPickerSelectionStaleRevision/UnknownKey/RetiredTarget`
  (lifecycle replaced between snapshot and commit);
  `TestMoveIntentNeverSnapshots`; mid-interaction daemon overlay
  preempts with `PickerClose`.
- Client: non-initial target commit sends exact opaque key +
  displayed revision, zero picker bytes toward the PTY; stale/future
  revision drops; refresh vs close/reopen resurrection refusal;
  duplicate commit dedup; literal-vs-command `s`/`x`; search-exit vs
  picker-close Escape; fragmented ESC/UTF-8/application-cursor/paste/
  mouse records; open-key + navigation + Enter in one pump batch.
- Previews: delayed/wrong-target preview cells discarded; malformed
  cells rejected; rapid changes bounded.
- Presentation: server output during local render doesn't corrupt;
  saturated ACK window during open interaction; failed write/flush
  restores cursor/mode; resize forces revision bump.
- Lifecycle: same-target select, stopped-session resume, vanished
  tab, unavailable remote row, destination failure → exact-source
  restore (S3 boundaries); transient-home cancel commits no history;
  direct-remote zero local dials; two attachments independent
  interactions; disconnect after selection-send → unknown-outcome,
  no blind retry; local observation completed for local ops.
- E2E (`acceptance.py`, table-driven × 5 transports):
  `client-picker-navigate` (open, rows asserted, cursor to
  NON-INITIAL row, Enter, committed attach with action ID +
  generation change + exact lifecycle/session/focus + observed
  output) + cancel sub-case. V10 green on every behavior layer + tip.
- Regression: V7 full `make test`, V1–V4, V6 lint, V8 build, V9
  benches (`internal/adapters/ipc` + `internal/usecase/daemon`,
  same conditions).

## 9. Layers (stacked, each builds + passes own gates)

- P6.0 `test/client-picker-interaction-wire`: protocol messages +
  validators + wire codecs + sessionwire cases + byte-level tests +
  version-bump (44) + matrix tests. Gates: V1 subset, V3, V4, V6, V8.
- P6.1 `feat/client-picker-daemon-interaction`: interaction tracking +
  capability gate + snapshot publish/refresh/close + typed selection
  resolve → unchanged handoff + tests. Gates: V2 daemon, V3, V6, V8.
- P6.2 `feat/client-picker-loop`: admission + loop + composition +
  render + commit/cancel + tail ownership + receipts + tests. Gates:
  V2 client, V4, V6, V8.
- P6.3 `test/client-picker-e2e`: harness scenario × 5 transports,
  V10 green, V7 full suite, V9 benches.
- Each: signed commit, `gh stack add`, Draft PR onto predecessor
  (P6.0 bottom onto S5 `docs/client-modal-contracts`), PR body with
  task IDs, HEAD, gates, gaps. No merges.

## 10. Non-goals (rejected in review)

Client VT emulator, persistent history, forward nav, desktop features,
compat layers, persistence changes, prompt/palette/notices/resize/copy
migration, shortcut-admission duplication, daemon overlay path removal,
`x` kill in client mode, P5.1 barrier/release message NAMES (ordering
guarantees ARE implemented per section 5 — names deferred), any new
client→server bytes beyond the typed interaction messages.
