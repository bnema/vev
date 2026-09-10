# Navigation transaction boundaries (P3.1)

Planning artifact for the client-ownership stack. Records the existing
source-held, destination-pending, destination-published, recovery, and
retirement boundaries for each navigation transaction variant on
`refactor/client-navigation-boundaries`. No route semantics changed, no
bookkeeping moved.

Five transaction variants exist. Same-peer, parked, cross-endpoint, resume,
and recovery stay distinct; they are not merged under a generic
dial-and-commit abstraction.

## Shared vocabulary

- **Source-held**: the client's live attachment (`dialer`, `attemptRequest`,
  `resumeToken`) plus the ledger's active route. Input the user produces
  while source-held belongs to the source until a destination publishes.
- **Destination-pending**: a selected route or in-band offer the daemon has
  not committed. Raw human input is held, never dropped; queued automation
  at or below the retired UI fence is discarded.
- **Destination-published**: the daemon committed the target
  (`CommittedRouteIdentity`) and, for UI-observed actions, published a full
  destination `Output`. Only then do history and receipts move.
- **Recovery**: the retained return route (`navigationTransition` for
  creation/recent/inventory, `killedSelection` for session kill). Recovery
  is separate from recent-history storage.
- **Retirement**: exact identities leave the ledger only through
  `retireRoute` (daemon-observed deletion with subscription proof),
  `retireKilled`/`commitKilledTransition` (kill recovery), eviction of
  non-referenced entries past the 20-entry cap, or in-place rename.

## Same-peer switch (in-band, one transport)

- Offer: daemon `AttachTarget` with `SamePeer` set, same endpoint, exact
  target (`client.go` AttachTarget case). The client derives the request
  from ledger tab memory via `samePeerHandoff` and sends
  `SamePeerSwitchRequest` on the control channel; the request bypasses held
  input (`TestSamePeerSwitchControlBypassesHeldInput`).
- Pending: `samePeerSwitchPending` + `samePeerInputGate.setPaused(true)`.
  Raw `Input`/`ImagePush` arriving at the sender while paused is held in
  `heldInput` and released to the same transport on unpause. Queued
  automation at or below the retired UI action is discarded instead
  (`discardRetiredUI`, `TestSamePeerGateDiscardsOnlyRetiredAutomatedInput`).
- Commit: daemon `CommittedRouteIdentity` for the offered target mutates the
  active route in place via `commitCommittedIdentity` (rename-safe: same
  lifecycle keeps process-local identity, so the obsolete name cannot
  survive as separate history). Tab memory updates without rotating
  identity or staling snapshots
  (`TestRouteLedgerRemembersTabsIndependentlyPerExactRoute`).
- Precommit rejection: `SamePeerSwitchFailure` with the matching request ID
  fails the UI handoff, clears the pending switch, and unpauses the gate.
  Held raw input is released to the source transport; no history is
  committed and no replacement transport is dialed
  (`TestSamePeerSwitchFailurePreservesSourceRouteHistory`, new in P3.1).
  A mismatched target fails the attach instead of committing a surprise
  destination.
- History boundary: the rename-preserving in-place path commits at identity
  publication; the UI receipt for the causing action still requires a full
  destination `Output` (`TestUIRunnerSamePeerActionWaitsForDestinationFull`,
  `TestUIHandoffRequiresDestinationFullAndCompleteDispatch`).

## Parked route (hybrid UDP home picker)

- The home picker borrows a transient local attachment for rendering only;
  transient attempts never commit (`transientPicker` skips all three commit
  sites in `attachAttempt.run`).
- Opening or cancelling the picker commits no destination and reorders no
  history (`TestHomePickerPreservesActiveRouteSnapshot`).
- Prepare/switch/resume keep the source transport parked daemon-side; resume
  requires an authoritative full paint, never a replay of one-shot effects
  (S2 evidence: `TestParkedRouteSuppressesQueuedClipboardForward`).
- SSH stdio never parks: close-and-dial instead
  (`TestCloseAndDialAttachTargetSkipsSamePeerSwitch`,
  `TestHybridStdioHomeOpenSkipsParking`).

## Cross-endpoint handoff (picker/discovery target)

- Transport authority crosses the `attachHandoff` boundary: the bound
  source route is the sole authority afterward; the connection-relative
  `SamePeer` hint is consumed before the target crosses it.
- Creation failures (handoff resolution or target dial) restore the source
  route and report a correlated `SessionCreationFailure`
  (`TestRemoteCreationFailuresRestoreSourceAndReportCorrelation`,
  `TestRouteNavigationReturnsToCreatedRemoteSession`).
- Recent-route selection validates against the latest snapshot
  (key/generation/snapshot-generation triple); a concurrent commit rejects
  with `errRouteStaleSelection` and leaves history alone
  (`TestRouteLedgerTransitionRejectsConcurrentCommit`,
  `TestRouteLedgerNavigationRejectsStaleActionsAndActiveNoOp`).
- A committed target that differs from the selection fails with
  `errRouteTargetChanged` and restores the prior live route
  (`TestRouteLedgerNavigateIsTransactionalOnFailureOrTargetChange`).
- Same-origin policy: local handoffs downgrade to client-owned environment
  and clear daemon navigation capabilities; remote handoffs preserve the
  serving daemon's policy, explicit tab, and request environment
  (`TestRouteLedgerSamePeerHandoffPreservesRemoteDaemonOwnedPolicy`).

## Resume and credentials

- Resume is attempted before exact attach only when the selected record
  carries a `resumeToken`; `errRouteResumeUnavailable` falls back to exact
  attach (`TestRouteLedgerNavigateResumesThenFallsBackAndRestoresOrigin`).
- Resume credentials belong to attachments, not history: committing a route
  with a token clears that token from older same-origin routes, and a
  same-peer move carries the credential to the new session so old history
  attaches exactly
  (`TestRouteLedgerNavigationAfterSamePeerSwitchDoesNotResumePreviousSessionCredential`).
- Expired resume falls back to a fresh exact attach; concurrent resume has
  exactly one winner (daemon-side).

## Recovery and retirement

- `navigationTransition` retains one return route per pending operation
  (creation with request ID, recent, inventory with serving selection).
  Both settle outcomes retire the same pending record; only the ledger
  commits successful destinations (`TestNavigationTransitionRetiresRecoveryOnce`).
- Creation/inventory recovery is separate from recent-history storage: a
  failed creation restores the prior route and reports
  `SessionCreationFailure`; a failed inventory handoff restores the serving
  route and reports `NavigationInventoryNavigationFailed` to the restored
  daemon (`TestNavigationTransitionInventoryFailure`,
  `TestInventoryFailureRestoresLatestRemoteIdentityAfterResumeRejection`).
- Session kill captures `killedSelection` (newest non-alias route) and
  retires all routes identifying the killed endpoint authority and
  lifecycle; origin is intentionally not part of alias equivalence.
  Without a previous route the ledger retires the active route and exits
  (`TestRouteLedgerKilledSelectionRetiresSourceAliasesAndCommitsPrevious`,
  `TestRouteLedgerKilledSelectionWithoutPreviousRetiresActive`,
  `TestKilledSessionReturnsToPreviousLocalRoute`).
- Daemon-observed deletion retires only with subscription proof and never
  the active route; recreation after retirement does not duplicate history,
  and retired exact identities cannot return through aliases
  (`TestRouteRetirementThenRecreationDoesNotDuplicateHistory`).
- Cursor (`RoutePosition`) updates mutate tab memory on the active exact
  route only; delayed frames from a prior transition are valid wire
  messages that mutate nothing.

## P3.2 extraction gate: documented no-op

The existing ledger is retained unchanged. No concrete duplicated
bookkeeping with identical invariants was demonstrated:

- The production commit paths (`commitTransition` for recent/creation
  selection, `commitKilledTransition` for kill recovery,
  `commitCommittedIdentity` for daemon-local moves, `commitAttach` for
  initial attach) each carry distinct preconditions and recovery owners.
  Merging them under a generic dial-and-commit abstraction would erase the
  boundaries recorded above and is explicitly rejected.
- `routeLedger.navigate` with its `routeTransitionConnector` callback set
  has no production callers; it exists only as the unit-level harness that
  pins the resume-before-attach, target-match, and restore-on-failure
  invariants (`TestRouteLedgerNavigate*`,
  `TestRouteLedgerNavigationAfterSamePeerSwitchDoesNotResumePreviousSessionCredential`).
  It duplicates no production bookkeeping and its tests are the mapped
  evidence for the cross-endpoint variant, so it stays.
- Discovery-cycle bookkeeping already lives under a lock separate from
  route history by design, so stale discovery cannot clear or commit
  unrelated navigation history (`TestRouteLedgerDiscoveryCyclesAreIndependent`).

No source changes beyond the P3.1 characterization test above.
