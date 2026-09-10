# Client ownership acceptance coverage

Planning artifact for the client-ownership stack (P1.1). Maps the M1–M6 matrix
and cross-cutting cases to actual tests on `test/client-ownership-e2e` at
baseline `eca63822`. Gaps are explicit owning-package assignments, not shipped
behavior claims.

Evidence levels: **simulated** (in-process fixtures/mocks), **integration**
(real daemon + client over IPC/stdio in `go test`), **live E2E** (disposable
Docker fixtures driven by `vev --ui-driver` via `make remote-acceptance`).

## M1 — Local / IPC

Covered (integration, `internal/app`):

- Cold startup, existing attach: `TestEnsureDaemonSpawnsThenDials`,
  `TestEnsureDaemonReturnsExistingDaemon`, `TestIntegration_NamedSurvivesReattach`.
- Named/ephemeral lifecycle:
  `TestIntegration_EphemeralSurvivesDetachAndReattaches`,
  `TestIntegration_EphemeralNotListedAfterDaemonRestart`,
  `TestIntegration_NamedSurvivesReattach`,
  `TestIntegration_KillDaemonPreservesMultipleNamedSessions`.
- Picker/palette/prompt/confirmation parity:
  `TestIntegration_CommandPaletteCreatesTab`,
  `TestIntegration_CommandPaletteRenamesEphemeralSession`,
  `TestIntegration_AltCWithoutPaletteDoesNotCreateTab`,
  `TestHeadlessDriverUsesRealRunnerAndDaemon` (driver: palette open/wait/close,
  text/keys actions, capture).
- Peer independence / shared session views:
  `TestAcceptanceTwoLocalAttachmentsKeepViewsOverTransports`,
  `TestAcceptanceAttachedCommandUsesItsConnectionOnly`,
  `TestIntegration_TwoAttachmentsReceiveSharedMutationPTYOutput`.
- Unchanged local Ctrl+V: `TestRunLocalAttachDoesNotInterceptCtrlVEvenIfClipboardConfigured`.

Gap: visual confirmation parity across picker/palette/prompt variants is only
covered at integration level; no live-E2E local-IPC scenario exists yet.
Owner: P1.E2E (extend `navigation_repro.py` with a local-only scenario).

## M2 — Direct remote / UDP

Covered (simulated, `internal/usecase/client`,
`internal/adapters/dgram`, `internal/protocol/wire`):

- Loss/reorder/retransmit, congestion, ACK batching: `TestTransportFloodClassification`,
  `TestCumulativeACKCoalescesForBoundedDelay`, `TestCongestionControllerBoundsAndIntegerGrowth`,
  `TestHeartbeatContinuesDuringRetransmitStorm`, `TestHealthChecksContinueDuringRetransmitStorm`.
- Compression: `TestOutputCompression`,
  `TestCompressedOutputRejectsMalformedPayloads`.
- Resume/expiry: `TestExpiredResumeFallsBackToFreshExactAttach`,
  `TestResumeNeedsExactAttach`, `TestResumeCredentialIsInvalidAfterDaemonRestart`
  (daemon), `TestConcurrentResumeHasExactlyOneWinner` (daemon).

Covered (integration, `internal/app`):

- No local shadow on direct/picked remote paths:
  `TestAcceptanceRemoteDirectAndPickerUseOnlyRemoteTransports`
  (stdio fixture; asserts `localCalls == 0`).

Gaps:

- No live-E2E direct-remote UDP case (fresh client container, `--remote`, no
  local daemon). Owner: P1.E2E.
- Path-change behavior has fixture coverage but no assertion that a changed
  path preserves the session without a fresh attach. Owner:
  `internal/adapters/dgram` (P1.2 candidate).
- Exact fresh attach after credential expiry is covered at unit level only.
  Owner: P1.E2E.

## M3 — Direct remote / SSH stdio

Covered (simulated/integration):

- EOF, blocked send, redial: `TestAttachDaemonVanishedOnEOF`,
  `TestAttachRestoredOnRecvErrorMidStream`,
  `TestTransportObservabilitySSHStdioEOFEndsReceive`,
  `TestCloseInterruptsBlockedSend`,
  `TestRecvReportsSSHExitWhenProcessClosesBeforeFrame`.
- Command quoting/isolation: `TestBuildCommandForRemoteLaunchQuotesBinaryAndEnvironment`,
  `TestBuildCommandForIsolatedRemoteLaunchQuotesRootAndOwner`,
  `TestIsolatedLaunchScriptOwnsAndCleansFreshRoot`.
- No UDP-parking dependency: `TestCloseAndDialAttachTargetSkipsSamePeerSwitch`,
  `TestValidateAttachRequestNavigationTable`.

Gaps:

- No live-E2E explicit-stdio direct-remote case. Owner: P1.E2E.
- stdio redial after remote daemon restart is fixture-level only. Owner: P1.E2E.

## M4 — Hybrid / UDP

Covered (simulated, `internal/usecase/client`):

- Same-endpoint switch reuses transport: `TestHybridBackSessionSamePeerOfferReusesRemoteTransport`,
  `TestHybridPickerSameHostSwitchReusesRemoteTransport`.
- Different host / expired switch falls back to new dial:
  `TestHybridPickerDifferentHostFallsBackToNewRemoteDial`,
  `TestHybridPickerExpiredSwitchFallsBackToNewDial`.
- Prepare timeout closes retained transport:
  `TestHybridPickerPrepareResponseTimeoutClosesRetainedTransport`.
- Back resumes retained transport: `TestHybridPickerBackResumesRetainedRemoteTransport`.
- Local selection commits local route: `TestHybridPickerLocalSelectionCommitsLocalRoute`.
- Home open/cancel preserves route/MRU: `TestHomePickerPreservesActiveRouteSnapshot`,
  `TestHomePickerSubscribesToServingDaemonHistory`.
- Zero dials without source: `TestInventoryRelayZeroDialsWithoutSource`.
- Stale completion drops: `TestInventoryRelayStaleCompletionDropsWithoutTouchingSlot`.
- Recovery: `TestInventoryFailureRestoresLatestRemoteIdentityAfterResumeRejection`,
  `TestInventoryReopenRetainsPhysicalQuerySlotUntilCancellationCompletes`.
- Parked prepare/suspend/resume + expiry (daemon):
  `TestAttachmentLossParksOnlyThatAttachment`,
  `TestTwoAttachmentsParkIndependently`,
  `TestParkExpiryRemovesOnlyOneAttachment`.

Covered (live E2E): `navigation_repro.py` exercises local A → local B →
remote → exact local A return with committed action (hybrid-UDP shape).

Gaps:

- Missing/incompatible/restarted local source degrades dial-only source
  without daemon startup: partially covered by
  `TestDialOnlyLocalDialerNeverStartsDaemon`; a hybrid-specific case is
  missing. Owner: P1.2 (`internal/app`).
- Source failure and lease expiry during a live hybrid session have no
  live-E2E case. Owner: P1.E2E.

## M5 — Hybrid / SSH stdio

Covered (simulated): `TestCloseAndDialAttachTargetSkipsSamePeerSwitch`
(close/dial, not parking); `TestHybridPicker*` fallback tests are
transport-agnostic at the client layer.

Gaps:

- No live-E2E hybrid-stdio scenario (home open/Back preserves source/history
  across close/dial; failed destination restores exact source). Owner: P1.E2E.
- No case asserting "no parked-only assumptions" on the stdio path. Owner:
  P1.2 (`internal/usecase/client`).

## M6 — Cross-endpoint / mixed carriages including IPC

Covered (simulated/integration):

- Exact endpoint/lifecycle, aliases, stale/purged/renamed targets:
  `TestRouteLedgerKilledSelectionRetiresSourceAliasesAndCommitsPrevious`,
  `TestRouteRetirementThenRecreationDoesNotDuplicateHistory`,
  `TestRouteLedgerSeparatesRemoteDaemonOrigins`,
  `TestCatalogSessionsAsInfoInvariants`, `TestListAllSessionsInvariants`.
- Explicit tab selection: `TestRouteLedgerRemembersTabsIndependentlyPerExactRoute`,
  `TestRouteLedgerSamePeerHandoffRestoresRouteTab`.
- Create/restore failure: `TestRemoteCreationFailuresRestoreSourceAndReportCorrelation`,
  `TestNavigationTransitionInventoryFailure`,
  `TestRouteLedgerNavigateResumesThenFallsBackAndRestoresOrigin`.
- Retirement without resurrection:
  `TestRouteLedgerKilledSelectionWithoutPreviousRetiresActive`,
  `TestNavigationTransitionRetiresRecoveryOnce`,
  `TestCommittedIdentityDoesNotReassignHomeRoute`.
- Environment/CWD authority:
  `TestValidateAttachRequestRequiresDaemonOwnedEnvironmentForRemoteTarget`,
  `TestAttachHelloIncludesCompleteLocalEnvironment`,
  `TestIntegration_AttachEnvironmentRefreshesFuturePTYChildren`.
- Remote learning/host store: `TestRunAttachWithDepsAlwaysLearnsRemoteHost`,
  `TestAttachRememberRemoteHost`, `TestMergeRemoteHostsOrderAndSource`.

Covered (live E2E): exact-session return + client-qualified rows in
`navigation_repro.py`.

Gap: mixed-carriage cross-endpoint switch (e.g. IPC local → stdio remote → UDP
remote) in one live scenario. Owner: P1.E2E.

## Cross-cutting cases

- Output in flight / ACK window: `TestAttachmentOutputBuildsPipelinedDependencyChain`,
  `TestOutputStateAckRequiresCurrentEpoch`,
  `TestSenderBarrierFlushesPendingAck`, `TestBlockingRuntimeObserverDoesNotDelayTerminalFlushOrACK`.
- Partial write/flush failure: `TestAttachmentOutputFailedSendRetriesSnapshotWithoutAdvancing`,
  `TestAttachmentOutputFailedSendKeepsTextCursorAndGraphicsSpeculative`,
  `TestReconnectOverlayRedrawFailureDoesNotObserveOrAckOutput`.
- Resize/theme: `TestAttachForwardsResize`,
  `TestAttachmentOutputResizeFrameThenNoopAndDamageAreDifferential`
  (resize coverage in `TestAcceptanceTwoLocalAttachmentsKeepViewsOverTransports`).
- Kitty assets/placements across transitions: `TestKittyProbeUsesOneInputPumpAndReplaysUnrelatedInput`
  (simulated); no live-E2E physical-graphics claim. Gap owned by interactive
  checks, explicitly out of `--ui-driver` scope.
- Split UTF-8/ESC/Alt/mouse, bracketed paste, Ctrl+V:
  `TestAttachConsumesSplitMarkerAndCancelsAmbiguityDeadline`,
  `TestAttachStdinCoalescesSplitBracketedPaste`,
  `TestAttachStdinForwardsSGRMouseReportAsSingleFrame`,
  `TestPasteCoalescer*` suite, `TestClipboardIntercept*` suite.
- Multiple actions in one batch: `TestUIInputPumpBatchFenceOrdering`,
  `TestClipboardInterceptMultipleCtrlVInOneChunk`.
- Peer geometry/theme/view differences: covered via two-attachment acceptance
  tests; geometry arbitration by latest valid claim asserted in
  `TestAcceptanceTwoLocalAttachmentsKeepViewsOverTransports`.
- Pane/tab move or deletion during selection/confirmation:
  `TestPickerDeleteDoesNotDeleteSourceAfterInitiatorReplacement`
  (daemon effect gate).
- Late completions after close/reopen/switch/reconnect:
  `TestPaletteGenerationLateDrainBoundaryExcludesStaleOSC`,
  `TestPaletteGenerationStaleTimerAndMarkerAreNoOps`,
  `TestAttachLateDrainCannotFinalizeReplacementEarly`,
  `TestUILateHandoffAndUnrelatedReplacement`.
- No replay/duplicate delivery: `TestTerminalInputPumpCancellation*` suite,
  `TestSamePeerGateDiscardsOnlyRetiredAutomatedInput`,
  `TestUIActionHistoryEvictsOnlyOnAcceptance`.
- Receipts at committed flush boundaries: `TestUIHandoffRequiresDestinationFullAndCompleteDispatch`,
  `TestUILocalCompletionUsesAppliedCallbackBoundary`,
  `TestUIRunnerAppliesFenceReceiptAfterMetadataPublication`,
  `TestUIActionTimeoutRetainsExactReceiptBoundary`,
  `TestUIRunnerSamePeerActionWaitsForDestinationFull`.
- Unknown outcome stays unknown: `outcome_unknown` path asserted via
  `TestUILateHandoffAndUnrelatedReplacement` and driver contract
  (`docs/ui-driver.md`); no blind retry.
- Strict version mismatch: `TestAttachVersionMismatch`,
  `TestTransportRoundTripAndVersionMismatchFrame`, `TestProtocolVersion`.
- ACK saturation / failed flush mapping for the future lease contract: no
  dedicated saturation test. Owner: P1.2 (`internal/usecase/client`
  ACK-queue tests exist: `ack_queue_test.go`; extend with saturation case).

## Baseline results (P1.1 entry, updated S1 exit)

- V1 (`go test ./internal/usecase/palette ./internal/usecase/picker
  ./internal/usecase/prompt ./internal/usecase/keys ./internal/protocol`):
  pass on baseline worktree.
- V2 (`go test -race ./internal/usecase/client ./internal/usecase/daemon
  ./internal/app`): pass on S1 worktree (daemon suite ~142s).
- V3 (`go test ./internal/protocol/... ./internal/adapters/sessionwire
  ./internal/adapters/ipc ./internal/adapters/dgram
  ./internal/adapters/sshstdio`): pass.
- V4 (`go test .`): pass, including architecture boundaries.
- V5 (`make mocks`): not required; no port changes in S1.
- V6 (`make lint`): pass (goimports clean, go vet clean).
- V7 (`make test`, full race suite): not run; V2+V3+V4 cover the touched
  packages with race detection. Full `make test` remains mandatory before
  any behavior-changing layer.
- V8 (`go build -o vev .`): pass.
- V9 (benchmarks): `pkg/renderer` and `pkg/vt` no longer exist in the tree
  (the plan's V9 package list is stale); ran the closest valid subset
  `./internal/adapters/ipc ./internal/usecase/daemon`, both ok. No
  baseline-vs-changed comparison was possible because S1 adds no
  production code paths; comparison becomes meaningful at S2.
- V10 (`make remote-acceptance`): pass. Baseline matrix: original
  `navigation_repro.py` plus `acceptance.py` scenarios
  `local-palette-cycle`, `direct-remote-ephemeral@udp`,
  `direct-remote-ephemeral@stdio`, `hybrid-exact-return@udp`,
  `hybrid-exact-return@stdio` (all PASS, ~48s including image build).
- Symbol drift check: all plan anchors verified except that
  `enterHomePicker`, `runTransientHomePicker`, `handleParkedPicker` are
  closures inside `internal/usecase/client/client.go` (not top-level
  functions). No semantic drift; paths in the plan remain correct.
