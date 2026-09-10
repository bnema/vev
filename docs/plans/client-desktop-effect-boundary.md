# Desktop-effect policy boundary (P2.2)

Planning artifact for the client-ownership stack. Records the existing
vev-initiated versus application-originated desktop effects, their
execution site, and their fanout/suppression rules on
`refactor/client-workstation-effects`. No policy change, no new wire
message, no new desktop capability, no local image interception.

## Effect classes

### Vev-initiated: remote clipboard image push (Ctrl+V)

- Trigger: a Ctrl+V keypress (`0x16`) on a **remote** attach. Local attach
  never intercepts, even with a reader configured
  (`TestRunLocalAttachDoesNotInterceptCtrlVEvenIfClipboardConfigured`).
- Execution site: client-side `clipboardIntercept`
  (`internal/usecase/client/clipboard_intercept.go`), between the theme
  scanner and the paste coalescer. The read runs on the stdin pump goroutine
  through `ports.ClipboardReader`.
- Foreground ownership (P2.1): the intercept carries the foreground context
  (`clipboardIntercept.ctx`, wired from `stdinPump.ctx` in `client.go`). A
  slow read aborts on `stopForeground`; the cancelled read resolves through
  the standard `ErrNoClipboardImage` contract (Ctrl+V forwarded as ordinary
  input), so no late image or notice can reach a replacement owner and the
  pump cannot hold a route switch
  (`TestTerminalInputPumpClipboardReadBlockedDoesNotHoldForegroundSwitch`).
  Delivery itself stays lease-gated (`foregroundSendLease`), as for all pump
  output.
- Bounds: 1 MiB cap enforced independently on both sides
  (`maxClipboardImagePush` client, `maxImagePushSize` daemon); oversize
  falls back to forwarding Ctrl+V with a daemon notice
  (`TestRunRemoteClipboardOversizedImageForwardsCtrlV`).
- Paste safety: a `0x16` inside bracketed-paste content (same chunk, split
  chunks, or behind a pending lone ESC) is never intercepted
  (`TestRunRemoteClipboardCtrlVInsideBracketedPasteIsNotIntercepted`,
  `TestClipboardInterceptCtrlVInsideBracketedPasteAcrossChunksNotIntercepted`,
  `TestClipboardInterceptCtrlVAfterLoneEscapePendingNotIntercepted`).
- Failure contract: `ErrNoClipboardImage` forwards silently; other errors
  notify the daemon (`ClientNoticeClipboardFallback`) and forward
  (`TestRunRemoteClipboardFailureNotifiesDaemonAndWritesOutputVerbatim`).
- Headless: `internal/app/ui_driver.go` wires `clipboard: nil` for headless
  runs, so no interception exists there. Physical runs wire
  `clipboard.New()`.
- Daemon side: `handleImagePushForAttachment` rechecks currency before and
  after writing the temp file, caps size, writes a 0600 temp file recorded
  for session-end cleanup, and injects the path through the ordinary PTY
  write path (bracketed-paste wrapped iff the pane reports mode 2004).

### Application-originated: OSC 52 clipboard set

- Trigger: pane application emits OSC 52; captured off the pane screen by
  the PTY reader (`TestPTYReaderForwardsOSC52ClipboardToAttachedClient`).
- Execution site: daemon-side only. `forwardClipboardAsync` snapshots
  attached clients, queues per-client items drained by one ordered worker
  (`TestForwardClipboardAsyncSerializesClipboardWrites`).
- Fanout: every currently attached client receives the sequence; invalid
  base64 and payloads over `scopy.OSC52MaxPayloadBytes` (75_000) are dropped
  silently (`TestPTYReaderDropsOversizedClipboardPayload`,
  `TestPTYReaderDropsInvalidBase64Clipboard`).
- Ownership: the source pane generation is validated after sendMu admission
  (`beginClipboardOwnerSend`); a queued forward whose pane moved owner is
  never sent to the former owner, and a send error from a retired owner
  neither detaches nor closes its client
  (`TestQueuedClipboardAfterPaneMoveDoesNotSendToFormerOwner`,
  `TestQueuedClipboardRevalidatesOwnerAfterWaitingForClientSendLock`,
  `TestClipboardSendErrorAfterPaneMoveDoesNotDetachFormerOwner`).
- Parked suppression: while `parkedRouteOutput`/`parkedRouteFullPending` is
  set, the forward is dropped without sending; resume produces an
  authoritative full paint rather than replaying the effect
  (`TestParkedRouteSuppressesQueuedClipboardForward`,
  `TestParkedRouteDoesNotReplayOneShotEffectsAfterResume`).
- The client never parses or rewrites arbitrary output for effects; OSC 52
  bytes arrive inside ordinary `Output` frames and the terminal handles them.

## Non-goals retained

New desktop capabilities, local image interception, client-side parsing or
rewriting of arbitrary output, and any replacement wire message for OSC 52
remain unapproved. The OSC 52 path is preserved as-is.
