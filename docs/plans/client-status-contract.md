# Client status contract (P4.2, design-only)

Status: proposed. No production edits accompany this document. No provider
implementation is authorized by this plan alone; a runnable task breakdown
follows only after contract approval.

## Current daemon-host behavior (preserved)

- Commands come only from the daemon-loaded config: `bar.top-right`,
  `bar.bottom-right`, `bar.interval` (defaults empty/empty/5s,
  `internal/adapters/config`, `domain.Defaults`).
- Execution is daemon-host via `barScriptRunner.run`
  (`internal/usecase/daemon/bar_script.go`) with `ports.ShellCommandRunner`:
  timeout 1s (`barScriptTimeout`), stdout cap 1024, stderr cap 512, display
  cap 256, first-line + `stripANSIAndControls` sanitization, warn-once
  failure dedup (`lastFailure`).
- Scheduling is per-session in `bar_refresh.go`: poller on the daemon clock
  with `MinBarInterval` (1s) floor, `running`/`pending` coalescing, context
  change (`lastContext`) forces refresh, `reload` channel wakes the poller on
  config change, `refreshBarScriptsAllSessions` applies new commands
  immediately.
- Scripts observe daemon-side context only: `VEV_ANCHOR`, `VEV_SESSION`,
  `VEV_TAB`, `VEV_PANE`, `VEV_PANE_CWD`, `VEV_COLS`.
- Composition reads snapshots at paint time: `barScriptSnapshot(sess)` feeds
  `barState.topRight`/`bottomRight` alongside `status`, `statusFeedback`,
  `mru`/`rankedRecent`, `attentionFrame` (`status.go:161,308,334`).
- The client has no status-provider code today: no reference to
  `topRight`/`bottomRight` exists under `internal/usecase/client`. The client
  renders daemon-published frames verbatim. This contract does not change
  that until its implementation is approved.

## Opt-in client-local provider contract

### Configuration authority

- The command string for a client-local provider comes exclusively from the
  **local** configuration authority (the config file loaded by the local
  process composition). A remote daemon MUST NOT supply a shell command over
  the wire; the wire carries only rendered text values (see fields below).
- Existing `bar.top-right` / `bar.bottom-right` scripts remain daemon-host
  execution with unchanged semantics. Client-local providers target the same
  two slots but never replace daemon execution implicitly: both run, and
  precedence (below) decides what paints.
- Opt-in per slot: unset means absent. No implicit fallback to a remote path,
  and remote pane CWD is never interpreted as a local path. A provider that
  needs a working directory uses only locally configured values.

### Generations

- **Provider generation**: a monotonically increasing epoch owned by the
  executing side, bumped on every configuration reload affecting the slot.
  Stale values (older provider generation than the latest admitted for that
  slot and attachment) are discarded, never painted.
- **Attachment generation**: the existing attachment identity/generation used
  in output arbitration binds a value to the attachment it was produced for.
  A value admitted for attachment A is never painted on attachment B; on
  navigation/switch, pending provider values for the retired attachment are
  dropped with the foreground owner.

### Bounds, queues, cancellation

- Keep the existing daemon bounds unless separately approved: 1s timeout,
  1024-byte stdout cap, 512-byte stderr cap, 256-rune display cap.
  Client-local execution adopts the same caps.
- Bounded work queue per attachment (proposed depth 1 with coalescing,
  mirroring `running`/`pending`): a slow provider run never stacks; a newer
  tick marks pending and supersedes.
- Cancellation: provider runs cancel on attachment retirement, slot
  reconfiguration, and client shutdown via the existing foreground-lifetime
  mechanism. A late completion after cancel is discarded.

### Sanitization and error presentation

- All provider output passes daemon-side-equivalent sanitization on the
  executing side before admission: first line, valid UTF-8, ANSI/control
  stripping (`sanitizeBarScriptOutput` semantics), display-cap truncation.
- Stale/error presentation is deterministic: on failure the slot keeps the
  last good value; a slot with no good value renders empty. Failures warn
  once per (provider, slot, signature) until the signature changes. No error
  text from stderr is painted (stderr is diagnostics only, capped).

### Precedence (same slot, daemon + client values)

1. A fresh admitted daemon value and a fresh admitted client value for the
   same slot: the **client-local value wins only when the slot is explicitly
   configured for client-local authority**; otherwise the daemon value wins.
   Authority is per slot, from local configuration, never negotiated.
2. A stale value (older generation) never overrides a fresh one regardless
   of side.
3. Absent/unset on the winning side falls through to the other side's latest
   admitted value, then to empty.

## Proposed fields (exact, pending approval)

Configuration (local file, new keys under the existing parser):

- `status.top-right.provider = local | daemon` (default `daemon`)
- `status.top-right.command = "<shell>"` (default empty = absent)
- `status.bottom-right.provider`, `status.bottom-right.command`: same shape.
- `status.interval`: duration, default 5s, floor 1s (`MinBarInterval`);
  invalid values warn and keep prior, mirroring `bar.interval`.

Wire/schema (rendered text only, never commands):

- `StatusSlotValue{Slot: top-right | bottom-right, Text: string (already
  sanitized, ≤256 runes), ProviderGeneration: uint64,
  AttachmentID: <existing attachment identity type>}` carried inside the
  existing publication path for the owning attachment only.
- No new carriage primitive: values travel with the attachment's ordered
  publication; no persistence-format change.

Validation: unknown `provider` value warns and keeps `daemon`; empty command
disables the slot; interval below minimum clamps and warns.

## Tests mapped (to be added only after approval)

- `TestClientProviderCommandNeverComesFromWire` (sessionwire/client):
  crafted inbound value with command content is rejected; only text fields
  admitted.
- `TestStaleProviderGenerationDiscarded` (client/composition): older
  provider generation never paints over a newer admitted value.
- `TestProviderValueBoundToAttachment` (client): value admitted for A never
  paints on B; retirement drops pending values.
- `TestProviderPrecedenceDaemonWinsByDefault` / `TestProviderPrecedenceLocalWinsWhenConfigured`
  (composition): per-slot authority matrix.
- `TestProviderFailureKeepsLastGood` / `TestProviderEmptyWhenNoGood`
  (provider harness): deterministic stale/error presentation; warn-once.
- `TestProviderQueueCoalesces` (provider harness, fake clock): slow run +
  newer tick yields at most one pending execution; cancel on retirement.
- `TestRemoteCwdNeverInterpretedLocally` (provider harness): provider env
  contains no remote-derived path.
- Existing pins stay green: `bar_refresh_test.go`, `bar_script` tests,
  `status` composition tests, config parse tests.

## Stop condition

P4.2 ends at this document. No provider implementation, no new config key,
no wire field, and no new dependency is authorized by this plan alone.
