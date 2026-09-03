# Durable session recovery

vev opens named-session state only while holding `$XDG_RUNTIME_DIR/vev/lifecycle.lock` (or the platform runtime fallback). Catalogue corruption prevents socket publication; vev never substitutes an empty healthy daemon.

## Session states

`stopped` is safe fresh metadata, `restoring` is validation in progress, and `degraded` preserves uncertain state without starting a replacement shell. A degraded session remains visible in `vev ls`, but attach is refused until an explicit recovery action succeeds. A session reserved for durable purge is hidden from listings and pickers until deletion either completes or leaves a retained broken record.

## Recovery commands

- `vev cmd -s NAME session-recovery discard`

Discard creates a new incarnation and retains the old record and snapshots under `snapshots/quarantine/` until an explicit later purge.

## Catalogue format upgrades

Catalogue format version 6 is independent from the live client/daemon protocol. At startup, vev losslessly converts supported version 3–5 records to version 6 before publishing its socket. The conversion preserves session identity, working directory, tabs, timestamps, checkpoint references, and degradation state; a wire-protocol update alone never resets durable sessions.

Before rewriting a legacy catalogue, vev creates and syncs a private `sessions.kv.pre-v6.bak` backup beside it. The catalogue is then replaced atomically. If the source is corrupt, from an unsupported future format, or conflicts with an existing backup, startup fails closed and leaves the catalogue untouched. Version 6 is not readable by older binaries; to roll back, stop vev and restore the backup before starting the older binary.

## Incompatible checkpoints

After verifying a VEVM manifest's digest, vev treats any VEVM version mismatch as an incompatible healthy checkpoint. It atomically replaces only that named session's exact healthy checkpoint with a fresh incarnation. Unlike a protocol reset, this replacement retains the session name and working directory, but has no checkpoint, tabs, terminal history, or recovery transcript.

Digest mismatches, corruption, validation failures, I/O errors, and ambiguous failures are never reset or purged. The session remains degraded for explicit recovery.

## Retention

Healthy sessions retain the committed checkpoint and up to two direct fallbacks. Degraded and unresolved state remains pinned.

## Fail-closed startup

If the catalogue cannot be opened or validated, the daemon does not publish its socket. Do not delete or edit state files. Check `vev-daemon.log` for `catalogue_validation_failed`, preserve the complete state directory, correct filesystem ownership or capacity problems, and retry. For corruption, make a backup before attempting recovery or exporting data.

A `lifecycle_owner_wait` event normally means another daemon is initializing or shutting down. Wait for ownership transfer; only terminate the prior process when its identity is known. The operating system releases `lifecycle.lock` when that process exits.

## Diagnostics

Durable state and catalogue migration backups are under `$XDG_STATE_HOME/vev` (default `~/.local/state/vev`). Snapshot quarantine is under its `snapshots/quarantine/` tree. Runtime ownership is under `$XDG_RUNTIME_DIR/vev`; logs are JSON lines in the state directory.

Use session names, incarnation IDs, generation numbers, reason codes, and cursors from recovery events when diagnosing a failure. Recovery logs never include terminal or snapshot object contents.
