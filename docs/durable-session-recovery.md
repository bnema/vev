# Durable session recovery

vev opens named-session state only while holding `$XDG_RUNTIME_DIR/vev/lifecycle.lock` (or the platform runtime fallback). Catalogue corruption prevents socket publication; vev never substitutes an empty healthy daemon.

## Session states

`stopped` is safe fresh metadata, `restoring` is validation in progress, and `degraded` preserves uncertain state without starting a replacement shell. A degraded session remains visible in `vev ls`, but attach is refused until an explicit recovery action succeeds.

## Recovery commands

- `vev cmd -s NAME session-recovery retry`
- `vev cmd -s NAME session-recovery restore GENERATION`
- `vev cmd -s NAME session-recovery export /ABSOLUTE/PATH`
- `vev cmd -s NAME session-recovery discard`

Retry validates persisted state without replacing it. Restore validates and promotes the selected catalogue fallback. Export writes recoverable data without mutation and requires an absolute destination path. Discard creates a new incarnation and retains the old record and snapshots under `quarantine/` until an explicit later purge.

## Migration and retention

The v0.x migration is resumable, retains legacy sources, and validates each referenced HEAD. Healthy sessions retain the committed checkpoint and up to two direct fallbacks; degraded, deleting, migrating, and unresolved transactions remain pinned.

## Fail-closed startup

If the catalogue cannot be opened or validated, the daemon does not publish its socket. Do not delete or edit state files. Check `vev-daemon.log` for `catalogue_validation_failed`, preserve the complete state directory, correct filesystem ownership or capacity problems, and retry. For corruption, make a backup before attempting recovery or exporting data.

A `lifecycle_owner_wait` event normally means another daemon is initializing or shutting down. Wait for ownership transfer; only terminate the prior process when its identity is known. The operating system releases `lifecycle.lock` when that process exits.

## Diagnostics

Durable state and recovery journals are under `$XDG_STATE_HOME/vev` (default `~/.local/state/vev`). Snapshot quarantine is under its `snapshots/quarantine/` tree. Runtime ownership is under `$XDG_RUNTIME_DIR/vev`; logs are JSON lines in the state directory.

Use session names, incarnation IDs, generation numbers, reason codes, and cursors from recovery events when diagnosing a failure. Recovery logs never include terminal or snapshot object contents.
