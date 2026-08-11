# Durable session recovery

vev opens named-session state only while holding `$XDG_RUNTIME_DIR/vev/lifecycle.lock` (or the platform runtime fallback). Catalogue corruption prevents socket publication; vev never substitutes an empty healthy daemon.

## Session states

`stopped` is safe fresh metadata, `restoring` is validation in progress, and `degraded` preserves uncertain state without starting a replacement shell. A degraded session remains visible in `vev ls`, but attach is refused until an explicit recovery action succeeds.

## Recovery commands

- `vev cmd -s NAME session-recovery discard`

Discard creates a new incarnation and retains the old record and snapshots under `snapshots/quarantine/` until an explicit later purge.

## Protocol upgrades

Each catalogue record stores the protocol version that wrote it. At startup, a record from any other protocol—including a newer protocol after a downgrade—is atomically replaced with a fresh incarnation before the daemon publishes its socket. Only the session name is retained; the working directory, layout, tabs, terminal history, recovery transcript, process-recovery state, and checkpoints are discarded. Records created before protocol tracking are treated the same way.

This reset is per named session and crash-safe. Once the fresh catalogue authority is committed, leftover snapshot objects are no longer authoritative and startup garbage collection can remove them. Catalogue corruption is not treated as a protocol mismatch and remains fail-closed.

## Incompatible checkpoints

After verifying a VEVM manifest's digest, vev treats any VEVM version mismatch as an incompatible healthy checkpoint. It atomically replaces only that named session's exact healthy checkpoint with a fresh incarnation. Unlike a protocol reset, this replacement retains the session name and working directory, but has no checkpoint, tabs, terminal history, or recovery transcript.

Digest mismatches, corruption, validation failures, I/O errors, and ambiguous failures are never reset or purged. The session remains degraded for explicit recovery.

## Retention

Healthy sessions retain the committed checkpoint and up to two direct fallbacks. Degraded and unresolved state remains pinned.

## Fail-closed startup

If the catalogue cannot be opened or validated, the daemon does not publish its socket. Do not delete or edit state files. Check `vev-daemon.log` for `catalogue_validation_failed`, preserve the complete state directory, correct filesystem ownership or capacity problems, and retry. For corruption, make a backup before attempting recovery or exporting data.

A `lifecycle_owner_wait` event normally means another daemon is initializing or shutting down. Wait for ownership transfer; only terminate the prior process when its identity is known. The operating system releases `lifecycle.lock` when that process exits.

## Diagnostics

Durable state and recovery journals are under `$XDG_STATE_HOME/vev` (default `~/.local/state/vev`). Snapshot quarantine is under its `snapshots/quarantine/` tree. Runtime ownership is under `$XDG_RUNTIME_DIR/vev`; logs are JSON lines in the state directory.

Use session names, incarnation IDs, generation numbers, reason codes, and cursors from recovery events when diagnosing a failure. Recovery logs never include terminal or snapshot object contents.
