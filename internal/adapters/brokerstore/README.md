# Offline broker store (Plan 001 P2.3)

`OpenOffline` is deliberately not wired into application composition. Supply a
private destination directory, immutable copies of any legacy inputs, and an
explicit policy for each imported endpoint. An empty input path means no input;
a named missing, empty, corrupt, or unsupported file fails closed. The lifetime
lock is `hosts.json.lock`, the exact lock file the legacy host writer takes, so
a store opened on a directory that live writer also uses excludes it for the
lifetime of the store. The legacy cache source has no such lock — its writer
takes none — so a cache source must be immutable or quiesced before migration.
Never point migration at a cache file that a writer can still mutate.

Each source's recovery manifest entry records its generic role label
(`membership` for the host registry, `observations` for the catalogue cache),
whether the source was supplied at all, and the SHA-256 of its bytes. Presence
is separate from the digest because an absent source and an empty one can share
a digest; a record that claims a source was absent while retaining its bytes is
rejected.

## Transaction and recovery

- Hold `hosts.json.lock` with Linux nonblocking exclusive `flock` until `Close`.
  The name matches the legacy host writer's lock, so the store and that writer
  exclude each other; the legacy catalog cache writer takes no lock at all.
- Validate bounded legacy hosts v1/v2/v3 and cache v2/v3/v4 without calling their
  existing writer adapters. Cache decoding freezes the existing exact legacy
  DTOs here; count-only v2 inventories are deliberately unsupported.
- Write `recovery.json`: original bytes, SHA-256 migration manifest with source
  presence, policies, generated registration identities, and initial unified
  state. Write, fsync, rename, then fsync the directory before attempting state
  publication.
- Write `state.json` with the same atomic sequence. Hosts, policies, snapshot,
  and manifest share one rename commit point. Only then return a ready store.
- If state is absent on restart, validate and finish the recovery record without
  resampling identities or rereading changed sources. If state exists but is
  corrupt, fail closed; never silently roll authority back.
- Keep recovery and original inputs indefinitely. Rollback is an explicit
  offline operator action using the original bytes, not automatic fallback.
  P2.3 does not retire or delete production files.

A failed mutation poisons the handle because rename may already have happened.
Close and reopen to resolve that outcome; do not blindly retry. Fault tests cover
both files at write/fsync/rename/directory-fsync boundaries. They simulate errors
and process reopening, not hardware power-loss behavior of a particular disk.

The file budget is 16 MiB per JSON file (including the base64 rollback inputs in
the recovery file). Inputs are bounded before decoding, must be regular files,
and cannot be symlinks. JSON rejects duplicate/unknown fields, trailing content,
invalid UTF-8, excessive nesting, and invalid domain/catalogue values.

## Authority

`BrokerHostStore` separates durable membership from advisory observations.
Host replacement uses a store revision CAS. Policy changes require a changed
registration identity/generation; generations cannot regress within an
incarnation. Full remove/re-add callers must supply a fresh incarnation.
Snapshots cannot add hosts, overwrite retired registrations, regress revisions,
or switch epochs after the first successful publication on an open handle.
`NewRegistry` requires `BrokerHostStore` explicitly: durable membership is not
an optional capability, and the registry never runs on a store that cannot
answer `LoadHosts`. Startup intersects cached observations with authoritative
membership, and creates unknown projections for registrations without
observations. An observation is adopted only when it satisfies the same durable
projection rules the store enforces (`ports.ValidateDurableHostProjection`):
availability and failure kind inside their closed ranges, and a catalogue-valid
session inventory for the exact incarnation and success time. Anything else is
typed as an invalid response, so a published snapshot is never one the store
would reject and silently never persist.

Store failures are classified with `internal/ports` sentinels:
`ErrBrokerHostConflict` (refresh the revision and retry),
`ErrBrokerStoreLocked` (another owner is live), and
`ErrBrokerStoreInvalidState` (resolve offline from the recovery record; state is
never silently replaced or rolled back). Malformed caller membership passed to
`ReplaceHosts` is validated before the CAS and returned as a plain caller error,
never as `ErrBrokerStoreInvalidState`; the durable state is unchanged.

Unbound cache v2/v3 records remain recoverable but cannot seed registration
identity. A bound cache entry seeds only the exact surviving host incarnation;
legacy cache schemas do not carry a registration generation. The imported host
record remains authoritative for generation. Transient checking/error objects
are never persisted. Retirement tombstones are process-local fencing state and
are never durable: the registry's persistable copy and the store both clear
`Snapshot.Removed` before writing, so reopening sees membership without stale
tombstones. No environment, launch, or trust policy is guessed.

Production cutover, lifecycle supervision, and IPC composition remain later
phases. Close ordering must drain registry snapshot writers before releasing the
store; the store does not own those workers.
