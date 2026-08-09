# Stack 2 route ledger/protocol red-team

## Builder decision

Add Stack 2 as daemon-neutral plumbing on top of the completed presentation branch:

- model route origins, exact lifecycle/name targets, committed identity, bounded complete snapshots, opaque key/generation navigation actions, and closed failure codes in `internal/ports`;
- bump the protocol version and append an optional exact target section to `Hello` while preserving the version peeker's first field;
- wire explicit local, remote, and discovery origins at `internal/app`/client composition boundaries;
- keep the client ledger private, bounded, lock-protected, and separate from display values and transport capabilities;
- validate exact local targets in the daemon and preserve existing remote-target authority;
- leave JRS/BSK command authority and the atomic global cutover for Stack 3.

## Critic pass

The read-only reviewer worker did not return a retrievable result in this harness.
The builder completed a local high-impact critic pass below and recorded each
objection with its resolution and acceptance check.

## Evidence so far

- PR #188 dependency branch already has strict Hello peeking and remote lifecycle/tab target validation.
- `internal/ports/wire.go` requires `Hello.Version` first; the new exact-target section is appended after the existing remote/policy section.
- Existing daemon route authority remains in `routeWithContext`; exact local validation is an additional lifecycle check before ordinary attach routing.
- Focused ports, client, app, and daemon tests pass after the current incremental changes.

## Accepted risks

- Stack 2 introduces codec/model seams and private transition primitives but does not yet publish snapshots or replace command authority; those remain explicit Stack 3 work.
- The first reviewer-worker attempts did not produce a retrievable result in this harness, so the builder performed the required critic pass locally before final validation.

## Builder critic pass and resolutions

- **Welcome identity mismatch:** a malicious or buggy daemon could return a committed lifecycle/name different from the Welcome metadata. `UnmarshalWelcome` now rejects mismatched session name or ephemeral state.
- **Stale exact target side effects:** exact local validation waits for an existing restore barrier before checking lifecycle identity; it does not create or mutate a session and rejects lifecycle replacement before normal attach.
- **Snapshot leakage:** `RecentRouteEntry` contains only opaque key/generation and display fields. Dialers, requests, resume credentials, and exact targets remain in private `routeRecord` values and are copied out only through navigation lookup.
- **Transactional navigation:** connector failure, stale key/generation, or committed-target mismatch returns before ledger commit; origin is restored from the selected private record rather than connector input.
- **Bounds/strictness:** snapshot entry count, labels, refs, closed enums, bool encodings, truncation, trailing bytes, and protocol-version peeking are covered by tests; focused race tests pass.

## Follow-up fixes

The implementation now commits a successful Welcome into the private ledger before
raw-mode entry, publishes a complete snapshot frame, and accepts/validates that
frame on the daemon attachment. The active route is metadata-only, the private
history cap is 20, origin keys distinguish daemon endpoints, route labels reject
invalid UTF-8/control text, navigation actions carry the snapshot generation,
active selection is a no-op, and failed transitions invoke the prior-route
restore hook before returning failure.
