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

# Client-owned route-position red-team

## Builder decision

Keep live tab/view authority on each daemon attachment, but remember the last
stable tab ID per exact client route in the process-local route ledger. Add an
optional preferred stable tab to Hello and a daemon-to-client route-position
publication bound to an exact lifecycle/name target. A missing preferred tab is
a hint miss and falls back to the daemon's normal attachment tab; it never
changes session identity or shared session tab state.

## Critic pass

- **Multiple clients:** daemon-global or session-global memory would couple
  clients. The remembered cursor stays in each Runner's private route ledger;
  the daemon applies it only to the requesting attachment.
- **Stale/out-of-order publications:** a tab ID without session identity could
  corrupt the route selected by a concurrent transition. RoutePosition carries
  the exact lifecycle/name target, and the ledger applies it only to that route.
- **Handshake ordering:** publishing another server frame before Welcome would
  recreate the resume-token failure found during manual testing. Initial route
  position is emitted only by post-Welcome rendering; Welcome stays first.
- **Deleted tabs:** remembered positions are hints, not exact picker authority.
  The daemon validates stable IDs under target-session ownership and falls back
  without rejecting the attachment when the tab no longer exists.
- **Discovery metadata:** RemoteTarget includes a point-in-time live tab and must
  not become route memory. Committed route records drop that catalog selector;
  PreferredTabID is the independent mutable cursor.
- **Resume and identity authority:** a preferred tab cannot select a session. It
  is accepted only for attach/resume and only after resume-token or exact-target
  session resolution succeeds.
- **Untrusted IDs:** both Hello and RoutePosition reject empty/oversized/control
  stable IDs and trailing or truncated payloads.
- **Publication completeness:** tab selection and repair always invalidate a
  render. RoutePosition is serialized with the attachment's post-commit output
  transaction and cached per transport incarnation; output rebase forces an
  initial publication after attach/resume.

## Accepted risks

- The state is intentionally process-memory only. Restarting the client loses
  remembered tabs; restarting a daemon does not make the daemon the memory
  owner, and missing/replaced stable IDs safely fall back.
- RoutePosition is an additional small control frame only when the exact target
  or active stable tab changes, not on every output diff.
