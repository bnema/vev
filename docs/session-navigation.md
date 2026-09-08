# Session navigation

The command palette lists sessions from two origins: native results owned by
the serving daemon (active, stopped, remote, recent routes) and imported rows
relayed from the attaching client's own local daemon. Imported rows carry
only opaque keys plus display metadata; resolution happens through a
dial-only local control connection in the same interaction namespace.

## Inventory relay

While attached to a remote daemon, the client runs one relay per attachment
when it holds a committed local route. The relay dials only the local
control socket: no Hello, no session creation, no attachment, no daemon
startup.

- The serving palette sends an open demand with an interaction generation
  when it opens, and a close demand on cancel or select. Demands are
  capability-gated on both sides.
- While open, the relay polls the local source once per second with at most
  one query in flight plus one coalesced latest update. Only changed
  snapshots publish, with a monotonic publication generation per
  interaction. Admitted keys and retirements never cross interactions.
- Enter on an imported row sends a selection with the opaque keys and the
  admitted input cause. Selection crosses before the close demand on the
  guarded serving connection.
- Resolve revalidates the key against a fresh capture and returns a
  non-mutating attach target. Endpoint-empty targets resolve against the
  committed local route as authority; the serving route stays the recovery
  destination through the inventory transition (settle once, restore on
  every destination failure).
- Failures (stale identity, gone source, incompatible, invalid target,
  navigation or restore failure) surface as palette feedback with the exact
  cause. Native commands stay usable throughout.

## Remote vision

Remote-origin rows display availability tags (`down`, `broken`, stopped)
inline so they never look attachable. Remote vision is read-only: Enter on
a remote row reports feedback and emits no selection, and selections for
non-local sources reject before resolve. Non-OK groups project no rows.

## Compatibility

Old clients ignore unknown server frames and never publish; old daemons
never send demands so new relays stay idle with zero control dials. Local
attachments ignore demands even when they decode. Version negotiation
stays strict equality.
