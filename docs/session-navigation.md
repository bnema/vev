# Session navigation

Palette destinations use bare session names in local-only mode. With remote
hosts configured, local sessions use `session@local` and remote sessions use
`session@host`, relative to the attaching client regardless of the serving
daemon. Offline hosts still enable qualified local labels. Local inventory
rows replace history rows for the same exact session;
homonyms with different origins or lifecycles remain distinct. A resume
credential belongs only to the route currently holding its attachment, so
older sessions in history use exact attachment after a same-daemon switch.

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
  when it opens, and a close demand on every successful close (execute,
  select, or cancel). Demands are capability-gated on both sides: the
  client advertises inventory in Hello only while a committed local route
  and the dial-only control source are available.
- While open, the relay polls the local source once per second with at most
  one query in flight plus one coalesced latest update. Only changed
  snapshots publish, with a monotonic publication generation per
  interaction. Admitted keys and retirements never cross interactions.
- Enter on an imported row sends a selection with the opaque keys and the
  admitted input cause, which stays zero for ordinary keyboard input.
  Selection crosses before the close demand on the guarded serving
  connection.
- Resolve revalidates the key against a fresh capture and returns a
  non-mutating attach target. Endpoint-empty targets resolve against the
  committed local route as authority; the serving route stays the recovery
  destination through the inventory transition (settle once, restore on
  every destination failure).
- Failures (stale identity, gone source, incompatible, invalid target,
  navigation or restore failure) surface as palette feedback with the exact
  cause while the originating interaction is still open; stale failures
  from closed interactions drop. Native commands stay usable throughout.

## Client-owned picker

The navigation picker runs as a typed client interaction when the serving attachment advertises the client-picker capability: the palette command `SSP` (session picker) opens it, and a client with a home route answers that command with a home-picker navigation directive instead, so the picker stays daemon-rendered on the home route.

The client renders the picker frame from the admitted snapshot rows, moves the cursor and filters with `/` locally, and sends only a typed selection (opaque row key plus displayed revision) or a close. The serving daemon re-resolves the key, revalidates the target lifecycle, and performs the unchanged navigation handoff; a stale revision, unknown key, retired target, or failed navigation returns a typed failure instead of committing. Escape closes the interaction without selecting, `s` (sort) and `x` (kill) are unavailable in this mode, and the cursor/search state is local to the interaction.

## Remote vision

Remote-origin rows display availability tags (`down`, `broken`, stopped)
inline so they never look attachable. Remote vision is read-only: Enter on
a remote row reports feedback and emits no selection, and selections for
non-local sources reject before resolve. Non-OK groups project no rows.

## Compatibility

Demands gate both sides on the negotiated inventory capability: a serving
palette that never receives the bit never demands, and a client without a
committed local route or control dialer ignores demands that decode. With
no demands the relay stays idle with zero control dials, and local
attachments ignore demands even when they decode. Unknown frame types
stay ignored by the receive pump. Cross-version attachment still fails at
the strict-equality handshake; graceful degradation applies only within
one negotiated version.
