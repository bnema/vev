# Remote resilience

Remote attachments connect directly to the selected vev daemon. UDP is the
normal carriage: SSH bootstraps an authenticated endpoint, then the attachment
carries the vev protocol over UDP. Set `VEV_REMOTE_TRANSPORT=stdio` to use an
SSH-only carriage explicitly when UDP is unavailable. vev must be installed on
the remote host.

The session remains shared across all attachments. It owns PTYs, VT state, tabs,
panes, shared PTY content geometry, and ordered mutations. Each attachment keeps
its own window size, selected tab and pane, copy mode, overlays, render/output
acknowledgements, and reconnect state. Reconnecting one attachment resumes that
attachment without replacing its peers or changing their attachment-local
windows/views; its latest claim may still update shared PTY geometry.

## Remote picker identity

The session picker shows remote live, stopped, broken, stale, and unavailable
rows using the same expandable session/tab shape as local rows. A row carries a
lifecycle ID and stable tab selector separately from its display origin; labels
are never parsed back into routes. Cached and stale rows remain navigable, but
activation is gated until the host catalog and the exact lifecycle/tab identity
are current. A successful picker handoff sends the structured target to the
owning daemon without creating a local shadow session. Picker-selected remote
attachments use the daemon's environment and persisted session working
directory; direct CLI remote attaches retain their client-request semantics.

Remote attachments remain direct: the selected remote daemon owns the session,
PTYs, rendering, input, resize, effects, and teardown. The client does not
proxy remote content through the local daemon. A picker-selected remote target
is a temporary direct handoff; the client keeps one input pump and remembers a
bounded home route. A remote daemon may request the home picker, and cancelling
that temporary local picker returns to the parked remote route, first attempting
resume and then falling back to a fresh attach when the resume token is stale.
These navigation actions and the strict handshake layout use protocol v27.
Attach handshakes may carry an exact lifecycle/name target, and successful
Welcomes can return the daemon's committed identity; the client keeps bounded
route identity and display snapshots separate from transport capabilities.

## Durable record compatibility

The catalogue record format is currently version 4 because records now retain
stable tab identity metadata. Readers accept version 3 for one-way upgrades, but
older binaries reject records written at version 4. Back up the vev state
before a rollback; downgrading after an upgrade can leave newer catalogue
records unreadable and may require recreating those sessions.

Remote catalogues are bounded and versioned. Complete catalogues contain ordered
tab IDs, names, state, active tab, attachment state, and MRU sequence. Stopped
tab metadata is retained across daemon restart; legacy count-only catalogues are
read-only compatibility rows and cannot provide exact tab identity.

Remote picker previews are bounded styled-cell snapshots fetched without
attachment or PTY mutation. They are held in a short-lived in-memory cache only;
viewport contents are not persisted or written to diagnostics. Live preview
requests use debounce, same-key single-flight, bounded concurrency, TTL
stale-while-revalidate, and failure cooldowns. Stopped sessions show a static
placeholder, and late or canceled preview responses are discarded when the
picker selection changes.

## Connection limits

- A connection handshake has one 15-second deadline from connection start
  through Hello, Welcome, and the initial committed publication. The deadline
  applies to local and remote connections.
- Each command request has a 10-second deadline for its result. A timeout
  abandons only that request; it does not complete a later request with a stale
  result.
- Shared PTY content geometry follows the latest valid, non-superseded attach,
  resume, or resize claim. A later attachment can therefore replace the current
  shared geometry; if the winning attachment detaches, the most recently
  claimed remaining valid attachment becomes authoritative.

## UDP operation

- The remote host selects a UDP port in `61000-61023` by default. Set
  `VEV_UDP_PORT_RANGE` to a range, one port, or `0` for an ephemeral port.
- The UDP carriage authenticates and retransmits protocol frames, preserves
  frame order, and reports bounded link-health state without logging payloads,
  addresses, keys, or identities.
- The attachment reconnects with its resume token after a path change. The
  remote session and its PTYs remain in place while the attachment is offline.

The raw UDP AEAD key is held only in memory during bootstrap and transport
setup. It is not written to durable state. Key buffers are cleared on the
reachable handoff paths on a best-effort basis; cipher implementations and
other I/O owners may retain internal copies for their lifetime.

## Connection states

| State | Meaning |
| --- | --- |
| `connected` | Recent authenticated contact and complete frame progress. |
| `degraded` | Authenticated packets arrive, but complete-frame or acknowledgement progress is delayed. |
| `probing` | Authenticated contact is absent and the carriage is probing for a recovered path. |
| `resuming` | This attachment is opening a replacement connection with its resume token. |
| `offline` | The attachment is re-establishing the selected remote transport. |
| `expired` | The session or attachment resume window is no longer available. |

In raw terminal mode, state changes appear as vev status text rather than bytes
written into the PTY's output stream.

## Diagnostics and checks

At debug level, transport-health logs contain only state, elapsed progress ages,
and bounded counters. `scripts/debug-remote-attach.sh --udp-health user@host`
collects redacted health lines and Linux `ss -u -m` socket-memory counters.

Useful checks are:

- block and unblock the configured UDP port range;
- move between networks or change VPN state;
- suspend and resume the attachment machine;
- reconnect to the same named session;
- verify that a second attachment retains its independent view while the first
  reconnects.

Keep hostnames, usernames, keys, and local paths out of shared logs and
screenshots.
