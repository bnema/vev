# Remote resilience

Remote attachments connect directly to the selected vev daemon. UDP is the
normal carriage: SSH bootstraps an authenticated endpoint, then the attachment
carries the vev protocol over UDP. Set `VEV_REMOTE_TRANSPORT=stdio` to use an
SSH-only carriage explicitly when UDP is unavailable. vev must be installed on
the remote host.

The session remains shared across all attachments. It owns PTYs, VT state, tabs,
panes, a fixed PTY content size, and ordered mutations. Each attachment keeps
its own window size, selected tab and pane, copy mode, overlays, render/output
acknowledgements, and reconnect state. Reconnecting one attachment resumes that
attachment without replacing or changing its peers.

## Connection limits

- A connection handshake has one 15-second deadline from connection start
  through Hello, Welcome, and the initial committed publication. The deadline
  applies to local and remote connections.
- Each command request has a 10-second deadline for its result. A timeout
  abandons only that request; it does not complete a later request with a stale
  result.
- The first valid attachment establishes the session's PTY content size. A
  later attachment can resize its own window without resizing shared PTYs.

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
