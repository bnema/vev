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
proxy remote content through the local daemon. The session picker and command
palette read remote session inventory from the daemon's durable catalog cache
and refresh it asynchronously while either overlay is open. Cached names appear
immediately, but selection still requires a fresh exact lifecycle and tab
identity.

A single client process separately keeps a bounded, in-memory route history
across local, direct remote, and picker-discovered attachments. Catalog targets
retain exact endpoints, while route snapshots keep endpoints private; rows from
those sources therefore remain separate when their authority cannot be compared
exactly. The daemon renders the latest attachment snapshot; active routes are
metadata-only. `JRS` and cross-daemon `BCK` transitions send typed
key/generation actions back to the client, while `BCK` to a live session on the
current authenticated daemon uses an in-band switch. Route history is
process-local and is not persisted or shared between clients.

A picker-selected remote target is a direct handoff with one input pump. Exact
lifecycle/name identities are carried separately from display labels, and a
successful daemon-local switch publishes a new committed identity before the
client republishes its snapshot. These navigation actions use the strict,
exact-match protocol version. Attach handshakes may carry an exact target, and
successful Welcomes can return the daemon's committed identity.

Selecting an ordinary active session row on the daemon already serving the
attachment uses an in-band switch. The daemon first offers an endpoint-empty,
exact lifecycle target; the client confirms it with its remembered stable tab
cursor while holding raw input. The daemon either commits the fenced attachment
transition and publishes the committed identity before the rebased full paint,
or sends a typed pre-commit rejection and leaves the source attachment usable.
No hostname, label, DNS result, or SSH alias authorizes reuse. Stopped and
cross-origin targets outside the hybrid UDP flow retain direct close-and-dial
handoff.

A UDP attachment opening the hybrid home picker keeps its authenticated remote
transport parked while a transient local connection renders the picker. The
remote daemon suspends that attachment's rendered output only after a
lease-bound prepare handshake, so the picker has sole terminal ownership and
the client does not acknowledge screen updates it did not display. Terminal-
external one-shot effects generated while parked, such as OSC 52 clipboard
writes, are intentionally suppressed and are not replayed after Back or a
switch; applying them later could affect a different terminal owner. Back
resumes the parked attachment's current VT state with a full paint. Selecting a
live or stopped session from the exact same configured endpoint sends the
structured lifecycle/tab target over the parked transport; the serving daemon
revalidates it and switches or restores with its persisted working directory
and daemon-owned environment. Different endpoints and SSH stdio routes close
and dial normally. While any switch takes longer than the short display
threshold, the client overlays an animated switching or starting toast until
the destination's authoritative output is flushed.

Opening the home picker does not change the client's active route or recent
session order, on either UDP or SSH stdio. Its status bar keeps the source
session label (for example, `misc@igor`), not the local session hosting the
picker. Local backing-session tabs and bar-script output are hidden while
this temporary attachment renders the picker. Selecting a destination commits
that route; Back returns to the source. Local tab selections also hand off to
the client, including selections in the backing session itself: the temporary
picker connection never becomes the destination attachment. Protocol version
40 carries the explicit local tab in `AttachTarget.PreferredTabID`; it overrides
remembered tab state. If that tab disappears before attach, the existing Hello
default-tab fallback applies. Upgrade the client and both daemons together.

### Real-client hybrid navigation check

On Linux with Python 3, tmux, OpenSSH and an sshd runnable as your current user:

```sh
go build -o /tmp/vev-hybrid-check .
python3 scripts/hybrid-navigation-repro.py --binary /tmp/vev-hybrid-check --artifacts /tmp/vev-hybrid-check-frames
```

This creates two private `VEV_ENV=dev` daemons and a loopback-only SSH server
with temporary keys, then drives a real client in a dedicated tmux server over
UDP and SSH stdio. It checks the remote picker label, Back, selecting a second
local tab, updated recent history, and returning via the remote palette to the
remembered local tab. Terminal captures are saved to the artifact directory;
the test daemons, tmux server, SSH server and temporary state are cleaned up.
Normal vev sessions and SSH configuration are untouched. Use `--transport udp`
or `--transport stdio` to run one carriage.

## Durable record compatibility

Catalogue record format version 6 stores durable session metadata independently
from the live protocol version. At daemon startup, supported version 3–5 records
are losslessly converted before socket publication and the original catalogue is
preserved as a private `sessions.kv.pre-v6.bak` backup. A wire-protocol change
does not reset session state. Unsupported or corrupt records fail closed without
modifying the catalogue. Older binaries cannot read version 6, so restore the
backup before a rollback.

Remote catalogues are bounded and versioned. The current schema is mandatory
and contains lifecycle IDs, ordered typed tab records, state, active tab,
attachment state, and MRU sequence. Peers without the exact schema are rejected
as version-incompatible rather than exposed as partial picker rows.

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
- Each parked-route control response and its required full paint have separate
  15-second client deadlines. A parked lease expires after 15 minutes and
  closes only its exact retained transport generation.
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
- Large full output snapshots use bounded zlib compression when it reduces the
  wire payload. Incremental output and non-beneficial snapshots retain their
  canonical encoding. The protocol validates the compression kind, declared
  decoded length, stream integrity, and trailing bytes before terminal output
  is applied.
- The attachment reconnects with its resume token after a path change. If the
  attachment resume window expires but its exact session lifecycle still
  exists, the client opens a fresh exact attachment instead. The remote session
  and its PTYs remain in place while the attachment is offline.

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
