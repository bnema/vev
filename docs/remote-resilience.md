# Remote sessions

```sh
vev attach user@host[:session]
```

vev must be installed on the remote host. The remote daemon owns the session: it runs the shells, renders the screen, and sends only small diffs to your client. Nothing is proxied through your local daemon.

## How the connection works

1. vev opens SSH to the host and starts a short-lived QUIC server there.
2. SSH hands back a one-time token and the server's certificate fingerprint.
3. Your client connects directly over QUIC (UDP), checks the fingerprint, and uses the token once.

Terminal traffic then goes over QUIC, not SSH. The token and keys stay in memory only.

If UDP is blocked, register the host with SSH only:

```sh
vev host add --transport ssh user@host
```

The transport is fixed when the host is added (`quic` by default). To switch, run `vev host rm user@host`, then add it again with the other `--transport`.

## What survives a network problem

- The remote session and its shells keep running while you are offline.
- The client reconnects by itself after a network change, a VPN switch, or a laptop sleep, and keeps retrying for up to 15 minutes of outage before it returns to the picker. Press Ctrl-C or Esc while it reconnects to give up and return to the picker.
- A stalled link does not block the client loop. Keys typed while the link stalls or reconnects are kept and sent in order once it recovers. Keys vev had not yet sent when the link dropped are replayed to the resumed session; keys already in flight at the drop may be lost.
- If the reconnect window has expired but the session still exists, the client opens a fresh attachment to it.
- Several clients can attach to the same session. Each keeps its own window, tab, focus, and copy mode.

The command palette is rendered by the remote daemon, so opening it needs a working link. A client-owned session picker that is already open remains locally interactive during an outage.

## Typing on a slow link

On remote sessions vev shows what you type before the server answers, like mosh. When the round trip exceeds about 60 ms, typed characters and backspace appear at once on the cursor line. Above about 160 ms they are underlined until the server confirms them. A wrong guess, such as a password prompt that echoes nothing, is erased when the server answers. Set `echo.predict = always` or `never` to change this; see [configuration](configuration.md).

## Connection states

| What you see | Meaning |
|---|---|
| The session | The link works. |
| `Connecting to session…` | vev is opening a session or reconnecting a dropped link, and keeps your keys. Switching to a running local session shows it only after 200 ms. |
| The picker with an error | The session could not be reached again, or you cancelled. |

vev does not yet report a slow (`degraded`) link separately. Connection notices never appear inside your shell output.

## Hybrid mode: local and remote together

Add a host once:

```sh
vev host add user@host
```

Then the session picker (`SSP`) shows local and remote sessions in one list. Remote sessions appear as `session@host`, with tags like `down` or `broken` when they can't be opened.

- Selecting a session switches to it, local or remote. `BCK` goes back to the previous one; `JRS` jumps to a recent one.
- Switching between sessions on the same daemon (two local sessions, or two sessions on the same host) reuses the live attachment, so there is no reconnect.
- A remote session opened from the picker uses the remote host's environment and saved working directory.
- `CNS` asks where to create the new session when several daemons are available.
- Session history and the last selected tab in each session are per client and kept only while that client runs. Switching back restores that tab; choosing a specific tab overrides it. If the remembered tab was closed, vev falls back to the first tab.
- Previews of remote sessions are cached in memory only, never written to disk.

After you leave a remote, vev keeps its connection warm so going back is instant. See [warm remote transports](configuration.md#warm-remote-transports).

## Timeouts

| What | Limit |
|---|---|
| Connect to first screen | 15 s |
| `vev cmd` result | 10 s |
| A detached remote attachment waiting to resume | 15 min |

## Upgrading

Client and daemons must run the same protocol version. Upgrade vev on your machine and on every remote host together.

## Debugging

```sh
VEV_LOG=debug vev attach user@host
scripts/debug-remote-attach.sh user@host   # collects redacted connection health
```

Useful manual checks: block UDP, switch networks or VPN, suspend the laptop, then reattach to the same session. For isolated QUIC/SSH impairment and real Wayland terminal captures, see the [visual resilience harness](../scripts/remote-resilience-harness/README.md).

Keep hostnames, usernames, and keys out of shared logs and screenshots.
