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

If UDP is blocked, use SSH only:

```sh
VEV_REMOTE_TRANSPORT=stdio vev attach user@host
```

## What survives a network problem

- The remote session and its shells keep running while you are offline.
- The client reconnects by itself after a network change, a VPN switch, or a laptop sleep.
- If the reconnect window has expired but the session still exists, the client opens a fresh attachment to it.
- Several clients can attach to the same session. Each keeps its own window, tab, focus, and copy mode.

## Connection states

| State | Meaning |
|---|---|
| `connected` | Everything works. |
| `degraded` | Packets arrive, but updates are slow. |
| `probing` | No contact; vev is looking for a working path. |
| `offline` | No contact for a while; vev is reconnecting. |
| `dead` | The connection is gone for good. |

States appear in the vev status bar, never inside your shell output.

## Hybrid mode: local and remote together

Add a host once:

```sh
vev host add user@host
```

Then the session picker (`SSP`) shows local and remote sessions in one list. Remote sessions appear as `session@host`, with tags like `down` or `broken` when they can't be opened.

- Selecting a session switches to it, local or remote. `BCK` goes back to the previous one; `JRS` jumps to a recent one.
- A remote session opened from the picker uses the remote host's environment and saved working directory.
- `CNS` asks where to create the new session when several daemons are available.
- Session history is per client and is not saved.
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

Useful manual checks: block UDP, switch networks or VPN, suspend the laptop, then reattach to the same session.

Keep hostnames, usernames, and keys out of shared logs and screenshots.
