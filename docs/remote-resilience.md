# Remote resilience

Remote attach starts with SSH bootstrap and then carries the vev session over authenticated UDP. The design is inspired by mosh's mobile-shell resilience, but vev uses its own protocol and is not compatible with mosh clients or servers.

## Operational model

- SSH bootstrap starts or finds the remote UDP endpoint.
- UDP carries normal session traffic after bootstrap. The remote proxy binds a port in a fixed range, `61000-61023` by default; `VEV_UDP_PORT_RANGE` overrides it (a range, a single port, or `0` for a random ephemeral port).
- Named sessions remain recoverable across daemon restarts; ephemeral sessions remain recoverable only while the daemon retains them.
- `VEV_REMOTE_TRANSPORT=stdio` keeps all traffic inside SSH when direct UDP is blocked.

## Recovery states

Resilience work uses these user-facing states:

| State | Meaning |
| --- | --- |
| `connected` | a complete record or meaningful ACK progress is recent |
| `degraded` | authenticated UDP packets still arrive, but complete-record or ACK progress is delayed |
| `probing` | authenticated UDP packet contact is absent, so vev probes for path recovery |
| `resuming` | a replacement transport is attaching to the same session |
| `offline` | authenticated UDP contact remains absent; vev re-runs the SSH bootstrap and re-dials UDP with the resume token |
| `expired` | the session or recovery window is no longer available |

When a terminal is in raw mode, recovery messages appear as vev status text rather than as bytes written into the remote program's output stream.

`VEV_REMOTE_TRANSPORT=stdio` is the SSH-stdio fallback: it keeps the session inside authenticated SSH when UDP is blocked or unavailable. It remains available while UDP recovery is attempted.

## UDP transport health diagnostics

At debug logging, UDP transport-health lines report only state, elapsed progress ages, and bounded transport counters; they do not include payloads, addresses, keys, or identities. For a real remote attempt, `scripts/debug-remote-attach.sh --udp-health user@host` stores redacted transport-health lines and Linux `ss -u -m` socket-memory counters, then prints the exact collected file paths.

## Manual checks

Useful scenarios for validating resilience behavior:

- block and unblock the configured UDP port range;
- move between networks or change VPN state;
- suspend and resume the client machine;
- retry `vev attach user@host:session` for the same named session;
- compare logs for UDP silence, probing, SSH bootstrap failure, proxy expiry, and missing sessions.

Keep hostnames, usernames, and local paths out of shared logs and screenshots.
