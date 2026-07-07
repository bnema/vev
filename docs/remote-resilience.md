# Remote resilience

Remote attach starts with SSH bootstrap and then carries the vev session over authenticated UDP. The design is inspired by mosh's mobile-shell resilience, but vev uses its own protocol and is not compatible with mosh clients or servers.

## Operational model

- SSH bootstrap starts or finds the remote UDP endpoint.
- UDP carries normal session traffic after bootstrap.
- Named sessions remain recoverable across daemon restarts; ephemeral sessions remain recoverable only while the daemon retains them.
- `VEV_REMOTE_TRANSPORT=stdio` keeps all traffic inside SSH when direct UDP is blocked.

## Recovery states

Resilience work uses these user-facing states:

| State | Meaning |
| --- | --- |
| `connected` | authenticated UDP traffic is flowing |
| `degraded` | traffic is delayed or incomplete, but recovery is still expected |
| `probing` | vev is trying alternate UDP paths before using SSH again |
| `resuming` | a replacement transport is attaching to the same session |
| `offline` | no path currently works, but retries continue |
| `expired` | the session or recovery window is no longer available |

When a terminal is in raw mode, recovery messages should appear as vev status text rather than as bytes written into the remote program's output stream.

## Manual checks

Useful scenarios for validating resilience behavior:

- block and unblock the configured UDP port range;
- move between networks or change VPN state;
- suspend and resume the client machine;
- retry `vev attach user@host:session` for the same named session;
- compare logs for UDP silence, probing, SSH bootstrap failure, proxy expiry, and missing sessions.

Keep hostnames, usernames, and local paths out of shared logs and screenshots.
