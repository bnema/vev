# Remote navigation acceptance

Run `make remote-acceptance` with Docker, Python 3 and `ssh-keygen` installed.

The runner builds `scripts/demo/Dockerfile` from the current worktree and starts
two disposable Arch containers on a private network. Per-run client and host
keys authenticate SSH with host-key pinning. No host credentials, state or
sockets are mounted. Containers, image, network and temporary keys are removed
on success, failure or interruption.

`navigation_repro.py CLIENT_CONTAINER REMOTE_CONTAINER` runs the original
hybrid-UDP regression: local A → local B → remote → local A with qualified
palette labels, exactly one imported local destination, the original session
lifecycle at return, and a successfully committed navigation action.

`acceptance.py CLIENT_CONTAINER REMOTE_CONTAINER SCENARIO[@stdio|@udp]` runs
the table-driven baseline matrix. Every scenario asserts committed outcomes
(action IDs, exact lifecycle/session/focus context, observed published
output); sending a key is never success. Transport selection passes through
the fixture process environment (`VEV_REMOTE_TRANSPORT=stdio` or unset for
UDP). Direct-remote scenarios use `--remote` with no local daemon and never
create a local home session as setup. `VEV_ACCEPTANCE_TOPOLOGY=local|direct`
selects which daemon serves the client-picker scenario: `local` keeps the
attachment and the picker rows in the client container, `direct` serves both
from the remote container. The client needs a pinned `remote`
SSH alias and `vev host add remote`. Scripts create uniquely named sessions
and detach their clients; the container owner handles session cleanup.

| Scenario | Modes |
|---|---|
| `local-palette-cycle` | M1 local IPC: palette open/action/close, committed output, stable lifecycle |
| `direct-remote-ephemeral@udp` | M2 direct remote UDP: fresh attach, committed output, stable lifecycle |
| `direct-remote-ephemeral@stdio` | M3 direct remote explicit stdio: same semantics, no UDP parking |
| `hybrid-exact-return@udp` | M4 hybrid UDP: local→remote→exact local return with committed action |
| `hybrid-exact-return@stdio` | M5 hybrid stdio: same shape across close/dial |
| `client-picker-navigate@udp` (topology `local`) | M1: client-owned picker presented by the client, searched commit, cancel without a route change |
| `client-picker-navigate@udp` (topology `direct`) | M2: same over the remote serving daemon, no local daemon involved |
| `client-picker-navigate@stdio` (topology `direct`) | M3: same across stdio |

A failed assertion fails the command. This is a targeted regression, not an
exhaustive transport or geometry matrix.
