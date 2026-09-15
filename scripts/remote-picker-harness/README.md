# Remote navigation acceptance

Run `make remote-acceptance` with Docker, Python 3 and `ssh-keygen` installed.

The runner builds `scripts/demo/Dockerfile` from the current worktree and starts
two disposable Arch containers on a private network. Per-run client and host
keys authenticate SSH with host-key pinning. No host credentials, state or
sockets are mounted. Containers, image, network and temporary keys are removed
on success, failure or interruption.

`navigation_repro.py CLIENT_CONTAINER REMOTE_CONTAINER` runs the original
hybrid transport regression: local A → local B → remote → local A with qualified
palette labels, exactly one imported local destination, the original session
lifecycle at return, and a successfully committed navigation action.

`acceptance.py CLIENT_CONTAINER REMOTE_CONTAINER SCENARIO[@stdio|@quic]` runs
the table-driven baseline matrix. Every scenario asserts committed outcomes
(action IDs, exact lifecycle/session/focus context, observed published
output); sending a key is never success. Transport selection is forwarded
into the client container (`docker exec -e VEV_REMOTE_TRANSPORT=stdio`, or
unset for QUIC); passing it to the `docker` CLI process alone never reached
the driver. Direct-remote scenarios use `--remote remote --session NAME`
against a named fixture session, so the attached target is a named exact
lifecycle rather than an ephemeral session, and never create a local home
session as setup. `VEV_ACCEPTANCE_TOPOLOGY=local|direct` selects which daemon
serves the client-picker scenario: `local` keeps the attachment and the
picker rows in the client container, `direct` serves both from the remote
container. The client needs a pinned `remote` SSH alias and
`vev host add remote`. Scripts create uniquely named sessions and detach
their clients; the container owner handles session cleanup.

Client and daemon debug logging is on (`VEV_LOG=debug`); the remote accepts
`VEV_LOG` over SSH so `_stdio`/`_quic-proxy` and the daemon they spawn log at
debug too. After every run, sanitized per-run logs and scenario evidence are
copied out of the containers to `/tmp/vev-acceptance-artifacts-*` (override
with `VEV_ACCEPTANCE_ARTIFACTS_DIR`). Only structured vev state logs and
committed contexts are captured: no host state, sockets, or key material.

| Scenario | Modes |
|---|---|
| `local-palette-cycle` | M1 local IPC: palette open/action/close, committed output, stable lifecycle |
| `direct-remote-named@quic` | M2 direct named remote QUIC: exact fixture lifecycle, committed output, stable lifecycle |
| `direct-remote-named@stdio` | M3 direct named remote explicit stdio: same semantics over SSH stdio |
| `warm-reuse@quic` | Warm hybrid QUIC: local→named remote→local→same remote in one Runner, same remote lifecycle, retained and new committed output, exactly one suspend/activate and one remote attach |
| `warm-reuse@stdio` | Warm hybrid stdio: same shape across the retained SSH process |
| `hybrid-exact-return@quic` | M4 hybrid QUIC: local→remote→exact local return with committed action |
| `hybrid-exact-return@stdio` | M5 hybrid stdio: same shape across close/dial |
| `client-picker-navigate-local@quic` | M1: client-owned picker presented by the client, searched commit, cancel without a route change |
| `client-picker-navigate-direct@quic` | M2: same over the remote serving daemon, no local daemon involved |
| `client-picker-navigate-direct@stdio` | M3: same across stdio |

Warm reuse asserts deterministic no-redial evidence rather than timing: the
client debug log must carry exactly one `remote attachment suspended` and one
`warm remote attachment activated` for the origin/session and zero
`warm remote activation failed`, and the remote daemon log must show exactly
one `client attached` for that session. A cold fallback or second bootstrap
(or, when the remote proxy log is available, a second proxy start) fails the
scenario. The remote committed lifecycle and the retained pane output before
and after the round trip are compared directly.

A failed assertion fails the command. This is a targeted regression, not an
exhaustive transport or geometry matrix. For a focused run, set
`VEV_ACCEPTANCE_SCENARIOS` to a space-separated subset of the scenario names
above (for example `warm-reuse@quic warm-reuse@stdio`); the containers, keys
and artifacts still come from the same setup.

Known gap: `client-picker-navigate` fails independently of this matrix. After
entering the search and the exact target name, the client-owned picker commits
the current session instead of the searched target. It fails in isolation and
at any log level; it predates the named-direct refactor, which previously
aborted the full run before this scenario was reached. The named-direct, warm
reuse and hybrid scenarios above are green.
