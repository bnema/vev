# Remote picker container acceptance

Run the disposable two-container acceptance harness with:

```sh
make remote-acceptance
```

The harness builds the current binary, starts a local client container and an
SSH-enabled remote daemon container on a private Docker network, and removes
all resources when it exits. Docker uses the normal CLI configuration, so
`DOCKER_HOST` or the active context selects a rootless daemon; the harness does
not assume a socket pathname.

Environment overrides:

- `VEV_HARNESS_BASE_IMAGE` selects the Docker base image used for both
  containers (default: `ubuntu:24.04`).
- `VEV_HARNESS_TARGET` selects the SSH target passed to the harness binary
  (default: `test@remote`).
- `VEV_HARNESS_ARTIFACT_DIR`, when set, writes a bounded
  `remote-picker-harness.json` containing output-state, checkpoint, and event
  metadata. It is unset by default; the artifact contains no terminal bytes,
  raw screen text, credentials, or target output. `run.sh` copies the report to
  that host directory before it removes the disposable containers.

Each logical transport has one persistent `vt.Screen` and a client-equivalent
output-state chain. The probe writes accepted full, incremental, and side-effect
frames to that screen, ACKs only accepted state-bearing frames, and retains a
bounded set of screen checkpoints plus non-sensitive event metadata. The local
picker phase uses separate local-picker and selected-remote probes to
characterize the existing `MsgAttachTarget` direct handoff and its event order.

The run exercises real SSH-stdio and UDP attachment, typed live catalog and
preview requests, tab creation/removal fencing, exact lifecycle fencing after
replacement, daemon restart resume, slow-preview cancellation, version
mismatch, and unreachable-host handling. It checks environment values in
memory without printing terminal viewport data.

Stopped-selector restoration and daemon-owned environment assertions are also
covered by the repository's daemon and route integration tests, where the
fixture can deterministically hold restoration at the stopped state.
