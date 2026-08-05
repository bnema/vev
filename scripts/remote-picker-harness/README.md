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

The run exercises real SSH-stdio and UDP attachment, typed live catalog and
preview requests, tab creation/removal fencing, exact lifecycle fencing after
replacement, daemon restart resume, slow-preview cancellation, version
mismatch, and unreachable-host handling. It checks environment values in
memory without printing terminal viewport data.

Stopped-selector restoration and daemon-owned environment assertions are also
covered by the repository's daemon and route integration tests, where the
fixture can deterministically hold restoration at the stopped state.
