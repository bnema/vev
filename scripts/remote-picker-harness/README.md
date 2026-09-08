# Container acceptance (Arch, three roles)

Run the disposable three-container acceptance scaffold with:

```sh
make remote-acceptance
```

The runner builds the real Arch image (`scripts/demo/Dockerfile`) from the
current worktree, starts local, remote-a, and remote-b roles with
fixture-only SSH credentials and distinct A/B host keys on a private Docker
network, verifies the Arch/SSH/binary prerequisites, records run identity
into `matrix.json`, and removes all resources when it exits. The E3 case
matrix and E4 vision review (plan P5) are not implemented yet: the scaffold
exits 0 on prerequisites alone and must not be mistaken for V6.

Legacy note: an older Go controller in this directory encoded a previous
two-container proxy proof; it is not executed by `run.sh` and its
assertions are obsolete. Docker uses the normal CLI configuration, so
`DOCKER_HOST` or the active context selects a rootless daemon; the harness does
not assume a socket pathname.

Environment overrides:

- `VEV_HARNESS_ARTIFACT_DIR`, when set, writes `matrix.json` containing
  run identity (image ID, source HEAD, binary hash, protocol version)
  with every acceptance case marked pending. It is unset by default; no
  report is written then. `run.sh` copies the report to that host
  directory before it removes the disposable containers.

Stopped-selector restoration and daemon-owned environment assertions are
covered by the repository's daemon and route integration tests, where the
fixture can deterministically hold restoration at the stopped state. The
container run exercises prerequisite verification only: SSH-stdio and UDP
attachment, catalog, fencing, and resume checks belong to later matrix
cases, not to this scaffold.
