# vev agent guide

## Project

vev is a Linux terminal multiplexer written in Go. It has a per-user daemon, a thin client, server-side rendering, and minimal socket/SSH diffs.

## Commands

```sh
go build -o vev .
make test
go test ./internal/usecase/daemon -race -run TestName
make lint
make mocks
go test ./pkg/renderer ./pkg/vt ./internal/adapters/ipc ./internal/usecase/daemon -run '^$' -bench=. -benchmem
```

- Format with **goimports**.
- Install dev tools once with `go install golang.org/x/tools/cmd/goimports@latest` and `go install github.com/vektra/mockery/v3@v3.7.4`.
- Use Mockery v3 consistently across Go projects.
- `make test` runs `go test ./... -race`.
- `make lint` checks `goimports -l`, then `go vet`.
- Enable daemon pprof with `VEV_PPROF_ADDR=127.0.0.1:6060 vev --daemon`.
- Set `VEV_LOG=debug|warn|error` to change log verbosity (default info).
- Logs live in `$XDG_STATE_HOME/vev`, or `~/.local/state/vev` when unset.
- Use feature worktrees under `.worktrees/<branch-name>`. Remove the worktree once its branch is merged or abandoned (`git worktree remove <path>`).
- Create vev-owned private directories with `pkg/safedir.EnsurePrivate`, not `os.MkdirAll`.

## Architecture rules

The exhaustive production matrix and separate test-import policy are enforced by `boundary_test.go`; package ownership is documented in `docs/architecture.md`.

- `pkg/` is reusable and never imports `internal/`.
- Production use cases import only `internal/ports`, semantic `internal/protocol` packages, `internal/domain`, and approved sibling use cases.
- Production use cases never import `internal/protocol/wire`, concrete adapters, `internal/app`, `internal/persist`, or `internal/platform`.
- `internal/ports` owns application seams, not codecs, raw frames, environment policy, or worker implementations.

Layer map:

- `main.go` → `internal/app`: CLI parsing, composition, daemon startup, and hidden subcommands.
- `internal/domain`: pure shared values and terminal capability policy.
- `internal/protocol`: typed session messages; `protocol/catalogue` owns remote JSON schema; `protocol/wire` owns binary encoding and raw carriage contracts.
- `internal/ports`: typed application connections and infrastructure-facing use-case seams.
- `internal/adapters/sessionwire`: typed message ↔ raw frame adaptation.
- `internal/usecase/daemon`: sessions, tabs, panes, VT screens, renderer shadows, and daemon features.
- `internal/usecase/client`: raw-mode thin client; writes output bytes verbatim and interprets nothing.
- `internal/adapters`: IPC, UDP, SSH stdio, PTY, terminal, clock, and observability implementations.

Before touching daemon teardown paths, read the lock-ordering notes at the top of `internal/usecase/daemon/client.go`.

## Ports and mocks

- Define application cross-layer interfaces in `internal/ports`; raw frame carriage interfaces live in `internal/protocol/wire`.
- Adapters implement ports and raw carriage contracts; use cases consume application connections through `internal/ports` and typed semantic values from `internal/protocol`, never raw carriage contracts.
- Tests should prefer generated `portsmocks.MockX` with `.EXPECT()`.
- Hand-written fakes are okay only when mocks make the test harder to understand.
- After changing a port, run `make mocks` and commit the generated updates.
- Time-dependent daemon code must use `ports.Clock`/`ports.Timer`, not direct wall-clock APIs.

## Tests

- Write Go tests as table-driven tests by default.
- Keep wire tests byte-for-byte and include truncated-prefix and trailing-garbage coverage.
- Prefer fake clocks over sleeps.

## Wire protocol

Typed messages and negotiated version live in `internal/protocol`. Remote discovery JSON lives in `internal/protocol/catalogue`. Message IDs, frames, strict codecs, compression, encoded bounds, and raw carriage interfaces live in `internal/protocol/wire`. Connection framing lives in concrete carriage adapters such as `internal/adapters/ipc`.

- IPC frames on a connection are 4-byte big-endian length, 1 type byte, then payload.
- Client message types occupy `1–13`, `15`, `32–33`, and `35` (`MsgParkedRouteRequest`); server types occupy `16–23`, `25–31`, `34`, and `36` (`MsgParkedRouteResponse`). Types `14` and `24` remain reserved.
- Version negotiation requires strict equality.
- `Hello.Version` and `CommandRequest.Version` must stay first so their version peekers work.
- Bump `ProtocolVersion` for any message layout change.

## Session flow

- Ephemeral numbered sessions survive detach while the daemon retains them, but are not persisted.
- Named sessions survive headless and persist across daemon restarts.
- The daemon starts on first use and exits when the last session ends.
- Each connection has a 15-second handshake budget from connect through the initial committed publication.
- Local and remote attach use the same typed `Hello`/`Welcome` session protocol. `sessionwire` translates typed traffic to raw frames carried by IPC, UDP, or SSH stdio.
- Remote attach opens a direct connection to the selected daemon over UDP by default; `VEV_REMOTE_TRANSPORT=stdio` explicitly selects an SSH-only carriage.
- A session owns shared PTYs, VT state, tabs, panes, PTY content geometry selected by the latest valid attachment claim, and ordered mutations. Each attachment owns its window/view, copy and overlay state, rendering/output state, and reconnect lifecycle; when the latest claimant detaches, the most recently claimed remaining attachment becomes authoritative.
- Command requests have a 10-second result deadline and are tracked per connection.
