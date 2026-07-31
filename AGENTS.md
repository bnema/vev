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
- Install dev tools once with `go install golang.org/x/tools/cmd/goimports@latest` and `go install github.com/vektra/mockery/v3@v3.7.1`.
- Use Mockery v3 consistently across Go projects.
- `make test` runs `go test ./... -race`.
- `make lint` checks `goimports -l`, then `go vet`.
- Enable daemon pprof with `VEV_PPROF_ADDR=127.0.0.1:6060 vev --daemon`.
- Set `VEV_LOG=debug|warn|error` to change log verbosity (default info).
- Logs live in `$XDG_STATE_HOME/vev`, or `~/.local/state/vev` when unset.
- Use feature worktrees under `.worktrees/<branch-name>`. Remove the worktree once its branch is merged or abandoned (`git worktree remove <path>`).
- Create vev-owned private directories with `pkg/safedir.EnsurePrivate`, not `os.MkdirAll`.

## Architecture rules

Hexagonal boundaries are enforced by `boundary_test.go`.

- `pkg/` is reusable and must not import `internal/`.
- `internal/usecase/` must not import `internal/adapters/`.
- Usecases depend on `internal/ports` and `internal/domain`.

Layer map:

- `main.go` → `internal/app`: CLI parsing, wiring, daemon startup, hidden subcommands.
- `internal/ports`: interfaces and wire protocol.
- `internal/usecase/daemon`: sessions, tabs, panes, VT screens, renderer shadows, daemon features.
- `internal/usecase/client`: raw-mode thin client; writes output bytes verbatim and interprets nothing.
- `internal/adapters`: IPC, SSH stdio, PTY, terminal, and clock implementations.

Before touching daemon teardown paths, read the lock-ordering notes at the top of `internal/usecase/daemon/client.go`.

## Ports and mocks

- Define cross-layer interfaces only in `internal/ports`.
- Adapters implement ports; usecases and `internal/app` consume ports.
- Tests should prefer generated `portsmocks.MockX` with `.EXPECT()`.
- Hand-written fakes are okay only when mocks make the test harder to understand.
- After changing a port, run `make mocks` and commit the generated updates.
- Time-dependent daemon code must use `ports.Clock`/`ports.Timer`, not direct wall-clock APIs.

## Tests

- Write Go tests as table-driven tests by default.
- Keep wire tests byte-for-byte and include truncated-prefix and trailing-garbage coverage.
- Prefer fake clocks over sleeps.

## Wire protocol

Wire payload types/codecs live in `internal/ports/frame.go` and `internal/ports/wire.go`. Connection framing lives in `internal/adapters/ipc/transport.go`.

- IPC frames on a connection are 4-byte big-endian length, 1 type byte, then payload.
- Client message types currently occupy `1–13` (`MsgOutputResetRequest` is `13`); server types currently occupy `16–23` (`MsgSessionMeta` is `23`). The reserved bands are `1–15` and `16+` respectively.
- Version negotiation requires strict equality.
- `Hello.Version` and `CommandRequest.Version` must stay first so their version peekers work.
- Bump `ProtocolVersion` for any message layout change.

## Session flow

- Ephemeral numbered sessions survive detach while the daemon retains them, but are not persisted.
- Named sessions survive headless and persist across daemon restarts.
- The daemon starts on first use and exits when the last session ends.
- Local attach sends `Hello`, receives `Welcome`, then pumps `Input`/`Resize` and `Output` frames.
- Remote attach runs `ssh -- host vev _stdio [session]` and proxies the same protocol over stdio.
- Only one client may attach to a session; a new attach replaces the old client.
