<h1 align="center">vev</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue?style=flat-square" alt="License: MIT"></a>
  <a href="https://github.com/bnema/vev/releases"><img src="https://img.shields.io/badge/platforms-Linux%20%7C%20macOS-blue?style=flat-square" alt="Platforms: Linux and macOS"></a>
  <a href="https://github.com/bnema/vev/commits/main"><img src="https://badgen.net/github/last-commit/bnema/vev/main?icon=github" alt="Last commit"></a>
  <a href="https://github.com/bnema/vev/stargazers"><img src="https://badgen.net/github/stars/bnema/vev?icon=github" alt="GitHub stars"></a>
</p>

<p align="center"><em>Norwegian: to weave</em></p>

<p align="center">A minimal, remote friendly terminal multiplexer for Linux and macOS.</p>

<p align="center"><img src="docs/assets/demo.gif" alt="vev demo: local and remote sessions, splits, stacked panes, floating window, notifications, detach and re-attach" width="800"></p>

---

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/bnema/vev/main/install.sh | sh
# Arch Linux: yay -S vev-bin
# or: go install github.com/bnema/vev@latest
```

Releases support Linux x86_64/arm64 and macOS Apple silicon.

## Usage

```text
vev                              create an ephemeral session
vev new <name>                   create a named session
vev attach <name>                attach to a local session (alias: a)
vev attach user@host[:session]   attach to a remote daemon
vev ls [<host>|--all]            list sessions
vev host add|rm <host>           manage remote hosts
vev host list                    list remote hosts
vev kill <name>|--all            kill sessions
vev ui-driver [options]          drive a headless attachment over JSONL
vev --ui-observe ...              expose passive observation for an attach
vev --ui-control ...              expose observation and input control
```

The daemon starts on first use and exits after the last session. Numbered sessions survive detach; named sessions also survive daemon restarts. Exiting the final shell removes that session and returns to the most recently used session; with no previous session, vev exits. See [durable-session recovery](docs/durable-session-recovery.md).

> [!NOTE]
> A release that bumps vev's protocol version resets named sessions saved by an older protocol. Session names are retained, but layouts, tabs, terminal history, recovery transcripts, and process-recovery state are discarded. Each local or remote daemon applies this reset to its own sessions when it first starts with the new protocol.

## Keys and palette

No prefix key; bindings use Alt.

| Key | Action |
|---|---|
| Alt+Space | command palette |
| Alt+f | floating terminal |
| Alt+1 … 9 | switch tab |
| Alt+h/j/k/l or Alt+Arrow | focus pane |
| Alt+a | jump to a session needing attention |

Type a code or fuzzy-search the palette. Common default codes:

| Code | Action | Code | Action |
|---|---|---|---|
| `CNT` | new tab | `CNS` | new named session |
| `SPR` / `SPL` | split right / left | `SPU` / `SPD` | split up / down |
| `STP` / `TFS` | stack / toggle stack | `TFP` | floating terminal |
| `MFP` / `MAT` | move pane / tab | `SSP` | session picker |
| `DET` | detach | `NTC` | notifications |

In the session picker, press `/` to fuzzy-search session names, hosts, tab names, and tab details; use Arrow keys or Ctrl+n/Ctrl+p to move between matches. Escape clears the query, and a second Escape leaves search.

When a client is attached to both a local and a remote daemon, `CNS` and `CNS <name>` list eligible daemon destinations in the existing palette. If two or more destinations exist, choose one explicitly with Up or Down before pressing Enter. The unqualified choice appears only when the client has an exact local route; remote choices remain host-qualified. A direct remote start without a proven local route never labels its home route as local.

Scroll up to enter copy mode; use vim motions, `v` to select, and `y` to copy. All codes and bindings are configurable in [configuration](docs/configuration.md).

## Remote attach

```sh
vev attach user@host[:session]
```

SSH bootstraps an authenticated direct UDP connection that resumes after sleep or network changes. Set `VEV_REMOTE_TRANSPORT=stdio` to use SSH only. The remote host needs vev installed; see [remote resilience](docs/remote-resilience.md) for firewall, transport, and host-list details.

## UI driver

`vev ui-driver` provides bounded capture, key/text input, and deterministic waits for a headless attachment. Existing clients stay unchanged unless `--ui-observe` or `--ui-control` is explicitly enabled. See [UI driver](docs/ui-driver.md) for the JSONL contract, private socket bridge, completion semantics, and isolated endpoint configuration.

## Scripting

`vev cmd` controls a running daemon without starting one:

```sh
vev cmd split-right
vev cmd toast -l warn "build failed"
vev cmd list-panes --json
```

Use `vev cmd --help` for commands and targeting options.

## Documentation

- [Configuration](docs/configuration.md): bindings, palette, theme, overlays, and bar anchors
- [Terminal compatibility](docs/terminal.md)
- [Remote resilience](docs/remote-resilience.md)
- [Durable session recovery](docs/durable-session-recovery.md)
- [UI driver](docs/ui-driver.md): headless capture/control and opt-in interactive observation
- [Performance](docs/performance.md)

## Development

Use `VEV_ENV` to keep development runs separate from your installed vev and from other builds:

```sh
VEV_ENV=dev go run .
VEV_ENV=dev go run . kill --all
rm -rf .dev/dev
```

Each name shares one daemon and its vev-owned files under `.dev/<name>/`; use separate names to run binaries side by side. This isolates vev configuration, runtime files, durable sessions, hosts, snapshots, and logs, but it is not an OS, filesystem, or network sandbox.

```sh
make test   # go test ./... -race
make lint   # goimports check, go vet
make mocks  # regenerate mocks
make demo   # regenerate docs/assets/demo.gif (needs Docker)
```
