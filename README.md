<h1 align="center">vev</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue?style=flat-square" alt="License: MIT"></a>
  <a href="https://github.com/bnema/vev/releases"><img src="https://img.shields.io/badge/platforms-Linux%20%7C%20macOS-blue?style=flat-square" alt="Platforms: Linux and macOS"></a>
  <a href="https://github.com/bnema/vev/commits/main"><img src="https://badgen.net/github/last-commit/bnema/vev/main?icon=github" alt="Last commit"></a>
  <a href="https://github.com/bnema/vev/stargazers"><img src="https://badgen.net/github/stars/bnema/vev?icon=github" alt="GitHub stars"></a>
</p>

<p align="center"><em>Norwegian: to weave</em></p>

<p align="center">A terminal multiplexer for Linux and macOS.<br>
Like tmux, minus the prefix key. Like mosh, minus mosh. One binary.</p>

<p align="center"><img src="docs/assets/demo.gif" alt="vev demo: local and remote sessions, splits, stacked panes, floating window, notifications, detach and re-attach" width="800"></p>

---

## Install

Install a pre-built release on Linux x86_64/arm64 or macOS Apple silicon (arm64):

```sh
curl -fsSL https://raw.githubusercontent.com/bnema/vev/main/install.sh | sh
```

On Arch Linux, install [`vev-bin`](https://aur.archlinux.org/packages/vev-bin) from the AUR:

```sh
yay -S vev-bin
# or
paru -S vev-bin
```

Alternatively, build and install the binary with Go:

```sh
go install github.com/bnema/vev@latest
```

The default UI runs entirely from the `vev` binary. The pre-built installer also installs optional external bar anchors, which only take effect when configured in `~/.config/vev/config`.

## Usage

```text
vev                              create an ephemeral session
vev new <name>                   create a named session
vev attach <name>                attach to a session (alias: a)
vev attach user@host[:session]   attach to a remote daemon over SSH
vev ls                           list local sessions
vev ls <host>                    list sessions on a known remote host
vev ls --all                     list local and remote sessions
vev host add <host>              add a pinned remote host
vev host rm <host>               remove a pinned remote host
vev host list                    list known remote hosts
vev kill <name>                  kill a session (--all kills everything)
```

The daemon starts on first use and exits with the last session. Ephemeral sessions are numbered, survive detach, and disappear with the daemon. Named sessions persist across daemon restarts and come back with their layout, recovered terminal transcript, and allowlisted processes.

A session may have multiple attachments. The session owns the PTYs, VT state, tabs, panes, shared PTY content geometry, and ordered mutations. Each attachment owns its window, view, copy mode, overlays, rendering/output state, and reconnect lifecycle, so attachments can focus different tabs or panes without changing one another's view. The latest valid, non-superseded attachment attach, resume, or resize claim controls shared PTY geometry; detaching that attachment falls back to the most recently claimed remaining valid attachment while preserving every attachment's own outer window.

## Keys

No prefix key; everything is Alt.

| Key | Action |
|---|---|
| Alt+Space | command palette |
| Alt+f | floating terminal for the current tab |
| Alt+1 .. 9 | switch tab |
| Alt+h/j/k/l, Alt+Arrow | focus pane |
| Alt+a | jump to a session needing attention |

The palette does the rest: type a short code (`SPR` split right, `CNT` new tab, `SSP` session picker, ...) or fuzzy-search the command list. `MFP` (`Move pane to tab`) and `MAT` (`Move tab to session`) open live-destination pickers; the unbound pane consume/expel actions are discoverable there as `MPL` and `MPR`. Named active and stopped sessions are fuzzy-searchable for navigation, and selecting a stopped session resumes it. Scroll up with the mouse to enter scrollback; vim keys move, `v` selects, `y` copies via OSC 52.

## Remote attach

```sh
vev attach user@host[:session]
```

The client opens a direct connection to the selected remote daemon. UDP is the default: SSH bootstraps an authenticated UDP endpoint, then the session runs over that connection and resumes after sleep or Wi-Fi changes. vev must be installed on the remote. If your firewall only allows SSH, open the UDP range first (default `61000:61023`, override with `VEV_UDP_PORT_RANGE`):

```sh
sudo ufw allow 61000:61023/udp
```

Where UDP is not an option, set `VEV_REMOTE_TRANSPORT=stdio` to keep the direct connection inside SSH; this is an explicit transport choice. Every connection has a 15-second handshake deadline, and each command request has a 10-second deadline. Details in [docs/remote-resilience.md](docs/remote-resilience.md).

List sessions on a known host (OpenSSH resolves aliases from your SSH config):

```sh
vev ls arch
vev ls --all
```

Remote session names appear as `session@host`. `vev ls --all` prints local sessions first, then remote sessions in host order. If one host fails, vev still prints the rest and exits non-zero.

Manage pinned hosts in the unified host state file through the CLI:

```sh
vev host add arch
vev host rm arch
vev host list
```

Successful attaches learn hosts into `$XDG_STATE_HOME/vev/hosts.json`; that file stores both pinned and learned hosts. See [docs/configuration.md](docs/configuration.md#remote-hosts).

## Scripting

`vev cmd` runs control commands against a running daemon; it never starts one. For example:

```sh
vev cmd split-right
vev cmd toast -l warn "build failed"
vev cmd list-panes --json
```

Target a session explicitly with `-s` (`vev cmd -s work new-tab`). Inside a pane, `--self` targets that pane; it cannot be combined with `-s`. Otherwise vev uses `$VEV` inside a pane, then the only live session; ambiguous targets fail. Run `vev cmd --help` for the scriptable command list and `vev cmd <command> --help` for command usage.

Move the focused pane or active tab with these exact forms:

```text
vev cmd [-s <source-session>] [--self] move-pane <destination-session> <destination-tab-id>
vev cmd [-s <source-session>] [--self] move-tab <destination-session>
```

Use `vev cmd -s <destination-session> list-tabs` to find stable destination tab IDs. Destinations must be live, but may be named or ephemeral; stopped sessions are not eligible. A moved pane is split to the right of the destination tab's focused pane and becomes that tab's internal focus without activating the tab. A moved tab is appended to the destination in the background. If the move empties the source session, its attachment follows the moved pane or tab and activates the destination; existing destination attachments keep their own views.

Moving the final tiled pane out of a tab is rejected while that tab has a floating pane slot; close the floating pane or move the whole tab instead. Named-session persistence is best-effort across a move, not crash-atomic across source and destination snapshots.

## Configuration

Optional file at `~/.config/vev/config`, reloaded live. Themes, key bindings, palette codes, the floating terminal, status bar scripts: see [docs/configuration.md](docs/configuration.md). Terminal color and VT compatibility notes live in [docs/terminal.md](docs/terminal.md).

## Development

```sh
make test   # go test ./... -race
make lint   # goimports check, go vet
make mocks  # regenerate mocks
make demo   # regenerate docs/assets/demo.gif (needs Docker)
```
