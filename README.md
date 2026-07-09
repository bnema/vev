# vev
***Norwegian: web, weave, tissue***

There are many terminal multiplexers, but this one is mine. Minimal aesthetic, purely focused on performance, rendering high-output of vertical text, agentic coding, and remote work over SSH. Built in Go with no external deps as much as possible.

> [!IMPORTANT]
> This is totally in alpha.

## Install

```sh
go install github.com/bnema/vev@latest
```

From a checkout:

```sh
make install        # install vev and default bar scripts to Go's bin directory
go build -o vev .  # local binary
```

`make install` places `vev-bar-top-right` and `vev-bar-bottom-right` beside the installed `vev` binary. If you install another way, copy the scripts from `scripts/` into a directory on `PATH` or replace the bar commands in config.

## Usage

```text
vev                              attach to (or create) an ephemeral session
vev new <name>                   create and attach to a named session
vev attach <name>                attach to an existing session (alias: a)
vev attach user@host[:session]   attach to a remote vev daemon over SSH
vev ls                           list sessions (alias: list)
vev kill <name>                  kill a session
vev kill -- <name>               kill a name that starts with -
vev kill --all                   kill all sessions and stop the daemon
vev kill --daemon                stop the active vev daemon
vev --help                       show help (aliases: -h, help)
vev --version                    show version (alias: version)
```

Ephemeral sessions get a number (`0`, `1`, `2`, ...) and are temporary daemon-retained sessions. They survive detach and transient client connection loss while the daemon is running, but they are not persisted to disk and disappear when the daemon is killed, exits, or the machine restarts. Named sessions are persisted, survive daemon restarts, and can be resumed later. New and renamed session names must start with a letter or number and may contain only letters, numbers, dots, underscores, and dashes.

The daemon starts on first use and exits when no sessions remain. Only one client is attached to a session at a time; attaching again displaces the old client.

## Configuration

vev reads `$XDG_CONFIG_HOME/vev/config`, or `~/.config/vev/config` when `XDG_CONFIG_HOME` is unset. Missing files use defaults. The daemon reloads mtime/size changes while running, polling about every two seconds.

```text
# Theme: auto follows the client; dark/light force built-in palettes.
theme = auto

# Rebindable actions. Leave a line out to keep its built-in binding.
open-palette = alt+space
jump-attention = alt+a
focus-pane-left = alt+h
focus-pane-right = alt+l
focus-pane-up = alt+k
focus-pane-down = alt+j
switch-tab-1 = alt+1
switch-tab-2 = alt+2
switch-tab-3 = alt+3
switch-tab-4 = alt+4
switch-tab-5 = alt+5
switch-tab-6 = alt+6
switch-tab-7 = alt+7
switch-tab-8 = alt+8
switch-tab-9 = alt+9

# Named session snapshots restore layout, cwd, visible content, scrollback,
# and foreground processes from this allowlist. Omit the key for these defaults;
# set it empty to disable process relaunch.
snapshot.restore_processes = vi,vim,nvim,emacs,man,less,more,tail,top,htop,btop,claude,codex,pi,opencode

# Right bar anchors. Empty command values disable the matching anchor.
bar.top-right = vev-bar-top-right
bar.bottom-right = vev-bar-bottom-right
bar.interval = 5s

# Command palette codes are trimmed, uppercased, then checked as 2-3 letters or digits.
code.new-tab = CNT
code.new-session = CNS
code.close-tab = CLT
code.split-right = SPR
code.split-left = SPL
code.split-up = SPU
code.split-down = SPD
code.stack-pane = STP
code.toggle-stack = TST
code.close-pane = CLP
code.focus-pane-left = FPL
code.focus-pane-right = FPR
code.focus-pane-up = FPU
code.focus-pane-down = FPD
code.next-tab = NXT
code.previous-tab = PVT
code.back-session = BSK
code.forward-session = FSK
code.session-picker = SSP
code.visual-mode = VIS
code.rename-session = RNS
code.detach = DET
```

Configuring an action replaces all of its built-in aliases. In the example above, the focus-pane lines keep Alt+h/j/k/l but not the Alt+Arrow aliases; omit those lines to keep all built-ins.

Key specs support `alt+<char>`, `alt+space`, and `alt+left/right/up/down`; digit key specs support `alt+1` through `alt+9`, not `alt+0`. `jump-attention` first opens the oldest attention tab in the current session; if none exists, it opens the oldest attention tab in another session. Invalid entries are logged as warnings and skipped where possible; duplicate config keys use the last value, binding conflicts keep the later action's defaults, and command-code conflicts drop the conflicting override.

### Bar right anchors

The top and bottom bar right anchors run configurable commands on the daemon host. By default, `bar.top-right = vev-bar-top-right` prints CPU, memory, then hour text (for example `C 12% M 48% 09:41`), and `bar.bottom-right = vev-bar-bottom-right` prints compact git state for the focused pane's `VEV_PANE_CWD`. Set either value empty to disable that anchor, or set it to another command to replace it.

Both anchors share `bar.interval`, which defaults to `5s`; values below `1s` are clamped to `1s`. vev also refreshes around attach, resize, focus, and copy-feedback changes. Commands are bounded by a timeout and output limit. On failure or timeout, vev keeps the last good value; if there is none, the anchor is empty.

Scripts receive these environment variables when available: `VEV_ANCHOR` (`top-right` or `bottom-right`), `VEV_SESSION`, `VEV_TAB`, `VEV_PANE`, `VEV_PANE_CWD`, and `VEV_COLS`. vev reads stdout only, uses the first line only, treats it as plain UTF-8 display text, and strips ANSI escape sequences and other control characters. Unicode and Nerd Font glyphs are supported. The v1 API does not support colors, styles, JSON, streaming output, or per-anchor intervals. The default git script runs `git status --porcelain` each refresh, so increase `bar.interval` or replace the command if that is too expensive for a large repository.

## Remote attach

```sh
vev attach user@host
vev attach user@host:session
```

Remote attach uses SSH only to start a UDP proxy on the remote host; the session itself then talks directly to the remote host over UDP. `vev` must be installed on the remote host, and the host name you pass to `vev attach` must be reachable both by SSH for bootstrap and by UDP for the session transport. Omitting `:session` opens an ephemeral remote session. Remote sessions can resume after temporary network disconnects, such as sleep or Wi-Fi changes, while the daemon still has the session. Named sessions additionally persist across daemon restarts.

vev's resilient remote attach work is inspired by [mosh](https://mosh.org/)'s mobile-shell approach to UDP-based recovery across flaky networks. vev uses its own wire protocol and implementation; it is not compatible with mosh clients or servers and does not copy mosh protocol code.

The default remote transport uses UDP. Its proxy binds a port in a fixed range on the remote (default `61000-61023`), so a host firewall that only allows SSH will drop the session datagrams and attach fails with `remote UDP transport unavailable: probe UDP transport`. Allow inbound UDP for that range on the remote to fix it — for example `sudo ufw allow 61000:61023/udp`, or on a trusted Tailscale network `sudo ufw allow in on tailscale0`. Override the range with `VEV_UDP_PORT_RANGE` on the remote (e.g. `VEV_UDP_PORT_RANGE=51000-51009`, a single `VEV_UDP_PORT_RANGE=51000`, or `VEV_UDP_PORT_RANGE=0` for a random ephemeral port).

Set `VEV_REMOTE_TRANSPORT=stdio` to use SSH stdio compatibility mode on networks where direct UDP is blocked or the SSH target is only reachable through jump-host or SSH-config-only routing. Stdio mode keeps all traffic inside SSH and does not require inbound UDP, but it may not notice sleep or Wi-Fi loss promptly. See `docs/remote-resilience.md` for concise operational notes.

## Terminal color

When the attaching client reports truecolor support, vev advertises newly spawned panes as `TERM=xterm-direct` and exports `COLORTERM=truecolor` to child processes. Otherwise, panes use the conservative `TERM=xterm-256color` and do not receive `COLORTERM`. This works for local and remote attach because the client capability is carried inside vev's protocol rather than relying on SSH `SendEnv`/`AcceptEnv`.

Applications that use terminfo can detect direct color from `xterm-direct`; applications that use the common environment convention can use `COLORTERM=truecolor` when it is present. Panes restored from a saved snapshot start conservatively before any client attaches; after attach or resume, newly spawned panes use that client's reported capability.

## Terminal compatibility

vev keeps a server-side VT screen for each pane. It tracks scroll regions with DECOM origin mode, ANSI insert mode, cursor-position and mode-query reports, alternate-screen state, bracketed paste, mouse tracking, synchronized updates, OSC 9/777 notifications, OSC 52 clipboard set requests, and double-width cells. Resize and edit operations repair double-width cells so CJK or emoji glyph halves are not left behind.

## Keys

All bindings use Alt directly; there is no prefix key.

| Key | Action |
|---|---|
| Alt+Space | open the command palette |
| Alt+1 .. Alt+9 | switch to tab by number |
| Alt+h/j/k/l | focus pane left/down/up/right |
| Alt+Arrow | focus pane by direction |
| Alt+a | jump to a session with attention |

Some terminals encode Alt+Space as a non-breaking space instead of `ESC Space`; in those terminals, the palette binding depends on whether the terminal can send `ESC Space` for Alt+Space.

## Command palette

Open the palette with Alt+Space. Type a command code or fuzzy-match the label/description. Use Up/Down or Ctrl-N/Ctrl-P to move, Enter to run, and Esc/Ctrl-C to close.

Successful commands are promoted near the top the next time the palette opens.

| Code | Action |
|---|---|
| CNT | create new tab |
| CNS | create and switch to a named session |
| CLT | close current tab |
| SPR / SPL / SPU / SPD | split pane right / left / up / down |
| STP | stack a new pane |
| TST | toggle focused stack |
| CLP | close focused pane |
| FPL / FPR / FPU / FPD | focus pane left / right / up / down |
| NXT / PVT | switch to next / previous tab |
| BSK / FSK | move back / forward through recent sessions |
| SSP | open session picker |
| VIS | enter visual mode |
| RNS | rename session |
| DET | detach |

Rename prompts are prefilled with the current session name. Empty names and names already used by another session are rejected.

## Session picker

Open it with `SSP` in the palette. It lists sessions and tabs, previews the selected target when space allows, and marks stopped sessions.

| Key | Action |
|---|---|
| j / k, Up / Down | move selection |
| Enter | switch to or resume selected target |
| x | kill/delete selected target |
| q, Esc, Ctrl-C | close picker |

## Panes and tabs

Tabs hold pane layouts. Panes can be split left/right/up/down, stacked, focused by direction, and closed from the command palette. Clicking a pane focuses it.

The top bar shows numbered tabs. The bottom bar shows the active session followed by recent sessions, fading toward older entries. Ephemeral sessions are marked with `*`, for example `0*`.

## Visual mode and copy

Visual mode freezes a view over history while the program keeps running underneath. Enter it with the `VIS` palette command or by scrolling up with the mouse wheel.

| Key | Action |
|---|---|
| j / k, Up / Down | move one line |
| PgUp / PgDn | move one page |
| g / G | jump to top / bottom |
| v or Space | toggle line selection |
| y or Enter | copy selection and exit |
| q, Esc, Ctrl-C | exit without copying |

Copy uses OSC 52 clipboard sequences when supported by your terminal.

## Development

```sh
go install golang.org/x/tools/cmd/goimports@latest
go install github.com/vektra/mockery/v2@latest
make test   # go test ./... -race
make lint   # goimports check, then go vet ./...
make mocks  # regenerate mocks
```
