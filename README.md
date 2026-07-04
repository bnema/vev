# vev — Norwegian: web, weave, tissue

There are many terminal multiplexers, but this one is mine. Written in Go, with no runtime dependencies beyond `golang.org/x/sys` and `golang.org/x/term`. Purely focused on performance, rendering high-output vertical text, agentic coding, and remote work over SSH.

> [!IMPORTANT]
> This is totally in alpha.

## Install

```sh
go install github.com/bnema/vev@latest
```

From a checkout:

```sh
make install        # go install .
go build -o vev .  # local binary
```

## Usage

```text
vev                              attach to (or create) an ephemeral session
vev new <name>                   create and attach to a named session
vev attach <name>                attach to an existing named session (alias: a)
vev attach user@host[:session]   attach to a remote vev daemon over SSH
vev ls                           list sessions (alias: list)
vev kill <name>                  kill a named session
vev kill -- <name>               kill a name that starts with -
vev kill --all                   kill all sessions and stop the daemon
vev kill --daemon                stop the active vev daemon
vev --help                       show help (aliases: -h, help)
vev --version                    show version (alias: version)
```

Ephemeral sessions get a number (`0`, `1`, `2`, ...) and die when you detach. Named sessions survive detach and can be resumed later. The daemon starts on first use and exits when no sessions remain.

Only one client is attached to a session at a time; attaching again displaces the old client.

## Remote attach

```sh
vev attach user@host
vev attach user@host:session
```

Remote attach runs `ssh user@host vev _stdio ...` under the hood, so `vev` must be installed on the remote host and reachable through your normal SSH config. Omitting `:session` opens an ephemeral remote session.

For faster reconnects, enable OpenSSH connection reuse:

```sshconfig
Host *
    ControlMaster auto
    ControlPath ~/.ssh/cm-%r@%h:%p
    ControlPersist 10m
```

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

The top bar shows numbered tabs. The bottom bar shows the session name; ephemeral sessions are marked with `*`, for example `0*`.

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
make test   # go test ./... -race
make lint   # goimports check, then go vet ./...
make mocks  # regenerate mocks
```
