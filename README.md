# vev

A terminal multiplexer for Linux, written in Go with no runtime dependencies beyond `golang.org/x/sys` and `golang.org/x/term`. Sessions run in a per-user daemon; the client is a thin pipe. Rendering happens server-side with damage tracking, so only minimal diffs cross the socket (or the SSH link).

## Install

```sh
go install github.com/bnema/vev@latest
```

Or from a checkout: `go build -o vev .`

## Usage

```
vev                              attach to (or create) an ephemeral session
vev new <name>                   create and attach to a named session
vev attach <name>                attach to an existing named session (alias: a)
vev attach user@host[:session]   attach to a remote vev daemon over SSH
vev ls                           list sessions
vev kill <name>                  kill a named session
vev --help                       show help
vev --version                    show version
```

Ephemeral sessions get a number (0, 1, 2...) and die when you detach. Named sessions survive detach and live until killed or their last window closes. The daemon starts on first use and exits when no sessions remain.

Remote attach runs `ssh user@host vev _stdio ...` under the hood, so vev must be installed on the remote host and reachable via your normal SSH config. Each remote attach opens one ssh connection. To make reconnects near-instant, enable OpenSSH connection reuse for your hosts in `~/.ssh/config`:

```
Host *
    ControlMaster auto
    ControlPath ~/.ssh/cm-%r@%h:%p
    ControlPersist 10m
```

Omitting `:session` opens an ephemeral session on the remote.

## Keys

All bindings use the Alt modifier directly, no prefix key.

| Key | Action |
|---|---|
| Alt+c | new window |
| Alt+n / Alt+p | next / previous window |
| Alt+1 .. Alt+9 | select window by number |
| Alt+x | close window (last window ends the session) |
| Alt+d | detach |
| Alt+r | promote the current ephemeral session to named |
| Alt+u | enter copy mode |

## Copy mode

Copy mode freezes a view over the scrollback while the program keeps running underneath. The status bar shows `[COPY]` and the cursor position.

| Key | Action |
|---|---|
| j / k, Up / Down | move one line |
| PgUp / PgDn | move one page |
| g / G | jump to top / bottom |
| Space | toggle line selection |
| y or Enter | copy selection and exit |
| q, Esc, Ctrl-C | exit without copying |

Copying uses OSC 52, so the selection lands in your terminal's clipboard even across SSH.

## Development

```sh
go test ./... -race
```

`pkg/` (VT emulator, renderer) never imports `internal/`; a boundary test enforces this. Format Go code with `goimports`. Benchmark notes live in `docs/performance.md`.
