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
vev kill --all                   kill all sessions and stop the daemon
vev kill --daemon                stop the active vev daemon
vev --help                       show help
vev --version                    show version
```

Ephemeral sessions get a number (0, 1, 2...) and die when you detach. Named sessions survive detach and live until killed or their last tab closes. The daemon starts on first use and exits when no sessions remain.

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
| Alt+Space | open the command palette |
| Alt+1 .. Alt+9 | switch to tab by number |

Some terminals encode Alt+Space as a non-breaking space instead of `ESC Space`; in those terminals, the palette binding depends on whether the terminal can send `ESC Space` for Alt+Space.

## Command palette

Open the palette with Alt+Space, then type a command code or fuzzy-match its label. Use Up/Down or Ctrl-N/Ctrl-P to move through matches, Enter to run the selected command, and Esc to close the palette.

The status bar marks ephemeral sessions with `*`, for example `0*`. Ephemeral sessions are removed when their client detaches. Renaming an ephemeral session makes it a persistent named session and removes the marker.

| Code | Action |
|---|---|
| CNT | create new tab |
| CLT | close current tab |
| NXT | switch to next tab |
| PVT | switch to previous tab |
| SSP | switch session or tab |
| CPY | enter scrollback mode |
| RNS | rename session |
| DET | detach |

Rename prompts are prefilled with the current session name. Type text, use Backspace to edit, Enter to submit, or Esc/Ctrl-C to cancel. Empty names and names already used by another session are rejected; the prompt stays open and shows the validation error.

## Scrollback and visual copy

Scrollback mode freezes a view over history while the program keeps running underneath. Mouse wheel up enters scrollback mode and shows `[SCROLL]`; scrolling back down to the live bottom exits. When a line selection is active, the selected rows are highlighted and the status bar shows `[VISUAL]`.

| Key | Action |
|---|---|
| j / k, Up / Down | move one line |
| PgUp / PgDn | move one page |
| g / G | jump to top / bottom |
| Space | toggle visual line selection |
| y or Enter | copy selection and exit |
| q, Esc, Ctrl-C | exit without copying |

Copying uses OSC 52, so the selection lands in your terminal's clipboard even across SSH. After a successful copy, the right side of the status bar reports `copied N chars to clipboard` until the next screen update. If the selection exceeds the OSC 52 copy limit, the status bar reports `selection too large to copy` instead.

## Mouse

vev enables terminal mouse reporting while attached. Mouse input on the status row is reserved for vev and is not sent to the child. In scrollback mode, the wheel scrolls the scrollback view. When the child enables mouse reporting, SGR mouse events are forwarded to it. Otherwise, wheel events in the alternate screen are translated to Up/Down arrow keys (`ESC[A` / `ESC[B`). This translation intentionally does not honor DECCKM application-cursor mode (`ESC OA` / `ESC OB`). An extremely rare input read split exactly between `ESC[` and `<` in an SGR mouse sequence may leak the partial `ESC[` to the child.

vev follows the child application's cursor visibility and style requests and appends a hardware cursor update after each paint. Visible cursors blink as requested by the terminal/application state.

## Development

```sh
go test ./... -race
```

`pkg/` (VT emulator, renderer) never imports `internal/`; a boundary test enforces this. Format Go code with `goimports`. Benchmark notes live in `docs/performance.md`.
