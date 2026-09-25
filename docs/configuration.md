# Configuration

vev reads `~/.config/vev/config` (`$XDG_CONFIG_HOME` is respected).

- No file means defaults.
- Changes apply live within a couple of seconds.
- Exception: `web.listen` and `web.origin` need a gateway restart. See [browser terminal](web-terminal.md#listener-and-public-origin).
- Invalid values log a warning and fall back to the default.

The full list with defaults:

```text
# Browser gateway: HTTP listener and browser-facing origin, read at startup.
web.listen = 127.0.0.1:8778
web.origin = http://127.0.0.1:8778

# Theme: auto follows the client; dark/light use neutral built-in defaults.
theme = auto
# In auto mode with palette inheritance enabled, infer a terminal accent.
theme.palette = on
# Accent policy: auto or one exact ANSI slot number from 0 through 15.
theme.accent = auto

# Palette placement: auto, center, top-left, top, top-right, left, right,
# bottom-left, bottom, or bottom-right.
palette.anchor = center

# One prewarmed floating terminal per tab. An empty command uses the normal shell.
floating.command =
floating.width = 80%
floating.height = 80%

# Let directional keyboard focus continue past a pane edge. Both default off.
nav.overflow-tabs = off
nav.overflow-sessions = off

# Show the focused pane's terminal title in tab labels; off keeps the process name only.
tabs.terminal-title = on

# Remove a numbered (ephemeral) session when its last client exits.
ephemeral.close-on-exit = on

# Rebindable actions. Leave a line out to keep its built-in binding.
open-palette = alt+space
toggle-floating-pane = alt+f
jump-attention = alt+a
focus-pane-left = alt+h
focus-pane-right = alt+l
focus-pane-up = alt+k
focus-pane-down = alt+j
# Optional pane rearrangement actions are unbound by default.
# consume-or-expel-pane-left = alt+H
# consume-or-expel-pane-right = alt+L
# grow-pane-width, shrink-pane-width, grow-pane-height, shrink-pane-height,
# and equalize-panes are also unbound by default.
switch-tab-1 = alt+1
# ... through switch-tab-9 = alt+9

# Per-pane scrollback: decimal MB; zero disables history.
scrollback.megabytes = 50
# Optional additional line ceiling; zero means byte-only retention.
scrollback.lines = 10000

# Processes relaunched when a named session is restored. Empty disables relaunch.
snapshot.restore_processes = vi,vim,nvim,emacs,man,less,more,tail,top,htop,btop,claude,codex,pi,opencode

# Optional right bar anchors: commands run on the daemon host. Empty disables an anchor.
bar.top-right =
bar.bottom-right =
bar.interval = 5s

# Command palette codes: 2-3 letters or digits.
code.new-tab = CNT
code.new-session = CNS
code.create-ephemeral-session = CES
code.close-tab = CLT
code.split-right = SPR
code.split-left = SPL
code.split-up = SPU
code.split-down = SPD
code.consume-or-expel-pane-left = MPL
code.consume-or-expel-pane-right = MPR
code.stack-pane = STP
code.toggle-stack = TFS
code.close-pane = CFP
code.focus-pane-left = FPL
code.focus-pane-right = FPR
code.focus-pane-up = FPU
code.focus-pane-down = FPD
code.resize-pane = RSZ
code.grow-pane-width = GPW
code.shrink-pane-width = SPW
code.grow-pane-height = GPH
code.shrink-pane-height = SPH
code.equalize-panes = EQP
code.move-pane = MFP
code.move-tab = MAT
code.next-tab = NXT
code.previous-tab = PVT
code.back-session = BCK
code.jump-recent-session = JRS
code.session-picker = SSP
code.visual-mode = VIS
code.toggle-floating-pane = TFP
code.rename-session = RNS
code.rename-tab = RNT
code.detach = DET
code.notifications = NTC
code.yank-last-notification = YLN
```

## Scrollback

| Key | Range | Default | Zero means |
|---|---|---|---|
| `scrollback.megabytes` | 0–4096 MB | 50 | no history |
| `scrollback.lines` | 0–1,000,000 | 10,000 | no line limit, bytes only |

- Limits are per pane, including hidden floating panes.
- They count uncompressed history, not process memory.
- A reload applies to existing panes right away. Lowering a limit drops the oldest rows.
- Idle panes compress old history in the background. This saves memory but does not raise the limit.

## Ephemeral sessions

With `ephemeral.close-on-exit = on` (default), a numbered session is removed once its last client is gone for good:

- The client process ends (terminal closed, SIGHUP, or SIGTERM). The client tells the daemon on a best-effort basis; if that message is lost, the rule below applies.
- A client that lost its connection does not reconnect before the resume window (15 minutes) expires.

The session stays while any other client is attached. A detach (keybinding, palette, or picker switch) keeps it, and so does a network drop within the resume window. Named sessions are never affected. Set `off` to keep numbered sessions until you kill them.

Closing the terminal of a client running inside SSH also sends SIGHUP, so a dropped SSH connection removes the session. Use a named session for work you want to resume.

## Development environments

Set `VEV_ENV=<name>` to move every vev file into `.dev/<name>/` under the current directory. Use it to test a build without touching your real sessions.

```sh
VEV_ENV=dev go run .
VEV_ENV=dev go run . kill --sessions
rm -rf .dev/dev
```

- Name: up to 64 letters, digits, `.`, `_`, or `-`, starting with a letter or digit.
- The same name shares one daemon. Use different names to run builds side by side.
- Config, sessions, hosts, snapshots, and logs are isolated.
- If the socket path gets too long, sockets move to `/tmp/vev-<uid>-<hash>`. Everything else stays in `.dev/<name>/`.
- It is not a sandbox: pane processes see your real system.

## Remote hosts

```sh
vev host add user@host   # add a host
vev host rm user@host    # remove it
vev host list            # show hosts and their status
```

- The broker stores hosts in `~/.local/state/vev/broker/state/state.json`.
- Hosts connect over SSH, so your SSH config (aliases, keys) applies.
- Remote sessions appear as `session@host` in `vev ls --all` and in the picker.

### Warm remote transports

After you leave a remote, vev keeps its connection open for a while. Going back is then instant: no new SSH login or QUIC handshake.

Configure this in `~/.config/vev/broker.json`. The file is created on first use and read when the broker starts.

```json
{
  "marker": "vev.broker.offline/v1",
  "registrations": [],
  "warmTransports": 8,
  "warmIdleTimeout": "off"
}
```

- `warmTransports`: how many idle connections to keep (0–64, default 8). The oldest one closes first. `0` disables it.
- `warmIdleTimeout`: close an idle connection after this long, such as `5m` (max `24h`). Default `off`: no time limit.

Warm connections never keep the broker running.

## Logs and durable state

| What | Where |
|---|---|
| Logs (JSON lines, e.g. `vev-daemon.log`) | `~/.local/state/vev/` |
| Sessions and snapshots | `~/.local/state/vev/` |
| Broker hosts and logs | `~/.local/state/vev/broker/` |
| Socket and lock | `$XDG_RUNTIME_DIR/vev/` |

`$XDG_STATE_HOME` is respected. Set `VEV_LOG=debug|warn|error` to change the log level (default `info`).

If the daemon refuses to start because its session data is broken, do not edit the files. See [durable session recovery](durable-session-recovery.md).

## Theme

| Setting | Effect |
|---|---|
| `theme = auto` | Follow your terminal's colors and light/dark mode. |
| `theme = dark` / `light` | Neutral built-in colors. Ignores the two settings below. |
| `theme.palette = off` | Neutral colors, no accent. |
| `theme.accent = auto` | Pick an accent from your terminal's ANSI colors (blue if unsure). |
| `theme.accent = 0`–`15` | Use exactly that ANSI color as the accent. |

- vev only colors its own UI (bars, borders, palette). Pane content is never recolored.
- Tinted backgrounds need a truecolor terminal. Otherwise the accent only colors text and borders.
- vev follows light/dark switches your terminal reports. It cannot see palette changes the terminal does not report.

## Bindings

Valid keys: `alt+<char>`, `alt+space`, `alt+left/right/up/down`, `alt+1` to `alt+9`.

- Setting an action replaces all its default keys. For example, setting `focus-pane-left` removes Alt+Left too.
- Tab switching works with AZERTY and other layouts without extra config.
- `alt+[` is not supported: terminals use it for escape sequences.
- Cmd/Super is not supported. Map it in your terminal to `ESC` + a character, then bind `alt+<char>` in vev.

## Pane consume or expel

Moves the focused pane between columns, like the Niri window manager. It has no default key: use palette codes `MPL` / `MPR`, or bind `consume-or-expel-pane-left/right`.

- A pane alone in its column joins the column next to it.
- A pane sharing a column leaves it and becomes its own column.
- Works only with column layouts. Nested mixed splits are not supported.

## Navigation overflow

Both are `off` by default.

- `nav.overflow-tabs = on`: Alt+h/l at the edge of the tab moves to the next tab.
- `nav.overflow-sessions = on`: Alt+k/j at the edge moves to the next session (alphabetical).

No wrap-around. Only keyboard focus overflows, never the mouse or floating panes.

## Copy mode

Scroll up with the mouse to enter. Scroll back to the bottom to leave.

| Key | Action |
|---|---|
| `h` `j` `k` `l` | move |
| `w` `b` `e` | word motions |
| `v` or Space | start selection |
| `y` or Enter | copy |

With the mouse: drag to select, double-click to select a word.

```ini
# Unicode whitespace always separates words.
# The default is " -_@".
copy.word-separators = " -_@"
# Disable animated wheel scrolling (default: off).
copy.reduce-motion = off
```

- `copy.word-separators = ""` uses only whitespace.
- `copy.reduce-motion = on` scrolls three rows per wheel step, without animation.

## Responsive overlays

Below 80 columns, popups (palette, picker, floating terminal, prompts) become full-width drawers at the bottom of the screen. The background dims while a popup is open.

## Palette anchor

- Default: centered.
- `palette.anchor = auto`: bottom bar from 80 to 95 columns, right-side rail from 96 columns.
- Any other value places it at that spot. Below 80 columns it is always a bottom drawer.

## Floating terminal

- `floating.command` runs through your shell. Changes apply on the next launch.
- Below 80 columns, the terminal takes the full width and keeps `floating.height`.
- Floating terminals are not restored after a daemon restart.

## Bar anchors

`bar.top-right` and `bar.bottom-right` show the output of a command in the bar. Both are off by default.

- The command runs on the daemon host every `bar.interval` (minimum `1s`), with the same environment as your panes.
- vev shows the first line of output, without colors. On failure it keeps the last good value.
- Scripts receive `VEV_ANCHOR`, `VEV_SESSION`, `VEV_TAB`, `VEV_PANE`, `VEV_PANE_CWD`, and `VEV_COLS`.
- Failures are logged with the exit code and stderr. Exit 127 means "command not found".

Release installs ship two example scripts: `vev-bar-top-right` and `vev-bar-bottom-right`. The second one runs `git status` on every refresh; raise the interval on large repositories.
