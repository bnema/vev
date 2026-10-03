# Terminal support

## Colors

Panes get truecolor when the pane host has the `xterm-direct` terminfo entry.

| Pane host has `xterm-direct`? | `TERM` in panes |
|---|---|
| yes | `xterm-direct` |
| no | `xterm-256color` |

Both set `COLORTERM=truecolor` and `TERM_PROGRAM=vev`.

- vev checks with `infocmp -x xterm-direct` before each new pane (1 second timeout). Existing panes keep their `TERM`.
- With `xterm-256color`, apps must read `COLORTERM` to use RGB colors.

### Outer terminal color modes

vev picks a color mode for each attachment, so several clients on one session can use different modes. Modes: `truecolor`, `256`, `16`, `monochrome`.

Detection reads the client's `TERM` and `COLORTERM`. The first matching row wins:

| Order | Signal | Mode |
|---|---|---|
| 1 | `COLORTERM` is `truecolor` or `24bit`, or `TERM` is `xterm-direct` or ends in `-direct` | truecolor |
| 2 | `TERM=xterm-kitty` inside kitty | truecolor |
| 3 | `TERM` is `dumb`, `vt52`, `vt100`, `vt102`, `vt220`, or ends in `-m` or `-mono` | monochrome |
| 4 | `TERM` contains `256color` | 256 |
| 5 | `TERM` is `linux`, `ansi`, `cons25`, or ends in `-16color` or `-color` | 16 (8-color terminals use the first 8) |
| 6 | anything else | 256 (unverified guess) |

When detection finds a terminal with fewer than truecolor colors, vev shows a one-time notice. The notice is not shown for the unverified guess or for a forced mode.

### Override the mode

| Where | Example | Notes |
|---|---|---|
| `VEV_COLORS` environment variable | `VEV_COLORS=16 vev` | Wins over the config key. An invalid value is ignored and logged as a warning. |
| `terminal.colors` in the [configuration](configuration.md) | `terminal.colors = mono` | Applies to every client that has no `VEV_COLORS`. |

Values: `auto`, `truecolor`, `256`, `16`, `mono`. `auto` (or an empty value) means detection. A forced mode never shows the notice.

`VEV_COLORS` and `terminal.colors` are read when the client starts; restart the client to apply a change. A config reload in the running daemon does not change attached clients.

### What each mode changes

Pane content:

| Mode | Pane colors |
|---|---|
| truecolor | Unchanged. |
| 256 | RGB colors map to the xterm 256-color palette. |
| 16 | RGB and indexed colors 16-255 map to the nearest of the 16 ANSI colors. Underline color is dropped. |
| monochrome | All colors are dropped. Attributes (bold, underline, reverse, and so on) stay. |

vev's own UI (bars, borders, pickers, overlays):

| Mode | UI |
|---|---|
| truecolor, 256 | Full theme. |
| 16 | No tinted backgrounds. Active and selected items use bold and reverse, secondary text uses faint. The accent is a terminal palette slot. |
| monochrome | Same as 16 with no color at all. |

The browser terminal and the headless `--ui-driver` always use truecolor, whatever `TERM`, `COLORTERM`, `VEV_COLORS`, or `terminal.colors` say.

### Install `xterm-direct`

Check on each machine that runs panes, including remote vev hosts:

```sh
tput -T xterm-direct colors   # expect 16777216
```

On Arch Linux: `sudo pacman -S ncurses`. On other distributions, install the ncurses terminfo package.

Without root, copy the entry from a machine that has it:

```sh
infocmp -x xterm-direct > xterm-direct.terminfo   # on the source machine
tic -x -o ~/.terminfo xterm-direct.terminfo       # on the target machine
```

Then open a new pane. SSH hosts and containers entered from a pane need the entry too.

## Kitty graphics

Images work when your outer terminal supports the Kitty graphics protocol. vev detects this with a short query at attach time; environment variables never enable it.

Supported:

- `kitten icat` with PNG and JPEG images.
- Placement, cropping, offsets, z-index, and deletes.

Not supported: file or shared-memory transfer, animations (first frame only), Unicode placeholders, and images in scrollback.

Terminals without Kitty graphics get text only and one warning.

## Kitty keyboard

When your outer terminal supports the kitty keyboard protocol (kitty, foot, ghostty, WezTerm, recent Alacritty), vev enables it at startup. Ctrl+1 to Ctrl+9 then switch to the 1st to 9th previous session in the status-bar history. Ctrl+1 goes back to the session you used last.

- Detection uses the same short query as Kitty graphics. Other terminals keep their usual keys, and Ctrl+digits reach the pane unchanged.
- It works with AZERTY and other layouts.
- Programs in panes that ask for the kitty keyboard protocol (Neovim, Helix, fish) receive it. Others get classic key bytes.
- Turn it off with `keyboard.kitty-protocol = off` in the [configuration](configuration.md).
- If vev is killed and your shell gets odd keys, run `printf '\e[<u'` to reset the terminal.

## Environment

- A session keeps the environment it was created with, including `SHELL`, `PATH`, and `HOME`. A session restored after a daemon restart takes the environment of the first local client that attaches.
- Desktop and login variables follow the client that last attached:
  - `WAYLAND_DISPLAY`, `DISPLAY`, `XAUTHORITY`, `XDG_CURRENT_DESKTOP`, and `XDG_SESSION_*` move as one group. A client with a non-empty `WAYLAND_DISPLAY` or `DISPLAY` replaces the whole group, so switching to another compositor or an X11-only desktop drops the old values. A client without a display (console) or connected over SSH leaves the group alone, even with X forwarding: after `ssh -X`, set `DISPLAY` yourself in the panes that need it.
  - `DBUS_SESSION_BUS_ADDRESS` and `XDG_RUNTIME_DIR` update when the client has them and are never removed. With systemd, all your graphical sessions share them.
  - `SSH_AUTH_SOCK`, `SSH_AGENT_PID`, `SSH_CONNECTION`, `SSH_CLIENT`, and `SSH_TTY` always match the last client and are removed when it has none.
- New panes get these values. Running processes keep their own environment; a shell pulls the current values with `vev env <fish|sh>` from its prompt hook:

  ```fish
  # ~/.config/fish/conf.d/vev.fish
  if set -q VEV
      function __vev_env --on-event fish_prompt
          vev env fish 2>/dev/null | source
      end
  end
  ```

  ```zsh
  # ~/.zshrc (bash: add the eval to PROMPT_COMMAND)
  if [[ -n $VEV ]]; then
    autoload -Uz add-zsh-hook
    __vev_env() { eval "$(vev env sh 2>/dev/null)" }
    add-zsh-hook precmd __vev_env
  fi
  ```

  The hook overrides values you set by hand in that shell, such as `SSH_AUTH_SOCK` after `eval (ssh-agent)`.
- `vev cmd env` lists the set variables as `KEY=value` for reading. Scripts should use `vev cmd env --json`, which also lists the variables to unset as `null`.
- In fish, the hook erases only global variables; a universal variable (`set -U`) with the same name shows through when the session has no value for it.
- vev always sets `TERM`, `COLORTERM`, `TERM_PROGRAM`, and `VEV`. `SHELL` picks the shell.
- A remote session opened from the picker uses the remote host's own environment, not yours. This keeps `HOME`, `PATH`, and `SHELL` correct on that host.

## VT features

Each pane has a server-side VT screen with scroll regions, alternate screen, bracketed paste, mouse tracking, synchronized updates, OSC 9/777 notifications, OSC 52 clipboard, and wide (CJK and emoji) characters.

## Pane size with several clients

When several clients attach to one session, pane size follows the client that attached or resized last. Each client keeps its own window, tab, and focus. When that client leaves, the previous one takes over.

## Alt+Space

Some terminals send a non-breaking space for Alt+Space. If the palette does not open, rebind `open-palette` in the [configuration](configuration.md).
