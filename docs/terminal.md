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
- If your outer terminal does not support truecolor, vev converts colors to 256 and shows a one-time warning.

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

- New panes inherit the environment of the client that last attached. Running processes keep theirs.
- vev always sets `TERM`, `COLORTERM`, `TERM_PROGRAM`, and `VEV`. `SHELL` picks the shell.
- A remote session opened from the picker uses the remote host's own environment, not yours. This keeps `HOME`, `PATH`, and `SHELL` correct on that host.

## VT features

Each pane has a server-side VT screen with scroll regions, alternate screen, bracketed paste, mouse tracking, synchronized updates, OSC 9/777 notifications, OSC 52 clipboard, and wide (CJK and emoji) characters.

## Pane size with several clients

When several clients attach to one session, pane size follows the client that attached or resized last. Each client keeps its own window, tab, and focus. When that client leaves, the previous one takes over.

## Alt+Space

Some terminals send a non-breaking space for Alt+Space. If the palette does not open, rebind `open-palette` in the [configuration](configuration.md).
