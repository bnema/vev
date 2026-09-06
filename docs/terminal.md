# Terminal support

## Color

Panes receive `TERM=xterm-direct` when the pane host can look up that terminfo entry, otherwise `TERM=xterm-256color`. Both profiles receive `COLORTERM=truecolor` and `TERM_PROGRAM=vev`. The profile describes vev's virtual terminal, not the terminal attached outside vev.

Before each PTY launch, vev runs `infocmp -x xterm-direct` on the daemon's host with the child's environment and working directory. This honors terminfo search settings such as `TERMINFO`, `TERMINFO_DIRS`, and `HOME`. The helper is resolved through the daemon's `PATH`. A missing helper, failed lookup, or one-second probe timeout selects the 256-color compatibility profile. Existing panes keep their original environment; newly opened or restored panes check again.

`xterm-direct` advertises 16,777,216 colors through terminfo and uses direct RGB color setters. With `xterm-256color`, applications need to recognize `COLORTERM=truecolor` or otherwise enable RGB output; `tput colors` reports only the entry's 256 indexed colors.

Each attachment independently detects its output color support from terminal environment signals. vev preserves RGB pane state and converts it to the xterm 256-color palette only for attachments that do not advertise truecolor. An attachment reporting a 256-color terminal receives a one-time warning before its first paint. Environment signals are advisory—especially through nested multiplexers—and do not advertise Kitty graphics support.

### Installing the direct-color entry

Check availability on the machine where the pane runs, including each remote vev host:

```sh
infocmp -x xterm-direct
tput -T xterm-direct colors
# Expected color count: 16777216
```

On Arch Linux and CachyOS, the `ncurses` package supplies the entry and the `infocmp`, `tput`, and `tic` tools:

```sh
sudo pacman -S ncurses
```

Other distributions may package extended terminfo entries separately. Install their terminfo database and ncurses utilities, then repeat the checks above.

For a user-local installation, export the entry on a machine that has it:

```sh
infocmp -x xterm-direct > xterm-direct.terminfo
```

Copy the file to the destination and compile it there:

```sh
tic -x -o ~/.terminfo xterm-direct.terminfo
```

Open a new pane to use the installed entry. The daemon also needs `infocmp` available in its `PATH`; restart the daemon if that search path needs to change. SSH destinations and containers entered from a pane need their own entry if they inherit `TERM=xterm-direct`. Installation on the pane host does not install it inside those environments.

## Kitty graphics

Kitty graphics support is attachment-local and explicitly declared only after a
bounded Kitty query and DA1 probe run through the client's existing terminal
input pump. Environment variables never enable graphics by themselves. The
probe preserves unrelated input, and late probe replies are quarantined rather
than sent to a pane.

A capable attachment renders the supported static/direct subset: current
streamed `kitten icat` PNG input, JPEG input after `kitten icat` converts it to
RGB/RGBA, bounded RGB/RGBA/PNG assets, ordinary placements, crop, offsets,
z-index, and supported deletes. vev allocates attachment-owned Kitty IDs, never
sends global deletes, clips placements to pane and attachment content, and
replays authoritative graphics through the same ordered output state chain as
ANSI text. Unsupported attachments retain text output and receive one bounded
warning when graphics are suppressed.

File, temporary-file, shared-memory, composition, relative placements, Unicode
placeholders, and graphics scrollback are unsupported. Animated images retain
their first frame as a static fallback.

## Environment

Local and direct CLI remote attachments supply the session's environment for
future PTY children. A later client-owned attachment updates that shared
environment; already-running processes keep their original environment. vev
replaces `TERM`, `COLORTERM`, `TERM_PROGRAM`, and `VEV`; `SHELL` selects the
command used to start each PTY.

Picker-selected remote attachments do not replace the session's environment
with the client's exports. When restarting a stopped remote session, vev uses
the destination daemon's environment and the persisted working directory. A
`CNS` handoff to another remote daemon also uses that daemon's environment,
starting in its home directory rather than the client's working directory. This
keeps `HOME`, `SHELL`, `PATH`, and XDG paths local to the destination host.

## VT compatibility

vev keeps a server-side VT screen per pane: scroll regions (DECOM), insert mode, cursor and mode-query reports, alternate screen, bracketed paste, mouse tracking, synchronized updates, OSC 9/777 notifications, OSC 52 clipboard, and double-width cells. Resizes repair double-width cells so CJK and emoji halves are never left behind.

## Session content size

The session's PTY content size follows the latest valid attach, resume, or
resize claim. Invalid or stale claims do not change shared PTY geometry. Each
attachment may have a different outer window and viewport; the winning claim
changes shared PTYs while preserving the other attachments' own presentations.
If the winning attachment detaches, the most recently claimed remaining valid
attachment becomes authoritative.

## Recovery

Named-session checkpoints preserve bounded scrollback and a recovery transcript containing the primary viewport followed by the active alternate viewport, when present. Each viewport trims only trailing untouched blank rows; leading, internal, styled, and otherwise touched blank rows remain.

On restore, vev appends the transcript after bounded scrollback as copy-mode history on a fresh blank VT screen. A resumed allowlisted process can redraw the live screen without erasing that recovered history. Recovery does not replay the PTY stream or restore the previous cursor or terminal modes.

## Alt+Space caveat

Some terminals encode Alt+Space as a non-breaking space instead of `ESC Space`. If the palette does not open, rebind `open-palette`.
