# Terminal support

## Color

When the client reports truecolor, new panes get `TERM=xterm-direct` and `COLORTERM=truecolor`; otherwise `TERM=xterm-256color`. The capability travels inside vev's own protocol, so it works over SSH without `SendEnv`/`AcceptEnv`. Panes restored from a snapshot derive the same capability from the daemon's startup environment until a client attaches and refreshes it.

## Environment

On each attach, the attaching environment becomes the session's source of truth for future PTY children. vev preserves it except that it replaces `TERM`, `COLORTERM`, `TERM_PROGRAM`, and `VEV`; `SHELL` selects the command used to start each PTY. Already-running processes keep their original environment.

The attaching client's exported environment is sent in the connection handshake and becomes the session's source of truth for future PTY children, including remote attaches. A later attachment updates that shared environment for children created afterward; already-running processes keep their original environment.

## VT compatibility

vev keeps a server-side VT screen per pane: scroll regions (DECOM), insert mode, cursor and mode-query reports, alternate screen, bracketed paste, mouse tracking, synchronized updates, OSC 9/777 notifications, OSC 52 clipboard, and double-width cells. Resizes repair double-width cells so CJK and emoji halves are never left behind.

## Session content size

The first valid attachment establishes the session's fixed PTY content size. Each
attachment may have a different outer window and viewport; resizing it changes
only its attachment-local presentation and does not resize shared PTYs.

## Recovery

Named-session checkpoints preserve bounded scrollback and a recovery transcript containing the primary viewport followed by the active alternate viewport, when present. Each viewport trims only trailing untouched blank rows; leading, internal, styled, and otherwise touched blank rows remain.

On restore, vev appends the transcript after bounded scrollback as copy-mode history on a fresh blank VT screen. A resumed allowlisted process can redraw the live screen without erasing that recovered history. Recovery does not replay the PTY stream or restore the previous cursor or terminal modes.

## Alt+Space caveat

Some terminals encode Alt+Space as a non-breaking space instead of `ESC Space`. If the palette does not open, rebind `open-palette`.
