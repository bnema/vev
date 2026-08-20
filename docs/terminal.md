# Terminal support

## Color

Panes always receive vev's stable compatibility contract: `TERM=xterm-256color`, `COLORTERM=truecolor`, and `TERM_PROGRAM=vev`. This contract does not mirror the terminal attached outside vev.

Each attachment independently detects its output color support from terminal environment signals. vev preserves RGB pane state and converts it to the xterm 256-color palette only for attachments that do not advertise truecolor. An attachment reporting a 256-color terminal receives a one-time warning before its first paint. Environment signals are advisory—especially through nested multiplexers—and do not advertise Kitty graphics support.

## Kitty graphics

Kitty graphics support is attachment-local and explicitly declared only after a
bounded Kitty query and DA1 probe run through the client's existing terminal
input pump. Environment variables never enable graphics by themselves. The
probe preserves unrelated input, and late probe replies are quarantined rather
than sent to a pane.

A capable attachment renders the supported static/direct subset: current
streamed `kitten icat` PNG input, bounded RGB/RGBA/PNG assets, ordinary
placements, crop, offsets, z-index, and supported deletes. vev allocates
attachment-owned Kitty IDs, never sends global deletes, clips placements to
pane and attachment content, and replays authoritative graphics through the
same ordered output state chain as ANSI text. Unsupported attachments retain
text output and receive one bounded warning when graphics are suppressed.

File, temporary-file, shared-memory, animation, composition, relative
placements, Unicode placeholders, and graphics scrollback are unsupported.

## Environment

On each attach, the attaching environment becomes the session's source of truth for future PTY children. vev preserves it except that it replaces `TERM`, `COLORTERM`, `TERM_PROGRAM`, and `VEV`; `SHELL` selects the command used to start each PTY. Already-running processes keep their original environment.

The attaching client's exported environment is sent in the connection handshake and becomes the session's source of truth for future PTY children, including remote attaches. A later attachment updates that shared environment for children created afterward; already-running processes keep their original environment.

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
