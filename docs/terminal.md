# Terminal support

## Color

When the client reports truecolor, new panes get `TERM=xterm-direct` and `COLORTERM=truecolor`; otherwise `TERM=xterm-256color`. The capability travels inside vev's own protocol, so it works over SSH without `SendEnv`/`AcceptEnv`. Panes restored from a snapshot start conservative until a client attaches.

## VT compatibility

vev keeps a server-side VT screen per pane: scroll regions (DECOM), insert mode, cursor and mode-query reports, alternate screen, bracketed paste, mouse tracking, synchronized updates, OSC 9/777 notifications, OSC 52 clipboard, and double-width cells. Resizes repair double-width cells so CJK and emoji halves are never left behind.

## Alt+Space caveat

Some terminals encode Alt+Space as a non-breaking space instead of `ESC Space`. If the palette does not open, rebind `open-palette`.
