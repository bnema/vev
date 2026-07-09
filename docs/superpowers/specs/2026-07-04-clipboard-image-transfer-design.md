# Clipboard image transfer (local → remote pane)

Date: 2026-07-04 · Status: approved (approach A)

## Problem

When attached to a remote session, pasting an image for a TUI coding agent (pi, claude) is impossible: the agent's Ctrl+V handler reads the *remote* clipboard, which doesn't exist on a headless host (pi's chain — `wl-paste` → `xclip` → native — returns null in every branch without `WAYLAND_DISPLAY`/`DISPLAY`; no OSC 52 image path exists). The image lives on the laptop.

## Approach

Replicate the *end state* of pi's local Ctrl+V, which is: write the image to a temp file and insert the literal file path as text at the editor cursor (pi-mono `interactive-mode.ts:2536-2555`). vev does the same across the wire:

1. **Client (remote attach only):** intercept `0x16` (Ctrl+V) on stdin. Run `wl-paste --list-types` (1s timeout). If an `image/*` type is present, pick by preference `png > jpeg > webp > gif` (same order as pi), read it with `wl-paste --type <mime> --no-newline`, and send one `ImagePush` frame. If no image / wl-paste missing / any error: forward `0x16` verbatim (today's behavior).
2. **Daemon:** on `ImagePush`, write the bytes to `os.TempDir()/vev-clip-<unixts>.<ext>` with mode 0600, remember the path on the session, and inject the path text into the focused pane's PTY — wrapped in `\x1b[200~…\x1b[201~` iff the pane's `Screen.BracketedPasteMode()` is true (first production use of the mode-2004 tracking), bare text otherwise. Injection goes through the same write path as regular input (`writeToPane`).
3. **Cleanup:** files recorded on the session are removed when the session ends (same lifecycle as the session teardown path). Best-effort; `/tmp` is the backstop.

## Wire protocol

- New client-originated message `MsgImagePush` (next free type in the 1–15 range) in `internal/ports`: `{ Mime string, Data []byte }`, hand-marshalled big-endian like the rest of `wire.go`.
- Bump `ProtocolVersion` (strict-equality negotiation; `Hello.Version` stays first field).
- Byte-for-byte golden tests + truncated-prefix / trailing-garbage invariants in `wire_test.go`, per repo protocol rules.
- Caps: client refuses images > 10 MiB (notifies via stderr/status); frame limit is 16 MiB so one frame always suffices. Daemon independently rejects > 10 MiB payloads (defense against old/foreign clients).

## Scope decisions

- **Remote attaches only.** On local attach, `0x16` passes through untouched — the agent reads the local clipboard natively and its own temp-file flow works.
- **Wayland only** on the client side (user's environment). The clipboard read sits behind a small client-side seam (interface in the client package or `ports` if it must cross layers) so `xclip`/OSC-52 backends can be added later without protocol changes.
- **No image conversion.** Bytes are transferred as-is; unsupported exotic formats are the agent's problem (pi converts BMP itself locally; that path simply isn't reachable here).
- **Known caveat:** while an image is on the local clipboard, Ctrl+V won't reach remote pane apps that bind it themselves (e.g. vim visual block). Text/empty clipboard → passthrough unchanged.

## Testing

- Client: fake clipboard runner (seam above) — image present → one ImagePush frame, no `0x16` forwarded; no image → `0x16` forwarded; oversized → refused + `0x16` not swallowed silently.
- Ports: golden wire tests as above.
- Daemon: ImagePush → file created 0600 with exact bytes, path injected with/without bracketed-paste wrapping depending on pane mode; session end removes the file; oversized rejected.
- Manual: end-to-end against the scratchpad harness (fake-ssh `_stdio` path) with a real PNG.

## Out of scope

Remote→local image transfer, multi-image batching, X11 client support, progress UI for large images.
