# Browser terminal

```sh
vev --web-daemon
```

The command starts a detached web gateway on **127.0.0.1:8778** and prints a private access link. Open that link in a browser. Running the command again prints the same link when that gateway is reachable. The port is fixed; another process or development profile using it prevents startup.

The gateway connects ordinary client attachments to the vev daemon. The daemon owns sessions, tabs, panes, palettes and PTYs. Each browser connection has its own terminal view. Closing the page detaches that view without killing its session shells. Reloading or choosing Reconnect creates a new ephemeral session; previous sessions remain available through the session picker (`SSP`). Repeated reloads can therefore accumulate shells: close unwanted sessions explicitly. Automatic reattachment is not implemented, and input is never automatically replayed.

## Controls

The header shows a connection-state indicator and the number of connected browser terminal views, refreshed every two seconds. Background session shells are not counted. Button icons come from Lucide (ISC license, available at `/icons-license`).

- Type directly into the terminal, including composed text and pasted plain text.
- Use **Alt+Space**, or the **Palette** button, for commands such as `CNT`, `SPR`, `SPD` and `SSP`.
- Use **Alt+h/j/k/l** to focus panes and **Alt+1…9** to switch tabs.
- Click panes to focus them. Mouse reports and wheel events follow the multiplexer input path.
- The grid fits the browser window, up to 512 columns and 256 rows.
- On touch devices, **Select** preserves native text selection without terminal mouse capture; **Keys** enables interaction and opens the keyboard. The initial view does not force the keyboard open.
- **Ctrl+Shift+F6** moves focus out of terminal input to the Palette button for keyboard navigation.

Touch controls have 44px targets. The layout accounts for safe areas and the visual viewport while preserving pinch zoom. Real-device keyboard, composition and selection behavior still requires validation on iOS Safari and Android Chrome; desktop device emulation is not a substitute. Browser scrollback and touch-to-terminal scrolling are not implemented.

Browser and window-manager shortcuts can intercept keys before vev receives them. The Palette button avoids the common Alt+Space window-menu conflict. Native clipboard shortcuts remain available; terminal copy-mode clipboard export is not implemented by the web adapter.

## Isolated visual testing

From a source checkout:

```sh
go build -o .dev/vev-web .
env VEV_ENV=web-visual .dev/vev-web --web-daemon
```

This uses the checkout's `.dev/web-visual` configuration, state and runtime directories instead of the normal installation. Keep the printed credential out of screenshots and reports. To inspect these test sessions from a physical terminal:

```sh
env VEV_ENV=web-visual .dev/vev-web ls
```

Useful visual checks: palette search, horizontal and vertical splits, tab switching, pane focus, shell input, full-screen applications, paste, scrolling, window resize and reload. Record browser/version, viewport size, action and expected result when reporting a problem.

`scripts/web-visual-smoke.cjs` exercises the local gateway with an installed Playwright module. Set `PLAYWRIGHT_MODULE` to that module's absolute path, `VEV_BINARY` to the built executable and `VEV_ENV=web-visual`, and optionally `CHROMIUM_PATH` to an installed Chromium executable. It creates test sessions and panes; run it only against an isolated profile. `scripts/web-access-smoke.cjs` uses the same variables to check the view counter and token rotation; it revokes all browser access on that isolated gateway.

## Security and implementation

The listener binds IPv4 loopback only. A fresh random 256-bit credential lives only in gateway memory and expires when that process stops. `vev --web-renew-token` replaces it on a running gateway, invalidates old links and cookies, disconnects browser views, and prints the new link without stopping session shells. The CLI retrieves or renews access through a profile-scoped Linux abstract Unix socket restricted by same-user peer credentials; no credential file is written. The access link carries it in a URL fragment; the page removes the fragment and exchanges it for an HttpOnly SameSite cookie. HTTP Host and Origin checks, authenticated WebSockets, a restrictive Content Security Policy, bounded input and connection limits protect the local terminal endpoint. Do not publish the credential or expose the port through a reverse proxy.

The browser uses the dependency-free `vev-vt` DOM renderer and a small native JavaScript transport adapter. Go owns rendering state and terminal input encoding. Assets are embedded in the binary; there is no CDN, bundler, HTMX or frontend framework.

The web gateway is a separate long-lived process; stopping the session daemon does not stop the gateway. Send SIGTERM to the gateway process to stop it. The initial web frontend does not render Kitty graphics, export OSC 52 clipboard requests or guarantee full grapheme/emoji fidelity. Remote session navigation uses the existing client path but is not yet covered by the browser visual smoke checks.
