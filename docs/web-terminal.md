# Browser terminal

```sh
vev --web-daemon
```

This starts a background web server on `127.0.0.1:8778` and prints a private link. Open it in a browser to get the normal vev UI: palette, tabs, and splits.

- Each browser tab is its own client. Closing the page detaches; shells keep running.
- Reloading creates a **new** session. Find old ones with the session picker (`SSP`) and close the ones you don't need.
- Running the command again prints the same link.

## Access from another machine

vev serves plain HTTP. For HTTPS, put a reverse proxy in front of it (for example Caddy, nginx, or a NetBird app).

```sh
vev --web-daemon --web-listen 127.0.0.1:8778 \
  --web-origin https://terminal.example.internal
```

Or in `~/.config/vev/config`:

```ini
web.listen = 127.0.0.1:8778
web.origin = https://terminal.example.internal
```

- `web.listen`: IP and port to listen on, such as `100.64.0.10:8778` or `[::1]:8778`. No hostnames.
- `web.origin`: the URL users type in the browser, without a path. Required when not listening on loopback.
- Flags override config. Restart the gateway to apply changes; shells survive.

The proxy must:

- keep the original `Host` and `Origin` headers,
- forward WebSocket upgrades,
- serve vev at the root path (no `/vev/` prefix).

> [!WARNING]
> Access to this page is access to your shell. Prefer loopback or one private IP. Avoid `0.0.0.0` unless a firewall restricts access.

## Controls

| Input | Action |
|---|---|
| Alt+Space or **Palette** button | command palette |
| Alt+h/j/k/l | focus pane |
| Alt+1…9 | switch tab |
| Click | focus pane |
| Shift + wheel | scroll 10× faster |
| Ctrl+Shift+F6 | move focus out of the terminal to the Palette button |

- The terminal fits the window, up to 512×256 cells.
- On touch devices, **Select** allows native text selection and **Keys** opens the keyboard.
- Your browser or window manager may catch some shortcuts first. The Palette button avoids the common Alt+Space conflict.

Not supported yet: browser scrollback, touch scrolling, copy-mode clipboard export, Kitty images, OSC 52, and full emoji fidelity.

## Security

- The access token is random, kept only in memory, and gone when the gateway stops.
- The link carries it once; the page then swaps it for an HttpOnly cookie.
- `vev --web-renew-token` revokes all links and browser sessions and prints a new link. Shells keep running.
- Host and Origin checks, a strict Content Security Policy, and input limits protect the endpoint.
- Everything is embedded in the binary. No CDN or frontend framework.

## Stop the gateway

The gateway is a separate process. Stopping the session daemon does not stop it. Send it `SIGTERM`.

## Testing a build

Use an isolated profile so tests don't touch your real sessions:

```sh
go build -o .dev/vev-web .
VEV_ENV=web-visual .dev/vev-web --web-daemon
VEV_ENV=web-visual .dev/vev-web ls
```

Keep the printed link out of screenshots.

`scripts/web-visual-smoke.cjs` and `scripts/web-access-smoke.cjs` run Playwright checks. Set `PLAYWRIGHT_MODULE`, `VEV_BINARY`, `VEV_ENV=web-visual`, and optionally `CHROMIUM_PATH`. Run them only against an isolated profile: they create sessions and revoke access.
