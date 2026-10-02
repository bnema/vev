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

## Measuring scroll performance

`scripts/web-scroll-bench.cjs` measures how much browser work a fast history scroll costs. It starts its own gateway in a temporary profile, prints 10,000 colored lines, scrolls up and back down with the mouse wheel, and sums the page's main-thread time from a Chromium trace. It stops the gateway and deletes the profile when it ends. Linux only.

```sh
go build -o .dev/vev-web .
PLAYWRIGHT_MODULE=/path/to/node_modules/playwright VEV_BINARY=$PWD/.dev/vev-web \
  node scripts/web-scroll-bench.cjs --viewport 1638x987 --scale 1.25
```

| Option | Default | Meaning |
|---|---|---|
| `--lines` | 10000 | History lines printed before scrolling |
| `--notches` | 120 | Wheel notches: half up, half down, one every 16 ms |
| `--port` | 8799 | Gateway port on 127.0.0.1 |
| `--viewport` | 1600x1000 | Page size in CSS pixels (headless only) |
| `--scale` | 1 | Device pixel ratio, for example `1.25` for 125% zoom (headless only) |
| `--cdp` | | Use a running Chromium instead of headless, for example `http://127.0.0.1:9222` |

The output is JSON. `wireFrames`, `wireKB` and `avgFrameKB` count the WebSocket updates the page received during the scroll. `totalMs` is the main thread's busy time; `scriptMs`, `styleMs`, `layoutMs` and `paintMs` split it. `totalMsPerNotch` above 16 means the browser cannot keep up with a 60 Hz wheel. Compare two builds with the same options and run each a few times: the numbers vary by about 10%.

Headless Chromium has no GPU compositor, so it underestimates paint. For numbers that match what users see, start Chromium with `--remote-debugging-port=9222`, keep the benchmark tab in the foreground, and pass `--cdp`. The window size and zoom then come from that browser. Close it afterwards: the debugging port lets any local process control it.

To try a frontend change without rebuilding, set `WEB_APP_JS` or `WEB_RUNTIME_JS` to a local `app.js` or `terminal.js`.
