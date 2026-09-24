# UI driver

`vev --ui-driver` lets a script or test read the rendered screen and send keys, over JSON Lines on stdin/stdout. It uses the normal vev client, so what you capture is what a user sees.

## Start

```sh
vev --ui-driver                      # new numbered session, 80x24
vev --ui-driver --session work       # named local session
vev --ui-driver --remote user@host   # remote session on a configured host
vev --ui-driver --picker             # start in the session picker
```

| Option | Meaning |
|---|---|
| `--session NAME` | create that named session (with `--remote`: open that remote session) |
| `--remote HOST` | target a configured remote host |
| `--cols N --rows N` | viewport size, max 512×256 |
| `--picker` | start in the picker (not with `--session`/`--remote`) |
| `--socket PATH` | connect to an existing client instead (see below) |

The driver uses your normal broker and daemon. It never starts, stops, or reconfigures them. Closing stdin detaches; sessions keep running.

## Protocol

The first line is the discovery response:

```json
{"version":1,"id":0,"result":{"attachment":"<handle>","generation":0,"control":true,"status":"picker"}}
```

`status` is one of:

| Status | Meaning | Keys accepted? |
|---|---|---|
| `picker` | in the local picker | no |
| `connecting` | attaching | no |
| `attached` | attached to a session | yes |

Wait for `attached`, then use the `generation` it reports for input. Never assume `generation == 1`.

- A run that asked for a session stays `connecting` while it retries, even if the broker is missing. It never shows the picker first.
- `picker` appears for `--picker` runs, a refused destination, or a failure that retrying cannot fix, with a short notice.
- `capture` and `wait` keep working in every state.

Every request has `version`, a nonzero `id`, `op`, and `attachment`:

```json
{"version":1,"id":1,"op":"capture","attachment":"<handle>","format":"both"}
{"version":1,"id":2,"op":"text","attachment":"<handle>","generation":1,"text":"printf 'driver ok'"}
{"version":1,"id":3,"op":"keys","attachment":"<handle>","generation":1,"keys":["Enter"]}
{"version":1,"id":4,"op":"wait","attachment":"<handle>","after_action":3,"expect":{"text_contains":"driver ok"}}
```

### Operations

- **`capture`**: `format` is `text` (default), `cells`, or `both`. Returns one string per row, plus cursor, geometry, and focus.
- **`text`**: literal printable UTF-8. No control characters; use `keys` for Enter or Tab.
- **`keys`**: `Enter`, `Escape`, `Tab`, `Backspace`, `Up`, `Down`, `Left`, `Right`, `Home`, `End`, `PageUp`, `PageDown`, `Space`, one printable ASCII character, `Ctrl+<char>`, or `Alt+<char>`.
- **`wait`**: waits until all conditions match. Conditions: `text_contains`, `session`, `focus`, `status`. With `after_action`, only screens after that action count.

### Completion

- An action is `processed` when vev has handled and rendered it. That does not mean the shell has finished running the command: use `wait` for that.
- If a request times out after the input was accepted, the reply has `accepted:true` and an `action_id`. The input is not undone. Use `wait` with `after_action` to follow up.

## Limits

| Limit | Value |
|---|---|
| Request size | 64 KiB |
| Input per request | 16 KiB, 1–256 keys |
| Request timeout | 5 s default, 30 s max |
| Connections per attachment | 4 |
| Active waits | 4 |

Error codes include `invalid_request`, `stale_attachment`, `unavailable`, `timeout`, `outcome_unknown`, `capture_too_large`, `input_busy`, and `navigation_failed`. Errors never contain screen content or credentials.

## Observe an interactive client

Observation is off by default. Enable it when you attach:

```sh
vev --ui-observe attach work    # read-only
vev --ui-control new work       # read and send input
```

The client prints a private socket path to stderr (or use `--ui-socket /abs/path`). Connect to it:

```sh
vev --ui-driver --socket /abs/path/ui.sock
```

- The socket is private (`0600`), and on Linux only your user can connect.
- `--ui-observe` rejects `keys` and `text`.
- Observers never resize, redraw, or slow down the real terminal. Disconnecting leaves the client running.

Not supported: image capture, mouse, paste, resize commands, and TCP access.
