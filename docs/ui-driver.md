# UI driver

`vev --ui-driver` exposes one opt-in attachment as a bounded JSON Lines stream. It is intentionally a hidden headless entry point for tests and local automation that need the rendered terminal, not a second terminal implementation.

## Headless attachment

Start a new numbered ephemeral session with the default 80x24 viewport:

```sh
vev --ui-driver
```

`--ui-driver` uses the per-user broker of the process' own XDG context. It never
provisions, reconfigures, or destroys a daemon, a session-scoped root, or an
endpoint: creation is expressed as one initial navigation through the broker, so
two drivers can share one per-user broker without either owning it.

The closed option set is:

- `--session NAME` creates that named local session instead of an ephemeral one.
- `--remote ENDPOINT` targets that configured remote endpoint: with `--session`
it resolves that exact remote session, without it it creates a remote ephemeral
session. Every remote identity comes from the broker's committed publication,
never from a hostname or an environment value.
- `--cols N --rows N` set the viewport, at most 512x256.
- `--picker` starts with no initial navigation at all and is exclusive of
`--session` and `--remote`.
- `--socket ABS` is the JSONL bridge to an already running opted-in client, and
is exclusive of every headless option.

`--launch-config` no longer exists. It is an unknown option and is refused with
the usage exit code before any terminal, broker, or filesystem interaction; a
driver that needs an isolated endpoint provisions it through the broker's own
configuration instead.

The first line on stdout is the discovery response. It is emitted as soon as the
client has published its initial state, which does not require a broker or a
daemon:

```json
{"version":1,"id":0,"result":{"attachment":"<opaque-handle>","generation":0,"control":true,"status":"picker"}}
```

`status` is a closed union of exactly three presentations:

| `status` | meaning | `generation` | session metadata in `capture`/`wait` | `keys`/`text` |
|---|---|---:|---|---|
| `picker` | the local picker; ready without a broker and without a daemon | `0` | all zero | refused (`unavailable`) |
| `connecting` | an attachment attempt that has not committed its first frame | `0` | all zero | refused (`unavailable`) |
| `attached` | a committed attachment | nonzero real action generation | the validated session identity and committed boundary | admitted under the current binding |

The former `detached`, `reconnecting`, and `transitioning` states no longer
exist and are not aliases of these three: a reconnect is `connecting`, a detach
is `picker`, and terminating closes the service instead of becoming a fourth
persistent presentation. A driver that never publishes anything is a real local
error; a destination that is refused, times out, or is lost leaves a published
`picker` and the same JSONL service running.

The `attachment` handle is the UI service of this run, never a session identity,
and it is stable for the whole run. `generation` is zero until a committed
attachment publishes one; a client must wait for `status` to become `attached`
and use the generation that publication reports, and never assume
`generation == 1`. A broker that is absent or incompatible is not fatal: the
driver stays in the picker with a bounded notice and keeps answering `capture`
and `wait`.

The stream then accepts one JSON object per line. Every request has `version`, a nonzero `id`, `op`, and the discovered `attachment`:

```json
{"version":1,"id":1,"op":"capture","attachment":"<opaque-handle>","format":"both"}
```

`format` is `text` (the default), `cells`, or `both`. Text contains exactly one newline-separated string per row, including trailing blank rows. Cells are row-major and contain `text`, `width`, `continuation`, and the rendered style. Captures also contain the revision, coherent route/session/focus context, geometry, and cursor.

Input targets the exact current `generation`:

```json
{"version":1,"id":2,"op":"keys","attachment":"<opaque-handle>","generation":1,"keys":["Alt+Space"]}
{"version":1,"id":3,"op":"keys","attachment":"<opaque-handle>","generation":1,"keys":["Escape"]}
{"version":1,"id":4,"op":"text","attachment":"<opaque-handle>","generation":1,"text":"printf 'driver ok'"}
{"version":1,"id":5,"op":"keys","attachment":"<opaque-handle>","generation":1,"keys":["Enter"]}
{"version":1,"id":6,"op":"wait","attachment":"<opaque-handle>","after_action":5,"expect":{"text_contains":"driver ok"}}
```

`keys` accepts only the documented finite grammar: `Enter`, `Escape`, `Tab`, `Backspace`, `Up`, `Down`, `Left`, `Right`, `Home`, `End`, `PageUp`, `PageDown`, `Space`, one printable ASCII character, `Ctrl+` with the supported ASCII control set, and `Alt+` followed by one supported unmodified ASCII character. Cursor navigation follows the terminal's application-cursor mode. Raw escape notation, keypad keys, combined modifiers, and unknown names are rejected.

`text` is literal printable UTF-8. It does not mean paste and cannot contain C0/C1 control characters; use an explicit key for Enter, Tab, or Escape.

## Completion and waits

An accepted action is sent through the normal client input owner. A `processed` action means that its input batch reached the normal dispatch boundary and its required render/publication boundary. It does not mean that a child process has echoed the input or completed a command. A local palette action uses the client event-loop boundary instead of a fabricated daemon fence.

Navigation actions remain pending until the existing handoff reaches a validated, written, and published full destination view. If the handoff or transport cannot be attributed safely, the action reports `outcome_unknown`; it is never replayed into a new generation. A failed route reports `navigation_failed`.

A request timeout only ends that request. If input was already accepted, the response has `accepted:true` and an `action_id`; the input is not rolled back. Use `wait` with `after_action` to retrieve the later result. An evicted action reports `action_expired`.

`wait` requires at least one predicate. All supplied predicates are conjunctive:

```json
{"version":1,"id":6,"op":"wait","attachment":"<opaque-handle>","after_action":4,"timeout_ms":5000,"expect":{"text_contains":"driver ok","status":"attached"}}
```

Supported predicates are literal `text_contains`, an exact `session` (`lifecycle_id` and `session_name`), an exact `focus` (`tab_id` and `pane_id`), and `status` (`picker`, `connecting`, or `attached`). Without `after_action`, a wait may match the current published snapshot. With it, only the action's confirmed publication boundary and later snapshots are eligible. This is a postcondition check, not a claim that the action alone caused the text.

## Limits and errors

- Requests are at most 64 KiB. A complete final JSON object without a newline is accepted; truncated JSON is not.
- Input is at most 16 KiB and a `keys` request has 1–256 tokens.
- Headless viewports are at most 512 columns by 256 rows.
- The default and maximum request timeout are 5 seconds and 30 seconds.
- Each attachment allows at most four socket connections, one admitted action across all controllers, four active waits, and 64 retained action records.
- Each connection has one response being written and at most two queued responses. A slow reader is closed after the bounded write deadline or queue overrun.
- Serialized response memory is bounded to 128 MiB per attachment. An oversized capture returns `capture_too_large`; it never resizes an interactive terminal.

Errors are structured and sanitized. Stable codes include `invalid_request`, `unsupported_version`, `permission_denied`, `stale_attachment`, `unavailable`, `timeout`, `outcome_unknown`, `capture_too_large`, `action_expired`, `input_busy`, `busy`, `navigation_failed`, and `endpoint_not_configured`. Error messages contain only the code; screen contents, credentials, endpoint internals, and environment values are not logged or returned.

EOF detaches this driver attachment and closes its access stream. It does not kill shells or sessions, does not stop the shared broker, and does not remove any root: the broker, its daemon, and every session stay alive for the other clients, and the pool's own idle shutdown remains the only thing that ends the daemon.

## Isolation

The driver owns no endpoint root, no daemon, and no process. An isolated endpoint
is provisioned through the broker's own configuration before the driver starts,
exactly like any other broker route: the broker is the single authority over
binary, root, environment, trust, and launch policy, and those values are never a
payload of a JSONL request, a selected hostname, or a client environment variable.
There is no per-invocation root creation, no owner token, and no remote cleanup
step at driver exit.

Directory isolation is not a process, filesystem, or network sandbox; environment
files and binaries remain sensitive.

## Existing interactive clients

Observation and control are disabled by default. Enable them before an attach command:

```sh
vev --ui-observe attach work
vev --ui-control new work
```

`--ui-control` implies observation. The client prints one socket path to stderr, or uses the supplied private path:

```sh
vev --ui-observe --ui-socket /absolute/private/ui.sock attach work
```

The socket parent is private (`0700`) and the socket is private (`0600`). On Linux, the server also requires the connecting peer's UID to match the client UID. A path collision is rejected and never unlinks an existing file. Read-only observation rejects `keys` and `text` before they reach the input owner.

The bridge form connects JSONL stdio to an already-running opted-in client; it does not create another attachment:

```sh
vev --ui-driver --socket /absolute/private/ui.sock
```

The physical terminal remains authoritative for geometry and terminal query responses. Mirroring observes bytes at the serialized terminal writer and publishes only after a successful physical flush. Capture and wait never write to the terminal, request a redraw, change focus, claim PTY geometry, send Resize, or acknowledge protocol output independently. If mirroring fails, capture becomes unavailable while the interactive client continues running. Disconnecting an observer leaves the interactive attachment and its shells running.

The driver intentionally does not provide image capture, mouse/paste/resize commands, subscriptions, scenarios, replay, automatic artifact export, public TCP access, or sandbox management.
