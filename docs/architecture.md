# Architecture

vev uses a hexagonal core with typed session messages at the client and daemon boundary. Binary framing and carriage selection stay outside the use cases.

## Package ownership

- `internal/domain`: pure shared values and invariants. `domain/terminalcap` owns terminal capability values and environment detection policy.
- `internal/protocol`: typed, transport-neutral client/daemon messages, protocol version, semantic validation, and handshake policy.
- `internal/protocol/catalogue`: independently versioned remote discovery JSON schema and bounded validation.
- `internal/protocol/wire`: message IDs, raw frames, strict codecs, compression, encoded limits, and raw `Transport`, `Dialer`, and `Listener` contracts.
- `internal/ports`: application-facing interfaces and the values required by those interfaces. It contains no codecs, raw frames, environment policy, or worker implementations.
- `internal/usecase`: client, daemon, and supporting application behavior. Production use cases may consume semantic protocol packages but never `protocol/wire` or concrete adapters.
- `internal/adapters/sessionwire`: translates between typed session connections and raw wire transports, including direction checks and decode-failure classification.
- `internal/adapters/uidriver`: owns strict JSONL decoding, response serialization, bounded controller queues, private Unix sockets, peer credentials, and the stdio bridge. It consumes `ports.UIService` and never creates an attachment.
- `internal/adapters/webterm`: owns the authenticated loopback HTTP/WebSocket frontend, browser event encoding, VT-backed terminal and transactional HTML output. It implements `ports.Terminal`; application composition supplies the ordinary client runner.
- `internal/adapters/uiterm`: owns the immutable VT mirror/snapshot and deterministic headless terminal. The concrete terminal writer (`adapters/term`) taps successful writes and flushes into this sink only when observation is enabled.
- `internal/adapters`: IPC, UDP, SSH stdio, PTY, terminal, VT-backed UI terminal/observation, JSONL UI-driver sockets, persistence-facing, and observability implementations.
- `internal/app`: CLI parsing and composition. It selects local or remote carriage, wraps raw dialers with `sessionwire`, injects typed ports into use cases, and owns explicit UI-driver endpoint launch configuration.
- `pkg`: reusable packages that never import `internal`.

## Dependency direction

```text
main → app → usecase → ports
             │          │
             └────────→ protocol ← catalogue
                              ↑
                        protocol/wire
                              ↑
                    carriage adapters
```

`internal/app` may import every layer to compose the process. Adapters depend inward on ports, protocol, and domain. The existing `adapters/snapshot → usecase/snapshot` dependency is an explicit exception until the snapshot codec has its own owner. Production dependency rules and the separate test-import policy are executable in `boundary_test.go`.

## UI observation and control

Headless UI-driver mode composes one normal `client.Runner`, one `uiterm` terminal, one `client.UI` service, and one bounded JSONL adapter. It does not bypass the palette, input scanners, foreground leases, navigation handoffs, output replay chain, or render publication fences. EOF cancels only that driver and detaches the attachment; it does not kill a session shell.

Interactive observation is a separate opt-in composition. `term.Terminal` remains the owner of raw mode, physical geometry, terminal queries, serialized output writes, and flush success. Its optional observation sink mirrors byte prefixes and successful flush boundaries into `uiterm`; UI transaction methods are delegated through a composition wrapper so semantic context is committed atomically with the physical flush. A private per-client Unix endpoint uses the same `ports.UIService`; `--ui-control` is the only mode that permits input operations. Slow or disconnected observers cannot claim geometry or block the physical terminal.

## Browser terminal

`--web-daemon` launches a separate gateway with an inherited HTTP listener, loopback by default. App composition resolves `web.listen`/`web.origin` and CLI overrides before binding. The web adapter validates the configured browser-facing Host and Origin without trusting forwarded headers; a proxy owns TLS. The same-user local control socket returns the running gateway's settings and memory-only credential; readiness probes contact only its local listener. Each authenticated WebSocket owns a normal client runner and a `webterm.Terminal`. The adapter encodes browser events as terminal input, mirrors flushed client output through `vev-vt`, and sends transactional HTML updates. Disconnect cancels only that attachment. The multiplexer remains daemon-owned; browser input does not weaken the restricted UI-driver automation contract.

## Client-owned picker presentation

Attachments that advertise the client-picker capability receive the navigation picker as a typed interaction instead of a daemon-installed overlay: `PickerSnapshot` carries opaque row keys plus display text, `PickerSelection` commits a key at the displayed revision, and `PickerClose` retires the interaction in either direction. `rt.picker` stays nil in that mode, so `handlePickerInput` keeps serving the daemon-owned overlay for every other attachment.

The client owns who writes the terminal for the interaction lifetime, while the daemon keeps mutation authority and resolves keys to navigation targets. `pickerLease` in `internal/usecase/client` tracks the presentation states: a snapshot is admitted only once the client applied output through the barrier epoch/state it carries, daemon paints are then applied to the output shadow and acknowledged inside the bounded window without being written, and the interaction is released only by a full paint accepted after the close boundary. Locally composed frames and repaints go through the same single writer, UI transaction, and flush as daemon output, so a frame is never claimed before it is displayed.

Input ownership is exclusive for the interaction lifetime. `pickerConsumer` in `internal/usecase/client` publishes the current owner to the stdin pump: while the picker owns input the pump decodes physical bytes and admitted automation batches into identified operations and the attach loop, its single model writer, applies them. Nothing falls back to the session pipeline: unknown, oversized, and paste-sized batches are consumed, a multi-key batch is never applied as a command, and an ambiguous escape prefix is bounded by a short deadline instead of being forwarded. The serving daemon mirrors the rule: while `pickerClientOpen` it drops raw key and mouse input for that attachment (`handleInput`/`handleInputForAttachment` return before the mouse scanner and the key router), so only typed `PickerSelection` and `PickerClose` can act. A terminal close — a resolved selection, a cancel, a stale-revision or retired-target rejection, or a superseding overlay — sends `PickerClose` when the client must retire the interaction itself, keeps consuming and dropping input until the daemon's authoritative repaint lands, and only then resumes normal routing.

## Session composition

```text
client or daemon use case
  ↕ ports.ClientConnection / ports.ServerConnection
adapters/sessionwire
  ↕ protocol/wire.Transport carrying wire.Frame
IPC, UDP, or SSH stdio adapter
```

Use cases exchange `protocol.ClientMessage` and `protocol.ServerMessage` values. `sessionwire` alone maps those values to message IDs and payload codecs. Blind proxies may forward raw frames, and the UDP adapter may inspect bounded frame classification for QoS, but neither path exposes bytes to a use case.

## Adding code

- Add application behavior to a use case and define any cross-layer interface in `internal/ports`.
- Add transport-neutral session meaning to `internal/protocol`.
- Add remote catalogue schema fields and validation to `internal/protocol/catalogue`.
- Add message IDs, binary layouts, strict decoding, compression, or raw carriage contracts to `internal/protocol/wire`.
- Implement I/O, queues, workers, environment integration, or technology selection in an adapter or `internal/app`.
- Bump `internal/protocol.Version` for negotiated wire layout changes (currently `45`, including the client-picker interaction).
