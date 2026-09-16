# Architecture

vev uses a hexagonal core with typed session messages at the client and daemon boundary. Binary framing and carriage selection stay outside the use cases.

## Package ownership

- `internal/domain`: pure shared values and invariants. `domain/terminalcap` owns terminal capability values and environment detection policy.
- `internal/protocol`: typed, transport-neutral client/daemon messages, protocol version, semantic validation, and handshake policy.
- `internal/protocol/catalogue`: independently versioned remote discovery JSON schema and bounded validation.
- `internal/protocol/wire`: Protobuf wire contract. Sources of truth are
  `schema/*.proto` (generated `*.pb.go` is never edited); `scan.go` owns
  strict protowire inspection, `preamble.go` owns limits/magic/epoch, and
  `transport.go` owns raw `Transport`, `Dialer`, and `Listener` contracts
  carrying `Envelope{Payload}` (one complete serialized directional
  envelope, no type byte).
- `internal/ports`: application-facing interfaces and the values required by those interfaces. It contains no codecs, raw frames, environment policy, or worker implementations.
- `internal/usecase`: client, daemon, broker, and supporting application behavior. Production use cases may consume semantic protocol packages but never `protocol/wire` or concrete adapters. The broker may use its own subpackages but cannot import sibling use cases. Client, daemon, and remote-registry use cases cannot own or import each other; they collaborate only through ports and application composition.
- `internal/adapters/sessionwire`: translates between typed session
  connections and raw wire transports: directional `oneof` envelope
  wrapping/unwrapping, the staged preamble state machine
  (PreambleRequest first, PreambleResponse acceptance, exact version,
  negotiated ceilings, one absolute deadline), direction checks, and
  decode-failure classification.
- `internal/adapters/brokerwire`: translates between typed broker messages
  and the separate broker Protobuf conversation (`schema/broker.proto`;
  client tags 101-110, server tags 201-209, disjoint from every session
  tag, each direction its own closed `oneof`). It owns the stateless
  broker codec over `wire.ScanEnvelope`, the broker preamble (roles 3/4,
  exact `protocol.Version`, 1 MiB..16 MiB / 1..64 KiB ceilings, 4 KiB
  byte-length bound), multipart snapshot assembly, per-connection operation
  and stream trackers, and the bounded admission lock. It is not activated
  by production composition until P3.2.
- `internal/adapters/quic`: one-stream QUIC carriage (TLS 1.3, epoch ALPN,
  exact SHA-256 pin, single bidirectional stream, bounded admission) plus
  the short-lived SSH bootstrap (ephemeral certificate, 32-byte token,
  nonce, ≤4 KiB readiness, ≤15 s expiry, atomic one-time consumption).
  QUIC library types never leave the package.
- `internal/adapters/uidriver`: owns strict JSONL decoding, response serialization, bounded controller queues, private Unix sockets, peer credentials, and the stdio bridge. It consumes `ports.UIService` and never creates an attachment.
- `internal/adapters/webterm`: owns the authenticated loopback HTTP/WebSocket frontend, browser event encoding, VT-backed terminal and transactional HTML output. It implements `ports.Terminal`; application composition supplies the ordinary client runner.
- `internal/adapters/uiterm`: owns the immutable VT mirror/snapshot and deterministic headless terminal. The concrete terminal writer (`adapters/term`) taps successful writes and flushes into this sink only when observation is enabled.
- `internal/adapters`: IPC, QUIC, SSH stdio, PTY, terminal, VT-backed UI terminal/observation, JSONL UI-driver sockets, persistence-facing, broker-wire, and observability implementations. `streamframe` owns the shared 4-byte big-endian length framing of one complete Protobuf envelope used by every stream carriage.
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

Every attachment presents the navigation picker itself. The serving daemon publishes a typed interaction instead of installing an overlay: `PickerOffer` names the intent and the acquisition barrier, `PickerSnapshot` carries opaque row keys plus display text, `PickerSelection` commits a key at the displayed revision, `PickerClose` retires the interaction in either direction, and `PickerFailure` reports a bounded rejection. The daemon owns no presentation model: it keeps facts, eligibility, and mutations, and resolves committed keys to navigation, move, or kill targets.

Source eligibility travels with every row. `PickerLine.Focusable` is the source's authorisation to rest the cursor on that row, while `PickerLine.Actions` is what the row may commit; an action row is always focusable, a section header never is, and a focusable row without actions is published for inspection only. The client's cursor visits focusable rows, narrowed by its own search query, so it never lands on a row the source skipped and the footer never promises a commit the cursor cannot make.

The client owns who writes the terminal for the interaction lifetime, while the daemon keeps mutation authority and resolves keys. `pickerLease` in `internal/usecase/client` tracks the presentation states: a snapshot is admitted only once the client applied output through the barrier epoch/state it carries, daemon paints are then applied to the output shadow and acknowledged inside the bounded window without being written, and the interaction is released only by a full paint accepted after the close boundary. Locally composed frames and repaints go through the same single writer, UI transaction, and flush as daemon output, so a frame is never claimed before it is displayed.

Input ownership is exclusive for the interaction lifetime. `pickerConsumer` in `internal/usecase/client` publishes the current owner to the stdin pump: while the picker owns input the pump decodes physical bytes and admitted automation batches into identified operations and the attach loop, its single model writer, applies them. Nothing falls back to the session pipeline: unknown, oversized, and paste-sized batches are consumed, a multi-key batch is never applied as a command, and an ambiguous escape prefix is bounded by a short deadline instead of being forwarded. The serving daemon mirrors the rule: while `pickerOpen` it drops raw key and mouse input for that attachment (`handleInput`/`handleInputForAttachment` return before the mouse scanner and the key router), so only typed `PickerSelection`, `PickerClose`, and `PickerPreviewRequest` can act. A terminal close — a resolved selection, a cancel, a stale-revision or retired-target rejection, or a superseding overlay — sends `PickerClose` when the client must retire the interaction itself, keeps consuming and dropping input until the daemon's authoritative repaint lands, and only then resumes normal routing.

### Row previews

A preview is data, not a daemon paint: the client asks for the row its cursor rests on with a debounced `PickerPreviewRequest` (80 ms, one request in flight per interaction), and the serving daemon answers with a bounded styled-cell `PickerPreview`. The daemon captures local rows through the navigation preview facts it already owns (`previewTarget` plus `snapshotPickerPreview`), captures remote rows through its own remote viewport client and cache, and registers the requested row as that viewer's render subscription, so a row that keeps changing keeps publishing while a superseded request publishes nothing. Unchanged or retired rows answer with a status-only preview, and the client clears its viewport rather than showing another row's content. The client composes the viewport into its own modal; the daemon never writes it.

### Attention and notifications

Attention is a daemon fact, not an overlay: a bell raised by a PTY, by a status-line notification, or by an agent run marks the owning tab, and the daemon publishes it in the frames its attachments receive. The status bar carries it as a pulsing glyph, the picker session header carries it inline, and `PickerLine.Attention` carries it on the tab row the client renders. Because a client-owned modal suppresses daemon paints, the attention pulse republishes the picker source for every attachment whose interaction the client presents, so a bell raised while the picker is open still reaches the displayed rows.

## Remote hosts

Remote discovery has two owners with separated writers. The daemon composes its own store, catalogue client, cache, and monitor for the structured rows and exact-target validation its picker serves. The launching client composes a runner-scoped `remotes.HostRegistry` over its own store, catalogue client, and a runner-local catalogue cache that seeds from the durable snapshot and never writes it, so one monitor can never truncate the other's file. The registry projects that discovery through `ports.RemoteDirectory` and resolves each endpoint once into a `ports.RemoteEndpointBinding` — the typed dialer every session on that endpoint attaches through, plus the environment to advertise — which the runner reuses for its initial remote attach and for every later handoff. Transport mode, launch allowlist, and endpoint environment stay in `internal/app` behind `ports.RemoteEndpointFactory`; the client never inspects them, and a resolution failure is never cached, so a refused endpoint fails the handoff instead of falling back to another resolver.

## Session composition

```text
client or daemon use case
  ↕ ports.ClientConnection / ports.ServerConnection
adapters/sessionwire
  ↕ protocol/wire.Transport carrying wire.Envelope (Protobuf)
IPC, QUIC, or SSH stdio adapter
```

Use cases exchange `protocol.ClientMessage` and `protocol.ServerMessage` values. `sessionwire` alone maps those values to directional `oneof` envelopes. Blind proxies may forward raw envelopes, but no proxy path exposes bytes to a use case.

## Adding code

- Add application behavior to a use case and define any cross-layer interface in `internal/ports`.
- Add transport-neutral session meaning to `internal/protocol`.
- Add remote catalogue schema fields and validation to `internal/protocol/catalogue`.
- Add Protobuf message variants, envelope fields, or preamble members to
  `internal/protocol/wire/schema/*.proto`, regenerate with
  `go tool buf generate`, and handle the new variant in `sessionwire`.
  Never edit generated `*.pb.go`; never add manual IDs or dispatch tables.
- Implement I/O, queues, workers, environment integration, or technology selection in an adapter or `internal/app`.
- Bump `internal/protocol.Version` for negotiated wire layout changes (currently `54`). The preamble epoch (`wire.ProtocolEpoch`, QUIC ALPN `vev/1`) bumps only for an intentional clean break.

## Broker wire core (Plan 001 P3.1, not activated)

`internal/adapters/brokerwire` owns the broker's separate Protobuf
conversation end to end: client tags 101-110 and server tags 201-209 are
disjoint from every session tag, and each direction is its own closed
directional `oneof` union. A broker connection opens with exactly one
broker preamble (roles 3/4) that carries the same magic, epoch, exact
`protocol.Version`, and 1 MiB..16 MiB / 1..64 KiB ceilings as the session
handshake; a serialized preamble is bounded to 4 KiB (`wire.PreambleLimit`,
re-exported through `brokerwire.CheckPreambleSize`) before scan or
allocation. The broker negotiates no optional capabilities, so nonzero
capability bits are refused with code 7.

The package is stateful only where P3.1 requires it: per-connection state
(ready/close, one subscription generation series, one assembler), multipart
snapshot assembly with an 80 MiB staged ceiling that charges at least 64
bytes per tab, and bounded operation/stream trackers. Every worker method
holds the connection lock across both the ready check and the tracker
mutation, so a racing close can never admit work after shutdown. The codec
itself stays stateless over `wire.ScanEnvelope`. No socket, framing pump,
or P3.2 transport is introduced here.

## Broker pool core (Plan 001 P2.2, not activated)

`internal/usecase/broker.Pool` owns bounded physical keys and logical stream
reservations, but no session data or traffic queues. Local and registered remote
requests use the same operation. A broker-owned `BrokerEndpointResolver` checks
current registration authority and exact requested policy on every request and
returns an authenticated service binding. Display aliases and adapter addresses
are not keys. Identity plus complete trust, launch, isolation, transport,
protocol, catalogue, and environment policy select one physical connection;
authenticated identity and policy are checked again after connection.

Clients receive pool-issued epoch-scoped connection IDs and supply strictly
increasing stream IDs. Failed admitted opens consume their IDs. Active clients,
physical keys, and pending/live streams have explicit caps; retired IDs need no
unbounded tombstone collection. Coalesced connection attempts are independent
of any single waiter and are canceled when their final reservation leaves.
Physical retirement keeps its key occupied until Close finishes. Fake-clock idle
eviction and shutdown close transports outside the bookkeeping lock.

Logical and physical ports expose terminal signals independent of message reads.
A stream terminal failure releases only its reservation; physical failure fans
out separately scoped typed loss errors. The pool owns neither framing nor
fairness: future transport adapters must implement independently bounded queues
and prompt cancellation/Close. Shutdown joins owned operations and relies on
that explicit port contract. No production composition or real multiplexing is
introduced here, and existing production connectivity ownership is unchanged.

Physical `Done` is authoritative: adapters publish stable `Err` and
`FailureKind`, then close physical `Done` before terminating affected logical
streams. Loss carries the exact epoch/connection/stream and a nonzero failure
kind (invalid/absent adapter kinds defensively become transport failure), with
its diagnostic cause retained locally through `errors.Is/As`. Resolver,
connector, and open errors map deterministically: deadline before cancellation,
then a validated broker code, otherwise unavailable. Invalid resolved bindings
and nil successful opens are incompatible adapter results. Adapter diagnostics
are never copied to display text. Resolved addresses are bounded opaque tokens,
not display labels or pooling identities.

At terminal observation the precedence is pool shutdown, request/client
cancellation, physical loss, entry retirement, then logical completion. Thus
shutdown reports cancelled rather than a synthetic transport loss caused by
local Close; a physical loss already published to a stream is not rewritten by
later shutdown. Explicit logical Close remains idempotent and preserves the
underlying Close error. Timer ownership is single-goroutine; Stop followed by a
nonblocking drain on false precedes Reset, supporting buffered timer adapters.
Policy identities have independent field-labelled validation and a named bound.
These are P2.2 port contracts, not a wire migration or production activation.
