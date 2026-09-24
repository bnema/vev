# Architecture

vev uses a hexagonal core with typed session messages at the client and daemon boundary. Binary framing and carriage selection stay outside the use cases. The current strict protocol version is 59.

`CommandResult` has a closed explicit outcome contract:

| Outcome | Meaning | Fields |
| --- | --- | --- |
| `succeeded` | The command completed successfully. | code and text are empty; output may be present. |
| `failed` | A definite failure, including validation/cancellation before dispatch. | code is nonzero; output is empty. |
| `outcome_unknown` | Dispatch was attempted but no final outcome can be established (timeout, cancellation, EOF, or lost reply). | code and output are empty; callers must not replay automatically. |

Every result keeps its request ID for strict correlation.

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
- `internal/adapters/brokerwire`: typed broker messages ↔ the broker Protobuf conversation (`schema/broker.proto`). See [Broker](#broker).
- `internal/adapters/brokeripc`: the private `broker.sock` endpoint. Implements `ports.BrokerListener` and `ports.BrokerService`.
- `internal/adapters/quic`: one-stream QUIC carriage (TLS 1.3, epoch ALPN,
  exact SHA-256 pin, single bidirectional stream, bounded admission) plus
  the short-lived SSH bootstrap (ephemeral certificate, 32-byte token,
  nonce, ≤4 KiB readiness, ≤15 s expiry, atomic one-time consumption).
  QUIC library types never leave the package.
- `internal/adapters/brokerconfig`: parses `broker.json` (registrations, local binding, warm-transport settings) and provides the immutable `ports.BrokerEndpointResolver`.
- `internal/adapters/uidriver`: owns strict JSONL decoding, response serialization, bounded controller queues, private Unix sockets, peer credentials, and the stdio bridge. It consumes `ports.UIService` and never creates an attachment.
- `internal/adapters/webterm`: owns the authenticated loopback HTTP/WebSocket frontend, browser event encoding, VT-backed terminal and transactional HTML output. It implements `ports.Terminal`; application composition supplies the ordinary client `Supervisor`.
- `internal/adapters/uiterm`: owns the immutable VT mirror/snapshot and deterministic headless terminal. The concrete terminal writer (`adapters/term`) taps successful writes and flushes into this sink only when observation is enabled.
- `internal/adapters`: IPC, QUIC, SSH stdio, PTY, terminal, VT-backed UI terminal/observation, JSONL UI-driver sockets, persistence-facing, broker-wire, and observability implementations. `streamframe` owns the shared 4-byte big-endian length framing of one complete Protobuf envelope used by every stream carriage.
- `internal/app`: CLI parsing and composition. It selects local or remote carriage, wraps raw dialers with `sessionwire`, and injects typed ports into use cases. The UI driver keeps no endpoint authority of its own: it composes the process' production broker connector and delegates every transport, daemon, and session decision to the broker.
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

Headless UI-driver mode composes one normal `client.Supervisor`, one `uiterm` terminal, one `client.UI` service, and one bounded JSONL adapter. The supervisor obtains snapshots and opens every attachment through the single `ports.BrokerService` façade; the client does not select a daemon carriage or construct a remote registry. It does not bypass the picker, input scanners, foreground leases, navigation handoffs, output replay chain, or render publication fences. EOF cancels only that driver and detaches the attachment; it does not kill a session shell.

Interactive observation is a separate opt-in composition. `term.Terminal` remains the owner of raw mode, physical geometry, terminal queries, serialized output writes, and flush success. Its optional observation sink mirrors byte prefixes and successful flush boundaries into `uiterm`; UI transaction methods are delegated through a composition wrapper so semantic context is committed atomically with the physical flush. A private per-client Unix endpoint uses the same `ports.UIService`; `--ui-control` is the only mode that permits input operations. Slow or disconnected observers cannot claim geometry or block the physical terminal.

## Browser terminal

`--web-daemon` launches a separate gateway with an inherited HTTP listener, loopback by default. App composition resolves `web.listen`/`web.origin` and CLI overrides before binding. The web adapter validates the configured browser-facing Host and Origin without trusting forwarded headers; a proxy owns TLS. The same-user local control socket returns the running gateway's settings and memory-only credential; readiness probes contact only its local listener. Each authenticated WebSocket owns a normal client `Supervisor` and a `webterm.Terminal`. The adapter encodes browser events as terminal input, mirrors flushed client output through `vev-vt`, and sends transactional HTML updates. Disconnect cancels only that attachment. The multiplexer remains daemon-owned; browser input does not weaken the restricted UI-driver automation contract.

## Client-owned picker presentation

A client publishes exactly one of three presentations at a time. `picker` is the local picker: the client is usable and describable without a broker and without a daemon. `connecting` is an attachment attempt that has not committed its first frame. `attached` is a committed attachment, and only it carries the validated session identity, the committed output boundary, and the real actionable generation; `picker` and `connecting` carry the run's stable service handle and their status with every session field zeroed, so a capture can never present session metadata as an attachment and no caller can fence an input action against a generation no attachment committed. The former `detached`, `reconnecting`, and `transitioning` values are removed without alias: a reconnect is `connecting`, a detach returns to `picker`, and terminating closes the UI service instead of becoming a fourth persistent presentation. The presentation is published inside the terminal's UI output transaction by its owner: the supervisor publishes `connecting` before the attachment foreground is granted and `picker` after the run drained and revoked, while the admitted foreground publishes `attached` only after its first frame was written, flushed, and committed.

Session navigation is client-owned. While attached, Alt+Space opens the daemon command palette; `SSP` / `session-picker` opens the client picker as an overlay over the live attachment. Cancel returns to the same attachment without reconnecting. Committing a different destination switches attachments through `connecting`; attachment loss or explicit detach leaves the picker as the standalone presentation. Search, sorting, cursor movement, selection, and broker-backed attachment opening are client-owned. Daemon move-destination offers still use the attached worker's separate move picker.

The selected row owns one client-scoped live preview subscription through `ports.BrokerService`. The broker opens one long-lived observation stream over its local or remote physical pool; the daemon pushes changed `RemotePreview` frames at a broker-selected interval (33 ms local, 125 ms remote). Cursor changes are debounced for 80 ms, and the client retains eight recent previews so a selection can remain visible while refreshing. Broker loss, commit, picker close, and client exit cancel the watch. The client checks epoch, connection, generation, target, and dimensions before painting; previews remain bounded, memory-only, and are never persisted.

The daemon does not own the session picker or resolve cross-daemon navigation; it can offer a move-destination picker to the attached client and receives its typed selection or close reply.

Pane/tab move destination pickers are separate from session navigation. The daemon captures and revalidates the move source; the attached worker displays the offer and sends the selected move target back. The command palette remains daemon-owned.

### Resize repaint fence

A terminal resize reaches the client as one coalesced invalidation owned by the attachment geometry collector in `internal/usecase/client`, and the supervisor's serialized waits (connect attempt, established ready loop, retry cadence, and non-retryable picker wait) consume it and repaint. The fence is exactly `PickerPresentation`, which is `PresentPicker`: the composition samples `Terminal.Geometry` at render time, so a repaint always observes the latest size. The pre-attachment `PresentConnecting` transition is deliberately refused, because `Begin` has already admitted the attachment foreground that owns the same terminal writer before `MarkAttached` and the initial publication; painting there would overwrite session output. For the same reason the attachment settlement wait never consumes the invalidation, so it stays coalesced and buffered through connecting and attached and is consumed with a repaint only after the attachment returns to the picker. The invalidation is independent of the attachment's own geometry sequence/wakeup, which still drives the typed resize to the live session.

### Attention and notifications

Attention remains a daemon fact. The broker publishes it from fresh observations but strips it from durable snapshots. The client's route ledger forwards attention order to the serving daemon for status-bar bells and cross-daemon jump-to-attention; the picker displays session and tab bells. The serving daemon uses that same route snapshot to display remote palette destinations, without monitoring other daemons.

## Broker-owned hosts and daemon composition

`vev _broker-ready --require local-authority|local-catalogue` is an internal,
dial-only machine probe. It connects directly to the selected broker IPC
socket, registers, subscribes, and validates complete snapshots. It never
ensures or spawns, opens a logical stream, reconciles, or mutates. An optional
`--offline-root ABS` selects the offline layout. Output is exactly one bounded
`vev.broker-ready/v1` JSON line with decimal-string epoch/revision IDs. Exit
codes are 0 ready, 1 internal/write, 2 arguments, 3 timeout, 4 terminal
security/protocol/configuration failure, and 5 cancellation.

The broker is the sole façade for local and remote inventory, endpoint membership, policy, observation, and logical session streams. `client.Supervisor` consumes `ports.BrokerService`; it does not import or compose a remote registry, remote adapter, endpoint factory, or direct session dialer. Host mutations and attachment requests therefore pass through the same broker authority and committed snapshot.

A daemon serves only the sessions on its own machine. It has no remote-directory monitor and does not compose remote transports. In production it accepts the broker's authenticated multiplexed physical carriage through `internal/adapters/daemonmux`; logical observation, control, and attachment streams share that carriage while retaining independent admission and lifetime. The daemon's Unix daemonmux listener is local-only composition infrastructure between the broker and daemon, not a public client endpoint. Ordinary clients connect to `broker.sock` and never dial a daemon socket directly.

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
- Bump `internal/protocol.Version` for negotiated wire layout changes (currently `59`). The preamble epoch (`wire.ProtocolEpoch`, QUIC ALPN `vev/1`) bumps only for an intentional clean break.

## Broker

The broker is a per-user process that sits between every client and every daemon. It starts on first use and stops after 5 minutes idle (`broker.DefaultIdleGrace`).

```text
client ──broker.sock──▶ broker ──daemonmux──▶ local daemon
                          │
                          └──SSH stdio or SSH+QUIC──▶ remote daemon
```

What it owns:

- **Hosts.** `internal/usecase/broker.Registry` holds configured hosts and publishes one `ports.BrokerSnapshot`: the local daemon first, then remotes. Configured authority (endpoint, policy, rank) comes from membership; observed facts (identity, version, availability, session catalogue) come only from probes. Only remote observations are persisted.
- **Connections.** `broker.Pool` keys one physical connection by authenticated identity plus exact policy. Many logical streams share it. Display aliases and addresses are never keys. Idle connections stay warm (`warmTransports`, `warmIdleTimeout`).
- **Daemon start.** Every stream carries a `ports.BrokerDaemonStartMode`: `ExistingOnly` (observation, stop) or `StartIfNeeded` (attach, create). The mode can narrow the launch policy, never widen it. A running daemon is never restarted. The start path lives in `internal/app/broker_daemon_start.go` and uses the shared spawn election in `spawn.go`.
- **Wire.** `internal/adapters/brokerwire` owns the broker's own Protobuf conversation (`schema/broker.proto`, client tags 101+, server tags 201+). It uses the same preamble, epoch, and exact `protocol.Version` as sessions. Streams carry the typed session protocol unchanged.
- **Local IPC.** `internal/adapters/brokeripc` serves `broker.sock`: owner-only directory, `0600` socket, same-user peer credentials. Setup, queues, and clients are bounded. A lost reply to a mutating operation is reported as `outcome unknown` and never replayed.
- **Remote helpers.** Hidden commands `_broker-mux-stdio`, `_broker-mux-quic-bootstrap`, and `_broker-mux-quic-proxy` run on the remote host and bridge to that host's private daemonmux socket. They take a closed `--daemon-start existing-only|if-needed` flag (default `existing-only`). QUIC credentials are fresh per connection and used once.

Other hidden entry points: `_broker-production-serve`, `_broker-production-launcher`, and `_broker-ready` (a dial-only readiness probe that prints one `vev.broker-ready/v1` JSON line).

Locks: the broker holds `lifecycle.lock` in its runtime directory while running. A separate spawn lock elects one process to start it. Locks are never stolen by age.
