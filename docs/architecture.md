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
  client tags 101-111, server tags 201-209, disjoint from every session
  tag, each direction its own closed `oneof`). It owns the stateless
  broker codec over `wire.ScanEnvelope`, the broker preamble (roles 3/4,
  exact `protocol.Version`, 1 MiB..16 MiB / 1..64 KiB ceilings, 4 KiB
  byte-length bound), multipart snapshot assembly, per-connection operation
  and stream trackers, and the bounded admission lock. It is not activated
  by production composition until P3.2.
- `internal/adapters/brokeripc`: the Plan 001 P3.3 private local broker
  endpoint. It implements `ports.BrokerListener` (accepting same-user clients
  and running one brokerwire session per accepted connection) and
  `ports.BrokerService` (the client adapter over the same conversation) on one
  per-user AF_UNIX endpoint, reusing the P3.2 private IPC carriage
  (`ipc.ListenMux`/`ipc.DialMuxContext`: owner-only directory, 0600 socket,
  same-user `SO_PEERCRED` admission, race-safe stale-socket recovery,
  foreign-path refusal) and the shared `streamframe` framing. It owns the
  bounded per-connection queues, the snapshot publisher, the per-stream typed
  bridge over `sessionwire`, and deterministic disconnect cleanup. Like P3.1 and
  P3.2 it is not activated by production composition.
- `internal/adapters/quic`: one-stream QUIC carriage (TLS 1.3, epoch ALPN,
  exact SHA-256 pin, single bidirectional stream, bounded admission) plus
  the short-lived SSH bootstrap (ephemeral certificate, 32-byte token,
  nonce, ≤4 KiB readiness, ≤15 s expiry, atomic one-time consumption).
  QUIC library types never leave the package.
- `internal/adapters/brokerconfig`: owns the strict isolated offline broker
  sandbox configuration (`config.json` marker), the textual offline-root
  validation that keeps the sandbox out of production runtime/state, and the
  immutable `ports.BrokerEndpointResolver` over the provisioned endpoint
  registrations, identities, policies, and absolute Unix mux routes. It is
  composed only by the hidden offline broker entry points (`_broker-serve`,
  `_broker-launcher`, and `_broker-status`).
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

### Resize repaint fence

A terminal resize reaches the client as one coalesced invalidation owned by the attachment geometry collector in `internal/usecase/client`, and the supervisor's serialized waits (connect attempt, established ready loop, retry cadence, and non-retryable picker wait) consume it and repaint. The fence is exactly `PickerPresentation`, which is `PresentPicker`: the composition samples `Terminal.Geometry` at render time, so a repaint always observes the latest size. The pre-attachment `PresentConnecting` transition is deliberately refused, because `Begin` has already admitted the attachment foreground that owns the same terminal writer before `MarkAttached` and the initial publication; painting there would overwrite session output. For the same reason the attachment settlement wait never consumes the invalidation, so it stays coalesced and buffered through connecting and attached and is consumed with a repaint only after the attachment returns to the picker. The invalidation is independent of the attachment's own geometry sequence/wakeup, which still drives the typed resize to the live session.

### Attention and notifications

Attention is a daemon fact, not an overlay: a bell raised by a PTY, by a status-line notification, or by an agent run marks the owning tab, and the daemon publishes it in the frames its attachments receive. The status bar carries it as a pulsing glyph, the picker session header carries it inline, and `PickerLine.Attention` carries it on the tab row the client renders. Because a client-owned modal suppresses daemon paints, the attention pulse republishes the picker source for every attachment whose interaction the client presents, so a bell raised while the picker is open still reaches the displayed rows.

## Remote hosts

Remote discovery has two owners with separated writers. The daemon composes its own store, catalogue client, cache, and monitor for the structured rows and exact-target validation its picker serves. The launching client composes a runner-scoped `remotes.HostRegistry` over its own store, catalogue client, and a runner-local catalogue cache that seeds from the durable snapshot and never writes it, so one monitor can never truncate the other's file. The registry projects that discovery through `ports.RemoteDirectory` and resolves each endpoint once into a `ports.RemoteEndpointBinding` — the typed dialer every session on that endpoint attaches through, plus the environment to advertise — which the runner reuses for its initial remote attach and for every later handoff. Transport mode, launch allowlist, and endpoint environment stay in `internal/app` behind `ports.RemoteEndpointFactory`; the client never inspects them, and a resolution failure is never cached, so a refused endpoint fails the handoff instead of falling back to another resolver.

Current state: the daemon-side monitor is deliberately still active. `Serve`
starts the runner through `daemon.WithRemoteMonitor` and joins it inside the
bounded remote-shutdown budget, and the daemon keeps its own remote directory
and host registry. That daemon-owned monitoring (and the host registry that
feeds it) belongs to the coordinated P7 removal set; it is not removed,
gated, or polled differently before that cutover, and no polling gate is
introduced here (GO-001, deferred to P7).

## Prepared local inventory boundary (Plan 001 P4.3, preparation only)

The daemon separates local registry state from foreign remote-directory rows.
`captureLocalSessionInventory` (returning `localSessionInventory`) copies live
sessions and inactive records and never reads the remote directory;
`captureSessionInventory` composes that local capture with the foreign rows
from `currentRemoteDirectory`. The local catalogue export
(`controlExec.RemoteCatalog`), the prepared navigation projection
(`localNavigationInventorySnapshot` / `localInventoryGroup`), the prepared
navigation resolver (`localNavigationResolve`), and the prepared picker
projection (`localPickerViews` / `localPickerViewProjections`) read only the
local capture, so they cannot observe or depend on remote monitoring. The
prepared control resolver (`localPickerControlAttachTarget`) is not an
inventory projection: it reads the live session registry directly under
`d.mu` and never the remote directory, the `sessionInventory` composite, or a
stopped record — only live registry sessions are eligible there.

The current hybrid projections keep their foreign behavior by composing the
prepared local output with the foreign rows: `snapshotNavigationInventory`
appends `remoteInventoryGroup` per host, `pickerViewProjections` interleaves
`foreignPickerViews` between the local live and stopped groups, and
`pickerControlAttachTarget` delegates to `localPickerControlAttachTarget` (via
`pickerControlAttachTargetLocal`) after its remote branch. Prepared local
resolution is exact: a structured foreign source key, registration, remote
target, or remote key is rejected before any lookup; broken and purging records
are never resumable; a stopped target that becomes live with the same lifecycle
resolves; and a stale lifecycle or a same-name replacement never resolves by
name. The prepared local resolver also rejects a stale preferred tab, but the
hybrid wrapper preserves the live behavior on top of it: when the exact local
lifecycle is still current and only the tab vanished between snapshot and
resolve, it retries with an empty `PreferredTabID`, matching the client's own
missing-tab fallback instead of failing the attach.

Current ownership: `internal/usecase/daemon` owns the local capture, the
prepared projections and resolvers, and the hybrid wrappers; `internal/app`
composes the daemon-side monitor; `internal/adapters/remote` plus the
runner-scoped `remotes.HostRegistry` own discovery. This is a preparation pass:
no production composition is cut over, no monitor is gated or removed, no
public protocol, schema, or version changes, and the signed purge machinery is
untouched.

P7 removal set: `WithRemoteMonitor` and the `remoteDirectory` /
`remoteMonitorRun` state; `remoteDirectorySnapshot` and `currentRemoteDirectory`;
`watchRemoteDirectory`, `refreshRemoteDirectoryViews`, and
`notifyNewRemoteFailures`; the foreign picker composition (`foreignPickerViews`,
`remoteInventoryGroup` and the remote groups in `snapshotNavigationInventory`,
the remote branch of `pickerControlAttachTarget`, `resolveRemoteInventoryEntry`);
the hybrid wrappers themselves (`captureSessionInventory`, the
`sessionInventory` composite, `pickerViewProjections`, `pickerControlAttachTarget`
and its `pickerControlAttachTargetLocal` vanished-tab fallback) once the local
projections read `localSessionInventory` directly; the remote picker, palette,
preview, attach, and route projections that read the directory; and the
daemon-side `ports.RemoteDirectory` / `ports.RemotePreviewClient` composition.
Once those foreign inputs are deleted, the prepared local functions are the
whole projection. The daemon-side monitor and host registry stay active until
then (GO-001, deferred to P7).

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
- Bump `internal/protocol.Version` for negotiated wire layout changes (currently `57`). The preamble epoch (`wire.ProtocolEpoch`, QUIC ALPN `vev/1`) bumps only for an intentional clean break.

## Broker wire core (Plan 001 P3.1, not activated)

`internal/adapters/brokerwire` owns the broker's separate Protobuf
conversation end to end: client tags 101-111 and server tags 201-209 are
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

## Broker registry and daemon observation (Plan 001 P5.2a, not activated)

`internal/usecase/broker.Registry` owns configured hosts and their immutable
observation projections. `ports.BrokerDaemonObservation` is the broker-native
projection of one daemon, local or remote: configured authority (local flag,
endpoint, registration, policy, display origin, rank) is never taken from a
probe, while observed identity, process incarnation, protocol version,
capability bitmask, availability, failure/freshness state, and the exact
session catalogue come only from probe results. A remote host's authority is
stamped by the registry from durable membership; the local entry's authority is
stamped from the broker's configured local binding, because the local daemon
has no registration and no durable membership. Unknown identity or version is
zero, never invented; an observed identity always carries a protocol version,
and availability is never zero (an unobserved daemon carries
`RemoteAvailabilityUnknown`). A
`ports.BrokerSnapshot` publishes these observations (`Daemons`, local entry
first when present, then remotes in registration order) plus the tombstone
set; `brokerwire` mirrors them through `SnapshotBegin`/`SnapshotDaemon`/
`SnapshotSession` parts. Local observations are never durable: the snapshot
store persists and reloads remote observations only, with remote policy
re-stamped from membership on load, so membership stays the single policy
authority for remotes. Observing a daemon never attaches to it and never starts
a stopped one.
Attachment streams declare one closed admission variant
(`ports.BrokerStreamAdmission`: exact attach/resume, named creation, ephemeral
creation) validated identically at the ports, broker IPC, and daemonmux
boundaries.

## Broker-owned local route (Plan 001 P5.3a, not activated)

The broker reaches its own machine daemon, which is not a configured host, over
one broker-owned local binding. `internal/adapters/brokerconfig` parses an
optional top-level `local` object beside the registrations: the required
`identity`, `route`, and `policy`, plus an optional `displayOrigin` that is
sanitized and defaults to `local`. The route must be a Unix daemonmux carriage
this process dials directly, so an SSH or QUIC route is refused and the binding
can never name another machine. The identity, policy, and route are the only
source of local authority, and `Config.LocalBinding` alone publishes them. The
binding's own carriage is deliberately excluded from `Config.LocalMuxRoute`:
that route is the single registration carriage a remote-side mux helper bridges
to, and no helper ever bridges the broker's own local carriage.

A local open-stream request carries no endpoint and no registration, so the
resolver fences it against the binding's identity and policy instead: a request
whose policy conflicts with the provisioned local policy is refused, and a
configuration with no local object provisions no local route. The resolved
(identity, policy) pair is the pool key, so every local logical stream shares
one physical daemonmux carriage to the local daemon, exactly as every stream to
one remote identity does; the route's opaque address selects that carriage and
never participates in the key.

The registry publishes a local observation whenever a local producer is
configured, always at index zero ahead of every remote and never more than one.
Its configured authority (local flag, display origin, policy, rank) is stamped
from the binding and never from durable membership, since the local daemon has
no membership; its observed identity, incarnation, protocol version,
availability, and session catalogue come only from the local probe. The local
entry is never durable: the snapshot store persists and reloads remote
observations only. Observing the local daemon completes only the daemonmux
physical preamble and closes the carriage, so it never opens a logical stream
(never attaches) and only ever dials an existing local Unix socket, so it never
starts a stopped daemon. Nothing in `internal/app` or `main` activates this
composition for ordinary operation; P7 performs the coordinated cutover.

## Broker local IPC (Plan 001 P3.3, not activated)

`internal/adapters/brokeripc` carries the P3.1 broker conversation over one
per-user Unix endpoint. The carriage is the P3.2 private AF_UNIX carriage in
`internal/adapters/ipc`: an owner-only (0700, `pkg/safedir.EnsurePrivate`)
parent directory, a bound socket tightened to 0600, race-safe recovery of a
stale socket whose owner died, refusal (never removal) of a path that is not a
socket, and same-user kernel peer credentials (`SO_PEERCRED` on Linux) on both
accept and dial, failing closed on a platform or build without that check. The
endpoint name (`broker.sock`) is deliberately distinct from the daemon's
`daemon.sock`.

The listener owns its accept loop: one client slot is acquired before each
accept, each accepted carriage completes the broker preamble and broker
admission in its own goroutine, and admitted sessions are published through a
bounded FIFO to `Accept`. A client that stalls during the preamble therefore
occupies one slot and one goroutine, never the loop. Each session runs the
brokerwire connection state machine (one Register, one subscription generation
series, bounded operation and stream trackers), one snapshot publisher that
writes the P3.1 multipart layout and coalesces through the core's capacity-one
subscription, and one bridge per logical stream. Streams carry the typed
session protocol: `sessionwire` runs over a bounded private byte pipe on each
side, and two relay goroutines move typed messages between the client's session
connection and the admitted broker core connection, so no session byte is
reinterpreted.

Every frame must carry the epoch and connection identity the listener assigned
at accept; a mismatched frame is fenced, and a stale stream open is answered
with a `StreamClosed` carrying the stale admission code. A malformed,
oversize, or wrong-direction envelope settles exactly the connection that sent
it. Setup is fully bounded on both halves: the client dialer bounds the Unix
dial, the preamble, the Register send, and the wait for Registered with one
context/deadline, closing the carriage to interrupt a peer that stalls after a
successful preamble and joining every setup worker, while a registered
connection detaches from that setup context; the server requires the client's
Register within the same accept-time handshake budget that bounded the preamble
and admission, so a silent same-user peer can neither pin an admitted core lease
nor hold a listener slot. Admission is the `ports.BrokerAuthority` seam,
implemented by `internal/usecase/broker.Authority`; the pre-Register guard
consults a registered marker set when Register handling begins, so a Register
arriving exactly at the deadline is admitted rather than settled as a timeout,
and a genuine registration timeout is classified as an orderly peer disconnect
so a listener draining its sessions never reports one as its own failure.
Operational bounds are explicit: concurrent clients
per listener, per-stream inbound chunks and bytes, pending mutating operations,
and the brokerwire per-connection stream and dedup bounds. Disconnect cleanup is
deterministic:
the session cancels its context, stops publishing, closes the outer carriage
(so a stream writer parked on a non-reading peer is interrupted), settles every
bridged stream (releasing its core connection and carriage), closes the
brokerwire state, joins every worker, closes the admitted core service, and
releases its client slot. Mutating operations report `outcome unknown` when a
reply is lost after the request was sent, and the client never replays them
blindly.

P3.3 introduces no production activation: nothing in `internal/app` or `main`
constructs this adapter for ordinary operation. P3.4 composes the broker use
case, its stores, and this listener behind the hidden `_broker-serve` sandbox
entry point, with the listener consuming
`ports.BrokerAuthority` instead of defining its own seam. The composing caller
owns the `Registry.Run` lifecycle: it starts the single run to own and drain the
durable writer and cancels and joins that run before closing the store, because
`NewAuthority` neither starts nor settles the run. P7 performs the coordinated
cutover.

## Broker offline sandbox (Plan 001 P3.4 slice C, not activated)

The hidden `_broker-serve --offline-root ABS [--idle-grace DURATION]` entry
point is a foreground, isolated sandbox over one operator-supplied offline root.
It is excluded from public help and changes no ordinary command, path, or
factory. The root must be absolute, cleaned, symlink-free, and outside the
production runtime and state directories; the runtime, state, and log paths are
derived only beneath it and secured with `pkg/safedir.EnsurePrivate`, and the
durable store is opened with `brokerstore.OpenOffline` over the sandbox state
directory with no legacy inputs.

`internal/adapters/brokerconfig` owns the strict sandbox configuration: a
bounded, owner-only `config.json` carrying the exact `vev.broker.offline/v1`
marker, with unknown fields, duplicate keys, trailing data, invalid UTF-8,
excessive nesting, and duplicate endpoints refused. Each immutable registration
provisions the exact daemon registration, the stable authenticated daemon
identity, the exact connection policy, and one absolute Unix mux route. The
resulting `ports.BrokerEndpointResolver` performs no I/O and invents no address:
a request must name a configured endpoint, carry its exact registration, and
request a compatible policy, so a request can never supply an address, identity,
secret, command, or policy of its own. Because the pool keys one physical
transport by the exact authenticated identity plus policy pair and deliberately
ignores the opaque route address, two registrations that share that pair must
agree on the one route the pool dials: an alias of an identical route is
accepted under distinct endpoints, while a second route for the same pair is
refused instead of being dialed for neither endpoint.

The composition samples a cryptographically random non-zero broker epoch and
builds a store-backed, observation-disabled `broker.Registry` (whose single
`Run` the supervisor owns and joins), the immutable resolver, a
`daemonmux.EndpointConnector` over `ipc.DialMuxContext`, a bounded `Pool`, the
idle `Supervisor` (5m default grace), `broker.Authority`, and the
`brokeripc` listener. A temporary startup lease pins the supervisor open until
the listener and its accept drain are ready; the drain continuously takes
admitted sessions without closing them, because each returned service is the
listener's own connection-scoped session. A signal or parent-context
cancellation, or the idle grace, shuts down in the fixed order listener ->
pool -> registry -> store -> log -> owner lock. The foreground process holds a
reusable lifetime owner lock (`internal/adapters/lifecycle.TryAcquire`) on the
sandbox runtime directory for its whole run, so a second owner fails closed.
The optional `idleGrace` field in `config.json` provisions the sandbox's
effective idle grace as a Go duration string. It is the single deterministic
source for that grace: `_broker-serve` and `_broker-launcher` refuse an explicit
`--idle-grace` that conflicts with the provisioned value, because a status probe
cannot read a running broker's effective grace through the broker IPC protocol.
When no grace is provisioned, an explicit flag (or the supervisor default)
applies.

## Broker offline connect-or-spawn and status (Plan 001 P3.4 slice D, not activated)

The hidden `_broker-launcher --offline-root ABS [--idle-grace DURATION]` entry
point is the intermediate half of a double fork: it validates the sandbox
configuration (including the idle-grace conflict rule), starts one detached
`_broker-serve` in a new session, and exits, exactly like the daemon launcher.
The hidden `_broker-status --offline-root ABS [--ensure] [--timeout DURATION]`
entry point reports the offline broker's state. Both are excluded from public
help and change no ordinary command, path, or factory.

Status without `--ensure` is dial-only: it performs one bounded probe of the
sandbox socket and prints exactly one bounded JSON document,
`{status, endpoint, epoch, revision, host_count}`, where `endpoint` is the
broker IPC socket path and the epoch, revision, and host count are zero when the
broker is offline. A missing socket and a stale socket left by a dead owner are
both recoverable absence; a foreign path, a live endpoint that fails the broker
handshake, and a stalled handshake are incompatible and never respawned over.
`--timeout` is the probe's single absolute bound in dial-only mode, so a caller
can tighten the probe or rely on the default (10s) and observe the same bound.
Dial-only status always exits zero, so the JSON is the sole result. `--ensure`
runs one absolute overall deadline (default 10s): it validates and secures the
sandbox root exactly as the launcher does before creating the spawn directory or
taking any lock. On success it prints the ready report and exits zero; on any
layout, configuration, or startup failure it still prints the offline report but
exits 3, so the caller always observes one offline JSON document and a distinct
exit code. The launcher is spawned under the ensure context
(`exec.CommandContext`), so the overall deadline or a signal bounds a launcher
that never exits; a launcher that already started the detached `_broker-serve`
is still safe to kill, because that serve is in its own session and survives.

A foreground `_broker-serve` that hits an unexpected terminal accept failure
commits the same ordered shutdown as a cancellation, so a socket that can no
longer admit work is never left bound.

Race safety rests on two descriptor-backed exclusive locks that are never
stolen by age and whose inodes are never unlinked. Both refuse any lock with
group or other access and trust an owner-only file whatever its exact owner bit
pattern, so a lock created under a restrictive umask (normalized to 0600 on
creation) or by a different tool never wedges startup; a group/other-readable
path stays refused. The broker lifetime lock
(`layout.Runtime/lifecycle.lock`) is held by the foreground `_broker-serve` for
its whole run; only the elected spawner probes it, and only as a liveness hint.
The spawn-election lock (`layout.Spawn/lifecycle.lock`, a separate private
directory beneath runtime) elects exactly one status caller to spawn. The
elected spawner re-dials after winning the lock, then holds the election until
the endpoint answers a valid broker registration and publishes its initial
snapshot, or until the deadline expires. A waiter never touches the lifetime
lock, so it can never briefly steal ownership from the broker it is waiting for,
and every wait is a bounded retry rather than a sleep loop. If the elected
spawner dies before readiness, the kernel releases its lock and a waiter takes
over; a duplicate `_broker-serve` fails its own lifetime acquisition
nonblocking. Every spawn uses the existing double-fork process pattern, and the
status probe and launcher inherit only the ambient environment minus the
performance-trace inputs.

## Broker remote mux helpers (Plan 001 P3.4 slice E, not activated)

The offline broker reaches a remote daemon's private daemonmux carriage over one
of three explicitly provisioned, bounded routes, and P3.4 slice E adds the
transport-selecting connector plus three hidden remote helpers. The routing
schema is strict and immutable: `internal/adapters/brokerconfig` parses a
registration's `route` as either the original Unix mux path string or an object
with an explicit kind, and refuses unknown kinds, unknown fields, trailing data,
and any kind/field mismatch. The bounded `ssh-stdio` and `ssh-quic` variants
carry an ssh target, an explicit remote argv, and optional trust inputs (a
known-hosts file and a connect timeout). A route can never carry an identity, a
policy, a command chosen at request time, or a secret. Identity and policy stay
authoritative on the registration, and the resolver publishes only an opaque
bounded route address: a digest of the route's own contents, so the pooling
layer never carries a target, a path, or an argv word.

`_broker-serve` composes a transport-selecting `daemonmux.EndpointConnector`. Its
dial function receives only the opaque address, looks the route up in the
immutable configuration it was built with, and refuses an address the
configuration did not produce. A `unix` route dials the provisioned private Unix
mux carriage directly; an `ssh-stdio` route starts one explicit ssh child (built
with `sshstdio.BuildCommandForMux` and carrying only trust inputs that narrow
verification, never disabling host-key checking, forcing an auth bypass, or
requesting a PTY) through `sshstdio.DialMuxContext`; an `ssh-quic` route runs one
SSH-authenticated QUIC bootstrap, reads exactly one readiness line, and pins the
freshly minted ephemeral certificate through `quic.DialMuxContext`. One
caller-supplied setup context bounds bootstrap, pinned dial, and the daemonmux
physical handshake together; `brokerMuxSetupTimeout` is an independent backstop
for the broker-side bootstrap readiness wait and the child's kill/reap cleanup,
so an earlier caller deadline stays authoritative while a helper that never
prints readiness is still interrupted and reaped within the bound. The captured
bootstrap stderr sink is read only after that kill and reap, and the sink is
itself synchronized, so the os/exec copy goroutine never races the diagnostic. A
successful physical connection detaches from that context and stays pool-owned.
QUIC credentials are minted fresh per physical
connection and consumed exactly once, and captured bootstrap stderr is bounded
and sanitized, so a token, a nonce, or raw remote text never reaches an address,
a log, or an error.

The three hidden helpers are excluded from public help. `_broker-mux-stdio
--offline-root ABS` validates the sandbox and bridges its own stdio to the single
provisioned private Unix daemonmux carriage. `_broker-mux-quic-bootstrap
--offline-root ABS` validates the sandbox, starts one detached
`_broker-mux-quic-proxy` in a new session, forwards its single readiness line,
and exits. `_broker-mux-quic-proxy --offline-root ABS` mints one ephemeral QUIC
server, admits exactly one authenticated carriage, and bridges it to the same
private Unix daemonmux carriage. A helper only ever bridges to the configured
private endpoint: it never starts a broker, an observer, or an ordinary daemon,
never dials the production daemon socket, and never fabricates a daemon
incarnation, which the daemon on the far side of the carriage alone owns.
Ordinary production composition still waits for P7; the helpers change no
ordinary command, path, or factory.
