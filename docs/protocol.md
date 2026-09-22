# Vev session protocol

This document defines the observable Protobuf session contract and its QUIC,
Unix, and SSH stdio carriage rules. Each invariant maps to a current test or
benchmark.

## Message directions

`internal/protocol/messages.go` closes both unions. The daemon rejects a
wrong-direction frame before any mutation (`sessionwire.ErrWrongDirection`
→ `protocol.DecodeWrongDirection`).

Client → server: `Hello`, `Input`, `Resize`, `Detach`, `Ping`, `List`,
`Kill`, `Theme`, `Ack`, `ImagePush`, `ClientNotice`, `CommandRequest`,
`OutputResetRequest`, `RemotePreviewRequest`,
`RouteAttentionSubscription`, `SamePeerSwitchRequest`,
`RecentRouteSnapshot`, `RouteNavigationFailure`,
`SessionCreationFailure`, `UIFence`, `NavigationInventoryRequest`,
`NavigationInventoryPublication`, `NavigationInventoryFailure`,
`PickerClose`, `PickerSelection`, `PickerPreviewRequest`.

Server → client: `Welcome`, `ErrorMsg`, `Output`, `Detached`, `Pong`,
`Sessions`, `CommandResult`, `AttachTarget`, `RemotePreview`,
`CommittedRouteIdentity`, `RouteNavigationAction`,
`RouteCreateSessionAction`, `RouteNavigationFailure`, `RoutePosition`,
`RouteRetired`, `SamePeerSwitchFailure`, `UIReceipt`, `UIViewUpdate`,
`NavigationInventoryResponse`, `NavigationInventoryDemand`,
`NavigationInventorySelection`, `PickerOffer`, `PickerSnapshot`,
`PickerClosed`, `PickerResult`, `PickerFailure`, `PickerPreview`,
`KillResult`.

`RouteNavigationFailure` is the one payload that appears in both unions.
`sessionwire` conversion switches (`decodeProtoClient`/`decodeProtoServer`,
`encodeProtoClient`/`encodeProtoServer`) are exhaustive over the
directional `ClientEnvelope`/`ServerEnvelope` `oneof` unions; unknown
or opposite-direction payloads fail with `DecodeUnknownType` /
`DecodeMalformed` before any mutation.

## Handshake timeline

One absolute 15-second budget (`protocol.HandshakeTimeout`) covers dial
through the first committed publication, on both ends. Every
connection opens with exactly one preamble envelope
(`PreambleRequest` client → server, `PreambleResponse` server →
client) carrying magic, epoch, version (exact equality mandatory),
role, and the immutable per-connection ceilings; the preamble is
bounded to 4 KiB before allocation and is never replayed on resume.
`Hello.MaxOutputInFlight` stays a per-attachment claim capped by the
negotiated ceiling.

- Client (`internal/usecase/client/handshake.go`): `newHandshakeContext`
  owns a single timer; `boundedDial` detaches the dial context so a
  successful carriage outlives the handshake; `Close` is the only
  interruption mechanism for blocked transport operations.
- Daemon (`internal/usecase/daemon/handshake.go`, `daemon.go:handleConn`):
  `newHandshakeContext` + `watchHandshakeTransport` close the exact
  admitted connection on timeout; `failHandshakeAttachment` retires only
  that connection and purges a route-created session only when empty. An
  accepted connection that implements the optional
  `ports.HandshakeDeadlineProvider` seam supplies the absolute deadline it
  was admitted with, which the daemon adopts verbatim: queue delay is
  consumed instead of restarting the budget, and an already-elapsed
  deadline fails promptly. Connections without the seam keep a fresh
  budget from accept. Completion stays where the daemon commits its
  initial publication; the sessionwire preamble's own completion is not
  the handshake deadline.

First frame routing (`daemon.go:handleConn`): `Hello` → full attach;
`List`, `CommandRequest`, `RemotePreviewRequest`,
`NavigationInventoryRequest`, `Kill` are one-shot
control connections answered and closed. Anything else, including any
malformed first frame, gets a typed compatibility response and no session
mutation:

| First-frame failure kind | Response | Needs RequestID |
|---|---|---|
| `DecodeMessageHello`, version mismatch | `ErrorMsg{ErrVersionMismatch}` | no |
| `DecodeMessageHello`, malformed | `ErrorMsg{ErrInternal}` | no |
| `DecodeMessageCommand` | `CommandResult{Outcome, Code, Text}` | yes, else silent close |
| `DecodeMessageKill` | `ErrorMsg{ErrInternal}` | no — a malformed kill has no reliable RequestID to correlate a `KillResult` to |
| `DecodeMessageRemotePreview` | `RemotePreview{Status: Malformed}` | no |
| `DecodeMessageNavigationInventory` | `NavigationInventoryResponse{Invalid / VersionMismatch}` | yes, else silent close |
| other / unknown | `ErrorMsg{ErrInternal, "expected hello"}` | no |

Live checks after decode: `Hello.Version`, `CommandRequest.Version`,
`NavigationInventoryRequest.Version`, and `RemotePreviewRequest.Version`
(independent catalogue-era schema version `1`) must equal the negotiated
constant or the request fails
with the version-mismatch response above. There is no multi-version
negotiation: equality is mandatory.

Command requests additionally carry a 10-second result deadline
(`daemon/command_tracker.go:CommandRequestTimeout`), tracked per
connection and correlated by `RequestID`. Every dispatched request produces
one closed `CommandResult` outcome: succeeded, failed, or outcome unknown.
Timeout or reply loss after dispatch is outcome unknown and is never replayed.

## Broker negotiation

Broker connections are a separate conversation from the session protocol.
`internal/adapters/brokerwire` owns it: client tags 101-111 and server tags
201-209 are disjoint from every session tag, and each direction is its own
closed directional `oneof` union. A broker connection opens with exactly one
broker preamble (`PreambleRequest` client → server, `PreambleResponse` server
→ client) using broker roles 3/4. The preamble carries the same magic
(`wire.PreambleMagic`), epoch (`wire.ProtocolEpoch`), exact `protocol.Version`
equality, and negotiated ceilings as the session handshake (envelope
1 MiB..16 MiB, stream chunk 1..64 KiB), and the serialized preamble is
bounded to 4 KiB (`wire.PreambleLimit`, exposed as
`brokerwire.CheckPreambleSize`) before scan or allocation.

Refusals use the shared `PreambleRejectionCode` taxonomy. There is no
capability negotiation in P3.1: nonzero capability bits are refused as
code 7 (`limit refused`), and code 6 (`out of order`) is reserved for the
connection dispatcher, which owns framing-order detection and never surfaces
it from the stateless codec. Brokerwire is not activated by production
composition until P3.2. The P3.3 private local endpoint
(`internal/adapters/brokeripc`) carries this conversation over one per-user
AF_UNIX socket: `broker.sock` in the per-user runtime directory, with
same-user peer-credential admission, bounded per-connection queues, and
production composition deferred to P3.4 and the P7 cutover. P3.4 adds
the hidden, isolated offline sandbox entries -- `_broker-serve`,
`_broker-launcher`, and `_broker-status` -- over an operator-supplied private
root, plus the hidden remote mux helpers `_broker-mux-stdio`,
`_broker-mux-quic-bootstrap`, and `_broker-mux-quic-proxy` that bridge a
provisioned private daemonmux Unix carriage to stdio or to one freshly
authenticated QUIC stream; ordinary production composition still waits for P7.
Setup is fully bounded: the dialer's one context/deadline covers the Unix
dial, preamble, Register send, and wait for Registered, and the server requires Register within
the same accept-time handshake budget that bounded the preamble and admission.

## Output, ACK, and flow control

An `Output` state is `(Epoch, Base, New, Echo, ViewRevision, Size, Full,
Context?, Data)`. State-bearing output (`New != 0`) requires a validated
`ViewContext` (non-zero publication, committed route identity, valid tab
and pane IDs); side-effect output (`New == 0`) carries no context and no
base. `ValidateOutput` enforces the epoch/base/new chain
(`New == Base+1` unless `Full` with `Base == 0`).

- `Ack{Epoch, State}` is cumulative per epoch; epoch `0` is invalid.
- At most `MaxOutputWindow` (8) states may be in flight per attachment
  (`daemon.go:maxUnackedOutputStates`); the daemon negotiates the window
  down from the transport capabilities (`Hello.MaxOutputInFlight` claim
  capped by the transport ceiling; datagram carriage reports 1).
- The client coalesces ACKs (`client.go:cumulativeAckQueue`) and keeps
  applying + acknowledging daemon frames even while a picker lease owns
  the terminal (see below) — admission without physical write.
- `Echo` carries the client's last input acknowledgement inside output;
  `OutputResetRequest` restarts the chain; `UIFence{ActionID}` orders UI
  operations and `UIReceipt` reports the processed boundary
  (`Processed` requires epoch/state/publication; `Unavailable` carries
  none).

Compression (`sessionwire` Output converters): only full snapshots at or above
1024 bytes are zlib candidates, and only when the compressed form plus
header is smaller. Decoded length is validated against
`MaxOutputDataLen` before allocation; decompression rejects
length-excess, trailing bytes, and non-`Full` compressed states.

Byte ownership: `Recv` returns freshly allocated, right-sized payload
copies the caller may retain; `Send`/`SendAsync` copies into the
adapter's queue. IPC reuses one grow-once read buffer internally.

## Picker admission without physical write

While a client-picker lease is acquiring/owned/releasing
(`client/picker_lease.go`), the client still applies every accepted
daemon frame to its output shadow and acknowledges it inside the bounded
window, but suppresses the physical write until the barrier
(epoch/state) is applied (acquire), an authoritative post-close full
paint arrives (release), or the lease aborts (disconnect/generation
change). The daemon side drops raw key/mouse input for that attachment
while `pickerOpen`; only typed picker messages act.

## Ordering, close, and admission semantics

- Each connection is one ordered reliable stream. Concurrent `Send` calls
  are serialized by a single writer (IPC) or a mutex (SSH stdio);
  exactly one `Recv` caller at a time.
- `Close` is concurrent-safe, idempotent, and unblocks in-flight
  `Send`/`Recv` with a terminal error (`io.EOF` on clean close).
- Bounded admission, three preserved contracts (`ports` +
  `wire.Transport`): synchronous wire attempt (`Send`), bounded async
  admission (`SendAsync`, 8 queued frames / 32 MiB bytes,
  `ErrBackpressure` when full), owned bounded synchronous operation.
  Buffer ownership, cancellation, and error semantics of each
  return are unchanged. Synchronous sends rendezvous with the writer
  and carry no async byte-budget charge; async sends are charged on
  admission and released exactly once on dequeue.

Current state: the one-shot kill control paths have moved to the explicit
result protocol. `Kill` carries a unique nonzero `RequestID`, and the daemon
answers every normally decoded kill request with exactly one correlated
`KillResult` before the control connection closes: `KillSucceeded`,
`KillFailed` (with a bounded structured `Failures` list), or a definite
`KillOutcomeUnknown`. The client consumes that result; a close without a
result, a mismatched `RequestID`, or a wrong-typed reply is `outcome unknown`
and is never replayed, so a control failure whose response cannot be delivered
is never mistaken for success. `CommandRequest` follows the same correlated
model through `CommandResult`: succeeded, failed, or outcome unknown. A timeout,
close, cancellation, malformed reply, or mismatched `RequestID` after send is
outcome unknown and is never replayed.

The remaining one-shot controls (`List`, remote preview, inventory, and picker
control) do not treat close as success, but they have no closed outcome field.
The daemon attempts one response and then closes; a client that receives no
valid response reports a receive error. An undeliverable response is logged
only where the handler has a send-failure path, so these controls remain weaker
than `KillResult` and `CommandResult`.

## Limits

| Item | Ceiling | Owner |
|---|---|---|
| Preamble envelope | 4 KiB (`wire.PreambleLimit`), exact magic/epoch/version/role | `protocol/wire/preamble.go`, `sessionwire/proto_handshake.go` |
| Absolute envelope | 16 MiB (`wire.AbsoluteEnvelopeLimit`), enforced before allocation | `protocol/wire/preamble.go`, `streamframe` |
| Control (non-Output, non-image) envelope | 1 MiB (`wire.ControlEnvelopeLimit`) | `protocol/wire/preamble.go` |
| `Output.Data` | `MaxOutputDataLen` = 16776809 (16 MiB minus worst-case Protobuf overhead) | `protocol/session.go` |
| Image envelope | 16 MiB absolute (image data rides the envelope ceiling) | `sessionwire` image converters |
| Queued envelopes per connection | 8 messages, 32 MiB bytes (`streamframe` defaults), then backpressure; sync sends uncharged, async charged once | `adapters/streamframe` |
| Encoding buffer | bounded per-connection framing buffers owned by `streamframe` | `adapters/streamframe` |
| Output states in flight | 8 (`MaxOutputWindow`) preferred for QUIC/IPC/SSH | daemon, negotiated down |
| QUIC unauthenticated peers | 4 connections, 3 s auth deadline, one bidirectional stream each; extra/unidirectional streams rejected | `adapters/quic` |
| QUIC bootstrap | 32-byte token + 16-byte nonce (base64), ≤4 KiB readiness (schema v1, host-independent port, SHA-256 pin, ≤15 s expiry), one bounded (512 B) auth record, atomic one-time consumption, token erasure on close | `adapters/quic` bootstrap, `app/_quic-bootstrap` + `_quic-proxy` |
| Handshake, dial → first committed publication | 15 s, single absolute deadline propagated unchanged, never reset | both ends |
| Command result | 10 s per request | `daemon/command_tracker.go` |
| `RemotePreview` viewport | 256×128 cells, 1 MiB | `protocol/preview.go` |
| Route snapshot publication | 32 entries | `protocol/routes.go` |
| Route label | 256 bytes, UTF-8, no controls | `protocol/routes.go` |
| SSH shutdown wait | 3 s (`sshCloseTimeout`) | `adapters/sshstdio` |

Framing on IPC/SSH stdio/QUIC stream: 4-byte big-endian length, then one complete
serialized Protobuf envelope (no type byte). Zero-length envelopes and
lengths above the ceiling fail without allocation. The strict scanner
rejects short reads, trailing bytes, duplicate variants, and unknown
fields before generated unmarshal.

## Authentication boundary

No session bytes are accepted before authentication and version
acceptance. Local carriage requires the preamble + `Hello`/`Welcome`
inside the 15-second budget; a failed handshake closes the exact
connection. Direct remote carriage is QUIC (`internal/adapters/quic`,
TLS 1.3, ALPN `vev/1`, exact SHA-256 pin, one bidirectional stream,
no 0-RTT); `VEV_REMOTE_TRANSPORT=stdio` selects SSH stdio instead.
Each remote attach starts one short-lived `_quic-bootstrap` over SSH
which spawns a detached `_quic-proxy` (ephemeral listener +
certificate, 32-byte token, ≤4 KiB readiness with host-independent
port/pin/expiry), prints the single readiness line, and exits. The
client resolves the SSH target host, composes `host:port`, pins the
certificate, and sends one bounded auth record before any preamble.
The token is consumed atomically on first use; concurrent, replayed,
or expired auth fails closed without IPC access, and the proxy exits
after session termination, expiry, or setup failure with token
material erased.

## Error taxonomy

- Envelope level: short payload, trailing bytes, duplicate variants,
  unknown fields, invalid wire types, oversize — all fail closed before
  generated unmarshal, no partial values escape.
- Typed boundary: `protocol.DecodeFailure{Category, Kind, Version,
  RequestID?, Err}` with categories
  `Malformed | UnknownType | WrongDirection`. The numeric `Type` field is
  retained only for log compatibility; direction is structural (closed
  `oneof` unions per direction). Payload bytes are never retained.
- Transport level: `io.EOF` (close), `ErrFrameTooLarge`,
  `ErrZeroLengthFrame`, `ErrBackpressure`, process-exit errors for SSH.
  `LinkState` reports transport connectivity only, never semantic
  handshake acceptance.

## Benchmarks

Run the protocol, framing, and typed-connection benchmarks from the repository
root:

```sh
go test ./internal/protocol/wire -run '^$' -bench 'BenchmarkHelloEnvelope|BenchmarkInputEnvelope|BenchmarkImageEnvelope|BenchmarkOutput' -benchmem -count 5
go test ./internal/adapters/ipc -run '^$' -bench 'BenchmarkTransport' -benchmem -count 5
go test ./internal/adapters/sessionwire -run '^$' -bench 'BenchmarkTyped|BenchmarkOutputProductionRoundTrip' -benchmem -count 5
```

Scanner specifications are memoized by descriptor (`specCache`), so each
message shape pays reflection once per process. Application and QUIC performance
budgets and reproducible evidence commands live in [performance.md](performance.md).

## Tooling

- QUIC: `github.com/quic-go/quic-go v0.62.0` (MIT). Verified API surface:
  TLS 1.3 (`MinVersion: tls.VersionTLS13`), ALPN-gated single bidirectional
  stream (`AcceptStream` server / `OpenStreamSync` client,
  `MaxIncomingStreams: 1`, `MaxIncomingUniStreams: 0`, extra streams
  rejected), stream cancellation (`CancelRead`/`CancelWrite`), stream
  deadlines (`SetDeadline`/`SetReadDeadline`/`SetWriteDeadline`),
  keepalive/idle limits (`KeepAlivePeriod`, `MaxIdleTimeout`,
  `HandshakeIdleTimeout`), listener ownership (`Listener.Accept/Close`),
  explicit close mapping (`CloseWithError`), 0-RTT disabled by using only
  `DialAddr`/`ListenAddr` (never `*Early`), resumption observable via
  `ConnectionState.Used0RTT`, tracing hook for observability.
  No QUIC types leak past `internal/adapters/quic` + `internal/app`
  composition; no datagrams, no 0-RTT mutations.
- Protobuf: `google.golang.org/protobuf v1.36.12` runtime +
  `protoc-gen-go v1.36.12` pinned as a Go tool
  (`tool google.golang.org/protobuf/cmd/protoc-gen-go` in `go.mod`);
  generation via `github.com/bufbuild/buf/cmd/buf v1.73.0` pinned as a Go
  tool (`tool github.com/bufbuild/buf/cmd/buf`). Sources live in
  `internal/protocol/wire/schema/`, generated Go lands directly in
  `internal/protocol/wire` (never edited); reproducibility command:
  `go tool buf generate` (config `buf.gen.yaml`), verified by
  `Makefile:protocol-check`. No gRPC.
- Supporting dependencies are pinned in `go.mod` and `go.sum`; QUIC requires
  the corresponding `x/crypto`, `x/net`, and `x/sys` modules.
