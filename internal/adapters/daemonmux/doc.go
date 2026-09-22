// Package daemonmux is the Plan 001 P3.2 daemon transport adapter.
//
// daemonmux will own one pooled physical connection to an authenticated
// daemon and the independently cancellable logical streams multiplexed over
// it: carriage framing, per-stream identity and bounded queues, admission, and
// the terminal publication that fences every affected stream. Like the
// brokerwire codec (P3.1) and the broker pool core (P2.2), it is not yet
// activated by production composition.
//
// The stateless wire adaptation is additively complete: the private
// directional codec over the daemonmux Protobuf envelopes (codec.go), its
// bounded ceilings (limits.go), its adapter-local message values
// (messages.go), the lossless conversions (convert.go), and the physical
// preamble that negotiates the per-connection ceilings, the authenticated
// daemon identity, and the accepted policy once (preamble.go). The negotiated
// effective minima are exported as MuxCeilings and are the seam the engine and
// scheduler constructors consume (NewStreamEngineWithCeilings,
// NewSchedulerWithCeilings); the default constructors delegate with the
// package policy. Encode and decode operate on one complete serialized
// envelope at a time over strict wire.ScanEnvelope. The in-memory stream
// table, admission, bounded inbound queues, and terminal publication live in
// engine_state.go. No socket, framing engine, or composition is introduced
// here.
//
// The daemon-side physical binding (binding.go) and the stateful physical
// preamble handshake (handshake.go) are the connection's authority layer. A
// ServerBinding is a validated, immutable (identity, incarnation) value over a
// non-empty closed set of distinct provisioned policy admissions, each naming
// the exact policy for one carriage shape and its locality (ServerPolicyAdmission).
// The broker-side handshake verifies the accepted response against the
// independently resolved endpoint identity and policy, the nonzero
// incarnation, and the ceilings it offered; the daemon-side handshake enforces
// the immutable binding instead of echoing the requested policy, resolves the
// accepted member (policy plus origin), negotiates
// the element-wise minima, and refuses with the precise shared rejection code.
// One caller-supplied context or deadline bounds the single request/response
// exchange, and every failure closes the carrier; a success leaves it open for
// the pump its owner starts afterwards. Neither file starts a pump, admits a
// stream, or introduces a socket, framing engine, or production composition.
//
// The private terminal-state primitive (terminal.go) is a deliberate engine
// foundation, not a stateless-codec concern: it introduces the publish-once
// terminal outcome that backs Done, Err, and FailureKind on a physical
// connection and on every stream, because that contract is the engine's
// foundation and defining it up front keeps the multiplexer carriage layered
// on top without changing it. It carries no framing, streams, queues, or
// socket.
//
// The outbound fair scheduler (scheduler.go) is the engine's outbound half: it
// owns one bounded, independently cancellable FIFO queue per admitted stream
// and orders already-encoded envelopes with byte-deficit round robin, a small
// bounded control priority, and immediate per-stream discard on Reset. It
// performs no I/O and owns no goroutine; the physical writer that drains it is
// pump.go. No socket, framing engine, listener, or composition is introduced
// here.
//
// The physical pump (pump.go) is the connection's carriage half: it composes
// the stateless directional codec, the stream engine, the fair scheduler, and
// the publish-once terminal state over the private FramedCarrier abstraction
// with exactly one reader and one writer goroutine. The reader strict-decodes
// each inbound direction and applies the engine; the writer drains the
// scheduler's fairly ordered immutable envelopes; a coalescing wake channel
// keeps the writer from polling or sleeping. A malformed outer envelope or a
// carrier Receive/Send error publishes the physical terminal outcome before
// every stream is terminalized, a stream-local semantic, reference, or queue
// refusal schedules exactly one Reset without touching siblings, and
// cancellation is an orderly close. An inbound Open that duplicates or replays
// an identity the engine already owns is ignored, so it never disturbs the
// live or retired stream it names; a fresh Open the engine refuses for
// capacity, a closed connection, or an invalid authority is answered with
// exactly one typed Mux Refused carrying its admission code and records no
// stream. An outbound queue overflow is settled the same way: the overflowing
// stream's engine record is reset and its one Reset is scheduled, and an
// inbound Close or Reset retires the matching scheduler record and discards
// its queued outbound frames. An orderly inbound Close preserves the inbound
// data that was already accepted: the stream moves to closing, accepts no new
// data, and stays readable until Take drains the last queued chunk, which is
// when it terminalizes and releases its slot; a Reset always discards the
// preserved queue at once. Flush is the explicit write barrier for callers that
// must publish a final frame before closing; Close itself cancels the writer
// and does not drain. Close unblocks the carrier through the adapter
// prompt-close contract and joins both goroutines. The carrier stays an
// abstract, testable seam: no socket, listener, streamframe, sessionwire, or
// ports physical implementation is introduced here.
//
// The raw-carrier bridge (carrier.go) is the connection's framing boundary: it
// adapts an existing raw framed transport - IPC, QUIC, or SSH stdio, each a
// wire.BoundedTransport - to the private FramedCarrier the pump and the
// preamble handshake drive. Send is synchronous; Receive is bounded to
// wire.PreambleLimit until the explicit, one-way Negotiate transition records
// the negotiated daemonmux envelope ceiling, so no receive allocates a body a
// peer did not bound. Close is idempotent and concurrent-safe, interrupts a
// blocked read or write, and joins every worker. It reuses the shared
// streamframe framing and introduces no socket, listener, sessionwire, or frame
// format of its own.
//
// The typed server-side listener (listener.go) is the daemon half of one
// physical connection: the pump's reader admits each incoming MuxOpen and
// notifies the listener through the pump's narrow admission-observer seam, the
// listener queues admitted streams in a bounded FIFO accept queue (at most
// MaxAcceptQueue, 128), and Accept hands each one back as an independent typed
// ports.ServerConnection built with sessionwire over that mux stream. Every
// stream carries one absolute local handshake deadline fixed at Open admission
// (protocol.HandshakeTimeout) that queue delay and the session handshake share;
// the deadline and the handshake completion signal are exposed to the daemon
// through the narrow optional HandshakePlumbing interface, so the daemon adopts
// this one deadline instead of starting a second one. Every admitted connection
// is stamped with the provisioned admission its listener accepted - the exact
// accepted policy and carriage origin plus the peer's declared purpose, closed
// admission variant, name, exact target, and bounded environment - and exposes
// it through the optional ports.SessionAdmissionProvider so a daemon use case
// consumes the accepting side's authority rather than the peer's Open. An Open
// whose declared locality contradicts the accepted origin (a liar Local) or that
// fails the closed admission contract is refused on its own stream. A stream that reaches its
// deadline, is refused because the accept queue is full, or is reset by the
// peer is settled on its own: exactly one mux Reset, its admission slot
// released, and no effect on a sibling or on the physical connection. An
// admission whose absolute deadline elapsed is settled by Accept itself, even
// before the watchdog observes it, and a connection whose inner session
// carriage closes on its own during the handshake (a sessionwire preamble
// failure) is settled by its terminal watcher with exactly one stream-local
// Reset, so no consumer Close is needed. Accept
// therefore never fails for a stream-local failure; it fails only when the
// listener was closed or when the one physical connection reached its terminal
// outcome, which is exactly when the shared daemon accept loop must stop. Close
// unblocks Accept, drains every admitted-but-unaccepted stream through its one
// stream-local Reset (releasing each slot and unblocking the broker's pending
// Open), and joins the deadline watchdog without closing the pooled physical
// pump. The listener is constructed before its pump starts: it registers the
// pump's single admission observer, which the pump refuses after Start. No
// socket, framing engine, or composition is introduced here.
//
// The aggregate listener (aggregate.go) is the daemon's global
// ports.ServerListener over several physical listeners: it owns one forwarding
// goroutine per registered child, delivers each child's independently accepted
// typed connection through one bounded FIFO, and isolates a child's terminal
// Accept failure - a lost or closed physical carriage - instead of surfacing it
// as the aggregate's own error. So the loss of one physical daemonmux transport
// never stops the daemon's shared accept loop while another or a newly
// registered child can still serve. Register and Remove take and release
// ownership of a child; Close unblocks Accept, closes every owned child, joins
// every forwarder, and closes every accepted-but-unaccepted connection. The
// aggregate consumes the typed ports.ServerListener/ports.ServerConnection
// contracts only, so it composes with the daemonmux Listener, the sessionwire
// listener, or any later physical listener. No socket, framing engine, or
// production composition is introduced here.
//
// The broker endpoint connector (connector.go) is the broker half of one
// pooled physical connection: EndpointConnector takes an injected narrow
// authenticated raw-carrier dial function, runs the physical preamble over the
// FramedCarrierBridge with the resolved endpoint's identity and policy as the
// authoritative values, negotiates the effective ceilings, and hands the live
// pump and LogicalConnector to a PhysicalConnection. One caller-supplied setup
// context or deadline bounds dial and handshake together; a successful
// connection is deliberately detached from it, so cancelling or expiring the
// setup context never stops a pooled physical connection, which Close alone
// owns. PhysicalConnection reports the immutable accepted identity,
// incarnation, and policy, delegates Done, Err, and FailureKind to the pump's
// publish-once terminal authority, exact-policy-checks every OpenStream before
// delegating to the LogicalConnector, and tears the pump and carrier down
// exactly once.
//
// The server supervisor (supervisor.go) is the daemon's owner of accepted raw
// physical carriage: Adopt bounds concurrent handshakes and admissions,
// completes the physical preamble against the immutable ServerBinding,
// restricts the pump's engine to the accepted physical policy so every inbound
// Open is checked before fresh stream admission, builds the typed Listener
// before the pump starts, and registers one owned child - which closes
// listener, pump, and carrier together - with the AggregateListener. One
// reaper per child closes and deregisters it on its own terminal outcome,
// isolating a lost or failed physical child from every sibling and from the
// aggregate, and Close tears every owned child down and joins the reapers
// without closing the caller-owned aggregate. The carriage stays an injected,
// abstract seam: daemonmux never dials, listens, authenticates, or inspects a
// socket, and no identity persistence or production activation is introduced
// here.
//
// The private Unix carriage (P3.2d) lives in internal/adapters/ipc, not here:
// ListenMux and DialMuxContext bind and connect a caller-supplied isolated
// AF_UNIX path (never the live daemon.sock) and return a raw bounded transport
// the EndpointConnector and ServerSupervisor consume unmodified. Listener setup
// creates or validates an owner-only parent and socket, recovers race-safely
// from a stale socket, refuses a foreign non-socket path, and both accept and
// dial verify same-user kernel peer credentials (SO_PEERCRED on Linux), failing
// closed on a platform or build without the check. Close unblocks Accept and
// I/O and unlinks only the socket inode the listener created. daemonmux gains
// no socket, dial/listen, or peer-credential code from this slice.
//
// The private QUIC carriage (P3.2e) lives in internal/adapters/quic, not here:
// quic.DialMuxContext and quic.Server.AcceptMux reuse the existing
// authenticated bootstrap contracts unchanged - the ephemeral certificate and
// its exact SHA-256 pin, the single-use 32-byte token, and the single-use
// nonce - and hand the connection's one bidirectional stream to the same
// EndpointConnector and ServerSupervisor as a raw bounded transport
// (wire.BoundedTransport). One QUIC connection is therefore one physical
// daemonmux connection and exactly one native QUIC stream: logical attachments
// are multiplexed inside that stream and are never mapped to native QUIC
// streams, the one-stream guard stays in force for the whole connection
// lifetime, and one freshly minted bootstrap server admits exactly one
// authenticated carriage. The caller's setup context bounds the credential
// wait, the pinned dial, the bounded auth record, and the physical preamble
// the connector runs afterwards; a refused pin, token, or nonce closes the
// carriage and hands nothing out, and a successful carriage detaches from the
// setup context exactly as the Unix and in-memory carriages do. daemonmux
// gains no socket, dial, TLS, or credential code from this slice.
//
// The private SSH stdio carriage lives in internal/adapters/sshstdio, not here:
// sshstdio.DialMuxContext starts one caller-supplied explicit command over a
// local ssh child process whose stdin/stdout are the raw bounded carriage, and
// sshstdio.NewStdioTransport is its helper half over the remote process' own
// stdio. The command is supplied by the caller, never the `vev _stdio` session
// command, so a mux carriage cannot become a second navigation path to a
// session; host-key and authentication policy stay owned by the target's
// effective OpenSSH configuration, and the carriage requests no remote PTY
// (`-T`). One child process is one physical daemonmux connection, exactly like
// the Unix and QUIC carriages: every logical attachment is multiplexed inside
// its stdio stream and is never mapped to another process. The caller's setup
// context bounds subprocess start and the physical preamble the connector runs
// afterwards together; a successful carriage detaches from it, and Close
// terminates and joins the subprocess. The child's stderr is captured bounded
// and sanitized, and a non-clean exit surfaces a typed sanitized error, never
// raw remote stderr. daemonmux gains no process, pipe, or framing code from
// this slice.
package daemonmux

import "path/filepath"

// SocketPath is the canonical production daemonmux endpoint beneath ipc.SocketDir().
func SocketPath(socketDir string) string { return filepath.Join(socketDir, "daemonmux.sock") }
