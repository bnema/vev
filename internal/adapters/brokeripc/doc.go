// Package brokeripc is the Plan 001 P3.3 private local broker IPC adapter.
//
// It exposes the client and server halves of one per-user Unix endpoint: a
// client adapter that implements ports.BrokerService over the brokerwire
// conversation, and a listener adapter that implements ports.BrokerListener by
// accepting same-user client connections and running one brokerwire session per
// accepted connection. The wire contract is exactly P3.1's brokerwire preamble
// and directional codec over the shared streamframe carriage; this package adds
// no message, tag, or preamble of its own.
//
// The carriage itself is the P3.2 private AF_UNIX carriage in
// internal/adapters/ipc: the endpoint lives in an owner-only (0700) directory
// created or validated by pkg/safedir.EnsurePrivate, the bound socket is
// tightened to 0600, a stale socket left by a dead owner is recovered
// race-safely, a live owner is reported instead of evicted, and a path that is
// not a socket is refused without being removed. Both accept and dial verify
// the connected peer's kernel credentials (SO_PEERCRED on Linux) and fail
// closed on a platform or build without that check, so no admission decision
// rests on filesystem permissions or an X11-style "the directory is private"
// assumption.
//
// One endpoint is one brokerwire conversation. The server assigns the
// connection scope at accept, answers the preamble with the negotiated
// ceilings, and waits for exactly one Register before admitting subscription,
// operation, or stream work; every later frame must carry the assigned epoch
// and connection identity or it is refused as stale without touching session
// state. Setup is fully bounded on both halves. The client dialer bounds the
// Unix dial, the preamble exchange, the Register send, and the wait for
// Registered with one context/deadline; a context cannot interrupt blocking
// framing I/O, so a setup step closes the carriage on expiry and joins its
// worker, which is what stops a peer that stalls after a successful preamble,
// and a connection that registered detaches from the setup context. Symmetrically
// the server requires the client's Register within the same accept-time
// handshake budget that bounded the preamble and admission, so a same-user peer
// cannot complete the preamble, take an admitted core lease, and then hold that
// lease and its listener slot indefinitely by staying silent. Bounds are
// explicit: accepted clients are capped per listener, stream
// inbound queues are bounded per stream, pending operations are capped per
// connection, and the brokerwire per-connection trackers bound streams and
// completed-operation dedup. Every accepted connection owns its own reader,
// publisher, and stream relays, so a slow or stalled client blocks only its own
// connection.
//
// A malformed or oversize outer envelope, a wrong-direction payload, and a
// frame for an identity that was never allocated are protocol violations and
// settle that connection alone. Disconnect cleanup is deterministic: the
// session cancels its context, terminates every bridged stream against the
// admitted broker service, closes the carriage, and joins every worker before
// Close returns.
//
// Like the brokerwire codec (P3.1) and the daemonmux carriage (P3.2), this
// adapter is not activated by production composition: no ordinary command,
// path, or factory constructs it. P3.4 composes the broker use case, its
// stores, and this listener behind the hidden `_broker-serve` sandbox entry
// point, and slice D adds the hidden `_broker-launcher` and `_broker-status`
// detached connect-or-spawn helpers over the same private root; P7 performs the
// coordinated production cutover.
package brokeripc
