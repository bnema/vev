// Private daemonmux QUIC carriage.
//
// This file exposes the explicit, mux-specific raw QUIC carriage the daemonmux
// multiplexer (internal/adapters/daemonmux) runs over. It introduces no
// transport and no second authentication scheme: it reuses the authenticated
// bootstrap contracts unchanged - the ephemeral certificate and the exact
// SHA-256 pin, the single-use 32-byte token, and the single-use nonce - and
// hands daemonmux the connection's one bidirectional stream as a raw bounded
// transport (wire.BoundedTransport).
//
// One QUIC connection is one daemonmux physical connection and therefore
// exactly one bidirectional stream: logical daemonmux attachments are
// multiplexed inside that stream and are never mapped to native QUIC streams.
// The one-stream guard stays in force for the whole connection lifetime, and
// the bootstrap credential is consumed exactly once, so one freshly minted
// Server is the daemon half of exactly one physical connection and
// DialMuxContext is the broker half of one.
//
// The caller-supplied setup context (or its deadline) bounds the whole setup:
// the credential's wait and expiry, the pinned TLS dial, the bounded auth
// record exchange, and the daemonmux physical handshake the caller runs
// afterwards over the returned carriage. A rejected pin, token, or nonce
// closes the carriage and hands nothing out. A successful carriage detaches
// from the setup context in the daemonmux EndpointConnector, which owns the
// physical lifetime from then on.
//
// The one-time token and nonce stay in the caller-held Readiness record, never
// in the dial address: the address is composed by the caller from the resolved
// target host plus the readiness port, exactly like the existing bootstrap
// dial. Authorization material is absent from addresses, logs, and errors.

package quic

import (
	"context"
	"errors"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
)

// ErrMuxCarriage reports a QUIC setup that produced no raw bounded carriage, or
// an AcceptMux call on a nil Server. The QUIC transport always implements
// wire.BoundedTransport, so this is a local construction fault, not a peer
// outcome; a transport that somehow lacks the capability is closed rather than
// handed to daemonmux unbounded.
var ErrMuxCarriage = errors.New("quic: mux carriage is not a bounded transport")

// DialMuxContext establishes one authenticated raw bounded daemonmux carriage.
// It reuses the bootstrap dial contract unchanged: the TLS handshake pins the
// ephemeral server's exact SHA-256 certificate fingerprint, and exactly one
// bounded token-plus-nonce auth record travels inside the encrypted single
// stream. Nothing is handed out unless the pin matched and the record was sent;
// a failed pin, refused record, or expired credential closes the carriage and
// returns an error that carries no bootstrap secret.
//
// addr is composed by the caller from the resolved target host and the
// readiness port. ctx (or timeout, whichever ends first) bounds resolution, the
// pinned dial, and the auth exchange together; the daemonmux physical handshake
// that follows shares the caller's setup context through
// EndpointConnector.Connect, so one setup deadline covers bootstrap, dial,
// auth, and mux handshake.
//
// The returned carriage owns exactly one bidirectional QUIC stream and
// satisfies the raw framed carriage daemonmux consumes (Send, RecvBounded,
// Close). Its lifetime is the caller's once returned: the one-stream guard
// stays in force, so a second native stream fails the whole carriage instead of
// becoming a logical attachment.
func DialMuxContext(ctx context.Context, addr string, readiness Readiness, config Config, timeout time.Duration, opts ...Option) (wire.BoundedTransport, error) {
	transport, err := Dial(ctx, addr, readiness, config, timeout, opts...)
	if err != nil {
		return nil, err
	}
	return asMuxCarriage(transport)
}

// AcceptMux waits for one authenticated peer on the bootstrap server and
// returns its raw bounded carriage for daemonmux. It reuses the bootstrap
// accept contract unchanged: the peer must present the single-use token and
// nonce within the auth deadline, the token is consumed atomically, and every
// failed or expired attempt closes its own carriage without handing anything
// out. Because the credential is one-time, one Server is the daemon half of
// exactly one physical daemonmux connection, so a daemon mints a fresh
// ephemeral server per accepted physical connection.
//
// ctx (or the advertised credential lifetime, whichever ends first) bounds the
// wait. A successful return transfers the carriage to the caller; daemonmux's
// ServerSupervisor owns the physical lifetime from then on.
func (s *Server) AcceptMux(ctx context.Context) (wire.BoundedTransport, error) {
	if s == nil {
		return nil, ErrMuxCarriage
	}
	transport, err := s.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return asMuxCarriage(transport)
}

// asMuxCarriage asserts the one raw bounded capability daemonmux consumes. A
// transport without it is closed rather than handed out unbounded.
func asMuxCarriage(transport wire.Transport) (wire.BoundedTransport, error) {
	bounded, ok := transport.(wire.BoundedTransport)
	if !ok {
		if transport != nil {
			_ = transport.Close()
		}
		return nil, ErrMuxCarriage
	}
	return bounded, nil
}
