package ports

import (
	"context"
	"time"

	"github.com/bnema/vev/internal/protocol"
)

// ClientConnection is one typed client-side session channel. Close must be
// concurrent-safe, required to unblock ReceiveServer (and every send mode),
// and must not need a prior graceful exchange: a retained dormant attachment
// is closed by the client when it is evicted, replaced, or the runner exits.
type ClientConnection interface {
	SendClient(protocol.ClientMessage) error
	ReceiveServer() (protocol.ServerMessage, error)
	Capabilities() protocol.ConnectionCapabilities
	LinkState() LinkState
	LinkEvents() <-chan LinkEvent
	Close() error
}

// ClientDialer establishes typed client-side session connections.
type ClientDialer interface {
	Dial(context.Context) (ClientConnection, error)
}

// ServerConnection is one typed client/daemon session channel. Close must be
// concurrent-safe and unblock ReceiveClient and every send mode.
type ServerConnection interface {
	ReceiveClient() (protocol.ClientMessage, error)
	SendServer(protocol.ServerMessage) error
	SendServerAsync(protocol.ServerMessage) error
	SendServerSynchronous(protocol.ServerMessage) error
	SendOutput(protocol.Output) error
	SendOutputAsync(protocol.Output) error
	SendOutputSynchronous(protocol.Output) error
	Capabilities() protocol.ConnectionCapabilities
	LinkState() LinkState
	LinkEvents() <-chan LinkEvent
	Close() error
}

// HandshakeDeadlineProvider is an optional boundary-safe port implemented by
// accepted server connections that already own an absolute handshake deadline
// fixed before the daemon saw them - for example a connection admitted by a mux
// listener whose stream spent part of its budget waiting in an accept queue.
//
// HandshakeDeadline returns that absolute deadline, stable for the connection's
// lifetime and never restarted. A daemon-consuming use case adopts it verbatim
// for the one handshake context that covers the first ReceiveClient through
// Welcome and the committed initial publication, so queue delay and the
// handshake share a single budget; an already-elapsed deadline fails the
// handshake promptly instead of starting a second protocol.HandshakeTimeout. A
// connection that does not implement this port keeps the ordinary behavior of a
// fresh budget started when the daemon takes it.
//
// Completion is deliberately not part of this port: an accepted connection may
// finish its transport preamble earlier, but the daemon's budget must end only
// where it commits its initial publication.
type HandshakeDeadlineProvider interface {
	HandshakeDeadline() time.Time
}

// ServerListener accepts typed server-side session connections.
type ServerListener interface {
	Accept() (ServerConnection, error)
	Close() error
	Addr() string
}
