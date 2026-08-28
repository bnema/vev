package ports

import "github.com/bnema/vev/internal/protocol"

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

// ServerListener accepts typed server-side session connections.
type ServerListener interface {
	Accept() (ServerConnection, error)
	Close() error
	Addr() string
}
