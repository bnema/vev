package client

import (
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/ports"
)

var errNilSessionTransport = errors.New("client: nil session transport")

func cloneAttachRequest(request AttachRequest) AttachRequest {
	if request.RemoteTarget != nil {
		target := *request.RemoteTarget
		request.RemoteTarget = &target
	}
	return request
}

// SessionConnection owns one already-open transport and the complete
// validated daemon-facing attach request. Local sockets and remote carriages
// use this same owner.
type SessionConnection struct {
	transport ports.Transport
	request   AttachRequest
}

// NewSessionConnection creates a connection for an already-selected transport.
// Request validation happens before the connection is used for a Hello.
func NewSessionConnection(transport ports.Transport, request AttachRequest) (*SessionConnection, error) {
	if transport == nil {
		return nil, errNilSessionTransport
	}
	request = cloneAttachRequest(request)
	if err := validateAttachRequest(request); err != nil {
		return nil, fmt.Errorf("invalid attach request: %w", err)
	}
	return &SessionConnection{transport: transport, request: request}, nil
}

// Transport returns the owned carriage for this connection.
func (c *SessionConnection) Transport() ports.Transport {
	if c == nil {
		return nil
	}
	return c.transport
}

// AttachRequest returns the complete request retained for this connection.
func (c *SessionConnection) AttachRequest() AttachRequest {
	if c == nil {
		return AttachRequest{}
	}
	return cloneAttachRequest(c.request)
}

// Close releases the owned transport.
func (c *SessionConnection) Close() error {
	if c == nil || c.transport == nil {
		return nil
	}
	return c.transport.Close()
}
