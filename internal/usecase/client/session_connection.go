package client

import (
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

var (
	errNilSessionTransport  = errors.New("client: nil session transport")
	ErrEphemeralSessionName = errors.New("client: ephemeral sessions cannot have a name")
)

// SessionTarget is the validated daemon-facing session selection. Endpoint
// details are resolved before a transport is opened and never enter this type.
type SessionTarget struct {
	Intent      uint8
	SessionName string
}

func (t SessionTarget) validate() error {
	switch t.Intent {
	case protocol.IntentEphemeral:
		if t.SessionName != "" {
			return ErrEphemeralSessionName
		}
		return nil
	case protocol.IntentNew, protocol.IntentAttach, protocol.IntentResume:
		if t.SessionName == "" {
			return domain.ErrInvalidSessionName
		}
	default:
		return fmt.Errorf("invalid session intent %d", t.Intent)
	}
	return domain.ValidateSessionName(t.SessionName)
}

// SessionConnection owns one already-open transport and its daemon-facing
// session target. Local sockets and remote carriages use this same owner.
type SessionConnection struct {
	transport ports.Transport
	target    SessionTarget
}

// NewSessionConnection creates a connection for an already-selected transport.
// Target validation happens before the connection is used for a Hello.
func NewSessionConnection(transport ports.Transport, target SessionTarget) (*SessionConnection, error) {
	if transport == nil {
		return nil, errNilSessionTransport
	}
	if err := target.validate(); err != nil {
		return nil, fmt.Errorf("invalid session target: %w", err)
	}
	return &SessionConnection{transport: transport, target: target}, nil
}

// Transport returns the owned carriage for this connection.
func (c *SessionConnection) Transport() ports.Transport {
	if c == nil {
		return nil
	}
	return c.transport
}

// AttachRequest returns the common request shape used after transport
// selection. It contains no remote/proxy semantic.
func (c *SessionConnection) AttachRequest() AttachRequest {
	if c == nil {
		return AttachRequest{}
	}
	return AttachRequest{Intent: c.target.Intent, SessionName: c.target.SessionName}
}

// cloneAttachRequest keeps the immutable request independent from transport
// ownership while protecting the exact remote target from caller mutation.
func cloneAttachRequest(request AttachRequest) AttachRequest {
	if request.RemoteTarget != nil {
		target := *request.RemoteTarget
		request.RemoteTarget = &target
	}
	if request.ExactTarget != nil {
		target := *request.ExactTarget
		request.ExactTarget = &target
	}
	return request
}

// Close releases the owned transport.
func (c *SessionConnection) Close() error {
	if c == nil || c.transport == nil {
		return nil
	}
	return c.transport.Close()
}
