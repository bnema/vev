package client

import (
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

var (
	errNilSessionConnection = errors.New("client: nil session connection")
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

// SessionConnection owns one already-open typed connection and its
// daemon-facing session target. Local and remote carriages use this owner.
type SessionConnection struct {
	connection ports.ClientConnection
	target     SessionTarget
}

// NewSessionConnection binds an already-selected typed connection to a target.
func NewSessionConnection(connection ports.ClientConnection, target SessionTarget) (*SessionConnection, error) {
	if connection == nil {
		return nil, errNilSessionConnection
	}
	if err := target.validate(); err != nil {
		return nil, fmt.Errorf("invalid session target: %w", err)
	}
	return &SessionConnection{connection: connection, target: target}, nil
}

// Connection returns the owned typed session connection.
func (c *SessionConnection) Connection() ports.ClientConnection {
	if c == nil {
		return nil
	}
	return c.connection
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
	if request.Environment != nil {
		request.Environment = append(make([]string, 0, len(request.Environment)), request.Environment...)
	}
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

// Close releases the owned connection.
func (c *SessionConnection) Close() error {
	if c == nil || c.connection == nil {
		return nil
	}
	return c.connection.Close()
}
