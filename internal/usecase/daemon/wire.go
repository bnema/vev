// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func frameWelcome(s *session, resumeToken uint64) (wire.Frame, error) {
	s.mu.Lock()
	var identity *protocol.CommittedRouteIdentity
	if s.incarnation != (domain.SessionLifecycleID{}) {
		identity = &protocol.CommittedRouteIdentity{
			Target:    protocol.ExactSessionTarget{LifecycleID: s.incarnation, SessionName: s.name},
			Ephemeral: s.ephemeral,
		}
	}
	w := protocol.Welcome{
		SessionID:         string(s.id),
		SessionName:       s.name,
		Ephemeral:         s.ephemeral,
		Capabilities:      protocol.CapabilityResume,
		CommittedIdentity: identity,
	}
	s.mu.Unlock()
	w.ResumeToken = resumeToken
	payload := wire.MarshalWelcome(w)
	if payload == nil {
		return wire.Frame{}, protocol.ErrInvalidRouteWire
	}
	return wire.Frame{Type: wire.MsgWelcome, Payload: payload}, nil
}

func frameError(code uint16, text string) wire.Frame {
	return wire.Frame{Type: wire.MsgError, Payload: wire.MarshalErrorMsg(protocol.ErrorMsg{Code: code, Text: text})}
}

func frameOutputState(b []byte, baseState uint64, state uint64, echoAck uint64) (wire.Frame, error) {
	payload, err := wire.MarshalOutput(protocol.Output{
		Epoch: 1, Base: baseState, New: state, Echo: echoAck,
		Size: domain.Size{Cols: 1, Rows: 1}, Full: state != 0 && baseState == 0, Data: b,
	})
	if err != nil {
		return wire.Frame{}, err
	}
	return wire.Frame{Type: wire.MsgOutput, Payload: payload}, nil
}

func frameDetached(reason uint8) wire.Frame {
	return wire.Frame{Type: wire.MsgDetached, Payload: wire.MarshalDetached(protocol.Detached{Reason: reason})}
}

func framePong() wire.Frame {
	return wire.Frame{Type: wire.MsgPong, Payload: wire.MarshalPong(protocol.Pong{})}
}

func frameSessions(infos []protocol.SessionInfo) wire.Frame {
	return wire.Frame{Type: wire.MsgSessions, Payload: wire.MarshalSessions(protocol.Sessions{Sessions: infos})}
}
