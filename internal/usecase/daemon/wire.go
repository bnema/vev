// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

func frameWelcome(s *session, resumeToken uint64) (ports.Frame, error) {
	s.mu.Lock()
	var identity *protocol.CommittedRouteIdentity
	if s.incarnation != (domain.SessionLifecycleID{}) {
		identity = &protocol.CommittedRouteIdentity{
			Target:    protocol.ExactSessionTarget{LifecycleID: s.incarnation, SessionName: s.name},
			Ephemeral: s.ephemeral,
		}
	}
	w := ports.Welcome{
		SessionID:         string(s.id),
		SessionName:       s.name,
		Ephemeral:         s.ephemeral,
		Capabilities:      ports.CapabilityResume,
		CommittedIdentity: identity,
	}
	s.mu.Unlock()
	w.ResumeToken = resumeToken
	payload := ports.MarshalWelcome(w)
	if payload == nil {
		return ports.Frame{}, protocol.ErrInvalidRouteWire
	}
	return ports.Frame{Type: ports.MsgWelcome, Payload: payload}, nil
}

func frameError(code uint16, text string) ports.Frame {
	return ports.Frame{Type: ports.MsgError, Payload: ports.MarshalErrorMsg(ports.ErrorMsg{Code: code, Text: text})}
}

func frameOutputState(b []byte, baseState uint64, state uint64, echoAck uint64) (ports.Frame, error) {
	payload, err := ports.MarshalOutput(protocol.Output{
		Epoch: 1, Base: baseState, New: state, Echo: echoAck,
		Size: domain.Size{Cols: 1, Rows: 1}, Full: state != 0 && baseState == 0, Data: b,
	})
	if err != nil {
		return ports.Frame{}, err
	}
	return ports.Frame{Type: ports.MsgOutput, Payload: payload}, nil
}

func frameDetached(reason uint8) ports.Frame {
	return ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: reason})}
}

func framePong() ports.Frame {
	return ports.Frame{Type: ports.MsgPong, Payload: ports.MarshalPong(ports.Pong{})}
}

func frameSessions(infos []ports.SessionInfo) ports.Frame {
	return ports.Frame{Type: ports.MsgSessions, Payload: ports.MarshalSessions(ports.Sessions{Sessions: infos})}
}
