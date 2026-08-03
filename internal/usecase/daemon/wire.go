// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func frameWelcome(s *session, ac *attachedClient) ports.Frame {
	capabilities := ports.CapabilityResume
	if ac.proxied {
		capabilities |= ports.CapabilityProxied
	}
	w := ports.Welcome{
		SessionID:    string(s.id),
		SessionName:  s.name,
		Ephemeral:    s.ephemeral,
		Capabilities: capabilities,
		ResumeToken:  ac.resumeToken,
	}
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(w)}
}

func frameError(code uint16, text string) ports.Frame {
	return ports.Frame{Type: ports.MsgError, Payload: ports.MarshalErrorMsg(ports.ErrorMsg{Code: code, Text: text})}
}

func frameOutputState(b []byte, baseState uint64, state uint64, echoAck uint64) ports.Frame {
	return ports.Frame{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{
		Epoch: 1, Base: baseState, New: state, Echo: echoAck,
		Size: domain.Size{Cols: 1, Rows: 1}, Full: state != 0 && baseState == 0, Data: b,
	})}
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
