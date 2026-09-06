// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func serverWelcome(s *session, resumeToken uint64) (protocol.Welcome, error) {
	s.mu.Lock()
	var identity *protocol.CommittedRouteIdentity
	if s.incarnation != (domain.SessionLifecycleID{}) {
		identity = &protocol.CommittedRouteIdentity{
			Target:    protocol.ExactSessionTarget{LifecycleID: s.incarnation, SessionName: s.name},
			Ephemeral: s.ephemeral,
		}
	}
	message := protocol.Welcome{
		SessionID:         string(s.id),
		SessionName:       s.name,
		Ephemeral:         s.ephemeral,
		Capabilities:      protocol.CapabilityResume,
		CommittedIdentity: identity,
	}
	s.mu.Unlock()
	message.ResumeToken = resumeToken
	if identity != nil {
		if err := identity.Validate(); err != nil {
			return protocol.Welcome{}, err
		}
	}
	return message, nil
}

func serverError(code uint16, text string) protocol.ErrorMsg {
	return protocol.ErrorMsg{Code: code, Text: text}
}

func serverDetached(reason uint8) protocol.Detached { return protocol.Detached{Reason: reason} }
func serverPong() protocol.Pong                     { return protocol.Pong{} }
func serverSessions(infos []protocol.SessionInfo) protocol.Sessions {
	return protocol.Sessions{Sessions: infos}
}
