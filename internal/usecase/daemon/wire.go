// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-tab PTY reader
// and VT screen, and the per-client debounced render scheduler.
//
// Concurrency model (sessions own one or more PTY-backed tabs):
//
//   - Serve runs the accept loop. Each accepted connection is handled by its
//     own goroutine (handleConn): it reads the first frame and routes it to a
//     session create/attach, a list, or a kill.
//   - Per session there are exactly two long-lived goroutines: the PTY reader
//     (drains child output into the VT screen and pokes a cap-1 dirty channel)
//     and the render scheduler (debounces dirties and paints the attached
//     client). Both are tied to the session context and unwind when the
//     session is killed (pty.Close unblocks the reader; ctx cancel stops the
//     scheduler).
//   - The daemon exits (Serve returns) when the last session is removed, or
//     when the parent context is cancelled (graceful shutdown notifies any
//     attached clients with ReasonServerShutdown).
//
// Locking: a pane's screen/scrollback and per-client renderer shadow are
// guarded by pane.mu/tab.mu as appropriate; the attached-client pointer by
// session.mu; the registry by Daemon.mu. When more than one is held the order
// is always attachedClient.sendMu > Daemon.mu > session.mu > tab.mu > pane.mu.
// The PTY reader only ever takes pane.mu, so it never blocks on a slow client.
package daemon

import (
	"github.com/bnema/vev/internal/ports"
)

func frameWelcome(s *session) ports.Frame {
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{
		SessionID:   string(s.id),
		SessionName: s.name,
		Ephemeral:   s.ephemeral,
	})}
}

func frameError(code uint16, text string) ports.Frame {
	return ports.Frame{Type: ports.MsgError, Payload: ports.MarshalErrorMsg(ports.ErrorMsg{Code: code, Text: text})}
}

func frameOutput(b []byte) ports.Frame {
	return ports.Frame{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{Data: b})}
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
