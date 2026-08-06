// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"strconv"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func frameWelcome(s *session, ac *attachedClient) ports.Frame {
	return frameWelcomeForAttachment(s, ac)
}

// frameWelcomeForAttachment emits the local-client handshake identity for one
// attachment owner. A remote view is a local daemon object rather than a
// synthetic session, so its opaque local view ID is used only for this wire
// identity while its remote session name remains presentation data.
func frameWelcomeForAttachment(owner attachmentOwner, ac *attachedClient) ports.Frame {
	if ac == nil {
		return ports.Frame{}
	}
	var w ports.Welcome
	switch owner := normalizeAttachmentOwner(owner).(type) {
	case *session:
		owner.mu.Lock()
		w = ports.Welcome{
			SessionID:    string(owner.id),
			SessionName:  owner.name,
			Ephemeral:    owner.ephemeral,
			RenderMode:   ac.renderMode,
			Capabilities: ports.CapabilityResume,
		}
		owner.mu.Unlock()
	case *remoteView:
		w = ports.Welcome{
			SessionID:    "remote-view-" + strconv.FormatUint(uint64(owner.id), 10),
			SessionName:  owner.key.sessionName,
			RenderMode:   ac.renderMode,
			Capabilities: ports.CapabilityResume,
		}
	default:
		return ports.Frame{}
	}
	w.ResumeToken = ac.resumeToken
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(w)}
}

// frameSessionMeta snapshots the authoritative remote session identity and
// ordered stable tab metadata for a proxied attachment. Its first publication
// is emitted during the handshake before that attachment may render output.
func frameSessionMeta(s *session, ac *attachedClient, revision uint64) (ports.Frame, error) {
	if s == nil || ac == nil {
		return ports.Frame{}, ports.ErrInvalidSessionMeta
	}
	// Capture the session tab set and this attachment's active stable tab under
	// the same session snapshot. A stale attachment view must fall back to the
	// first current tab instead of emitting metadata whose active ID is absent
	// from Tabs and therefore invalid on the wire.
	s.mu.Lock()
	active := ac.viewSnapshot().tabID
	meta := ports.SessionMeta{
		LifecycleID: s.incarnation,
		Revision:    revision,
		SessionName: s.name,
		Tabs:        make([]ports.SessionTabMeta, 0, len(s.tabs)),
	}
	activePresent := false
	for i, tab := range s.tabs {
		id := domain.TabStableID(tab.stableID)
		meta.Tabs = append(meta.Tabs, ports.SessionTabMeta{ID: id, Name: tabDisplayName(tab, i), Attention: tab.attention})
		activePresent = activePresent || active == id
	}
	if !activePresent && len(meta.Tabs) != 0 {
		active = meta.Tabs[0].ID
	}
	meta.ActiveTabID = active
	s.mu.Unlock()
	payload, err := ports.MarshalSessionMeta(meta)
	if err != nil {
		return ports.Frame{}, err
	}
	return ports.Frame{Type: ports.MsgSessionMeta, Payload: payload}, nil
}

func frameError(code uint16, text string) ports.Frame {
	return ports.Frame{Type: ports.MsgError, Payload: ports.MarshalErrorMsg(ports.ErrorMsg{Code: code, Text: text})}
}

func frameOutputState(b []byte, baseState uint64, state uint64, echoAck uint64) (ports.Frame, error) {
	payload, err := ports.MarshalOutput(ports.Output{
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
