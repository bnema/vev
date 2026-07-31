package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
)

var errSessionMetaUnavailable = errors.New("session metadata is unavailable")

// sessionMetaSnapshot captures the authoritative tab order while session.mu
// owns every field represented on the wire.
func (s *session) sessionMetaSnapshot() (ports.SessionMeta, bool) {
	if s == nil {
		return ports.SessionMeta{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tabs) == 0 || s.active < 0 || s.active >= len(s.tabs) {
		return ports.SessionMeta{}, false
	}
	meta := ports.SessionMeta{
		SessionName: s.name,
		Active:      uint16(s.active),
		Tabs:        make([]ports.SessionTabMeta, len(s.tabs)),
	}
	for i, tb := range s.tabs {
		meta.Tabs[i] = ports.SessionTabMeta{
			Index:     uint16(i),
			Name:      tb.name,
			Attention: tb.attention,
		}
	}
	return meta, true
}

func frameSessionMeta(meta ports.SessionMeta) (ports.Frame, error) {
	payload, err := ports.MarshalSessionMeta(meta)
	if err != nil {
		return ports.Frame{}, err
	}
	return ports.Frame{Type: ports.MsgSessionMeta, Payload: payload}, nil
}

func sameSessionMeta(a, b ports.SessionMeta) bool {
	if a.SessionName != b.SessionName || a.Active != b.Active || len(a.Tabs) != len(b.Tabs) {
		return false
	}
	for i := range a.Tabs {
		if a.Tabs[i] != b.Tabs[i] {
			return false
		}
	}
	return true
}

func cloneSessionMeta(meta ports.SessionMeta) ports.SessionMeta {
	meta.Tabs = append([]ports.SessionTabMeta(nil), meta.Tabs...)
	return meta
}

// sendSessionMetaIfChanged sends metadata before a proxied output frame. The
// caller holds ac.sendMu, which serializes this snapshot shadow with every
// output write. The shadow is committed only after the transport accepts the
// metadata frame.
func (ac *attachedClient) sendSessionMetaIfChanged(sess *session, expected transportSnapshot, ticket *roleEffectTicket) error {
	if ac == nil || sess == nil || !ac.proxied {
		return nil
	}
	meta, ok := sess.sessionMetaSnapshot()
	if !ok {
		return errSessionMetaUnavailable
	}
	if ac.sessionMetaSent && sameSessionMeta(ac.sessionMeta, meta) {
		return nil
	}
	frame, err := frameSessionMeta(meta)
	if err != nil {
		return err
	}
	if err := ac.beginExpectedTransportSendLocked(expected, ticket); err != nil {
		return err
	}
	send := expected.transport.Send
	if async, ok := expected.transport.(ports.AsyncTransport); ok {
		send = async.SendAsync
	}
	err = send(frame)
	if ticket != nil {
		if err != nil {
			ticket.reportTransportFailure(expected)
		}
		ticket.endTransportSend()
	}
	if err == nil {
		ac.sessionMeta = cloneSessionMeta(meta)
		ac.sessionMetaSent = true
	}
	return err
}

// markSessionMetaSent records a successfully sent snapshot. It acquires
// ac.sendMu itself, so callers must not hold that lock.
func (ac *attachedClient) markSessionMetaSent(meta ports.SessionMeta) {
	if ac == nil {
		return
	}
	ac.sendMu.Lock()
	ac.sessionMeta = cloneSessionMeta(meta)
	ac.sessionMetaSent = true
	ac.sendMu.Unlock()
}
