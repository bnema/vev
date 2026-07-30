package daemon

import (
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/domain"
)

// attachmentSession is the private attachment-facing session contract. Local
// process, tab, persistence, and teardown behavior remains on *session.
type attachmentSession interface {
	core() *sessionCore
	snapshotView(viewOptions) sessionView
	statusSegments(includeTerminalTitle bool) statusSnapshot
	capturePrimary(*attachedClient, primaryCaptureRequest) (*capturedRenderState, bool)
	activateTargetLocked(tabIndex int) bool
	isProxy() bool
}

// sessionCore is the state shared by every attachment-capable session. It is
// embedded as the first field of session so the existing s.mu selector remains
// the one lock guarding both attachment membership and local tab state.
type sessionCore struct {
	mu sync.Mutex

	id          domain.SessionID
	name        string
	ephemeral   bool
	caps        sessionCapabilities
	client      *attachedClient
	snatched    map[*attachedClient]struct{}
	createdAt   int64
	incarnation domain.IncarnationID
	mruAt       atomic.Uint64
	coordinator atomic.Pointer[renderCoordinator]
}

func (s *session) core() *sessionCore {
	if s == nil {
		return nil
	}
	return &s.sessionCore
}

func (s *session) isProxy() bool { return false }

func (s *session) capturePrimary(ac *attachedClient, req primaryCaptureRequest) (*capturedRenderState, bool) {
	return captureLocalPrimaryRenderState(s, ac, req)
}

// activateTargetLocked validates and selects a local tab. Caller holds s.mu.
func (s *session) activateTargetLocked(tabIndex int) bool {
	if tabIndex < 0 {
		return true
	}
	if tabIndex >= len(s.tabs) {
		return false
	}
	s.active = tabIndex
	return true
}

// localSession narrows an attachment entry for operations that own local PTYs,
// tabs, snapshots, or durable lifecycle state.
func localSession(entry attachmentSession) (*session, bool) {
	sess, ok := entry.(*session)
	return sess, ok && sess != nil
}

func localSessionsSnapshot(entries map[domain.SessionID]attachmentSession) []*session {
	locals := make([]*session, 0, len(entries))
	for _, entry := range entries {
		if sess, ok := localSession(entry); ok {
			locals = append(locals, sess)
		}
	}
	return locals
}
