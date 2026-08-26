package daemon

import (
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/domain"
)

// sessionCore is the state shared by every attachment-capable session. It is
// embedded as the first field of session so the existing s.mu selector remains
// the one lock guarding both attachment membership and local tab state.
// Geometry claim paths use the partial order mu -> geometryMu -> sizeMu; no
// path acquires an architectural lock while holding sizeMu.
type sessionCore struct {
	mu sync.Mutex

	id        domain.SessionID
	name      string
	ephemeral bool
	caps      sessionCapabilities
	// attachments is the session-owned membership registry. Connection
	// generation and effect state remain attachment-local; admission and view
	// code use this collection as their membership source.
	attachments      map[*attachedClient]struct{}
	attachmentOrder  map[*attachedClient]uint64
	nextAttachmentID uint64
	// geometryOwner is the attachment whose latest attach/resume/resize claim
	// controls shared PTY/layout geometry. The pointer is published atomically
	// so resize transactions can reject stale claims without taking session.mu.
	// geometryMu serializes claim publication with the final shared-layout commit.
	geometryMu      sync.Mutex
	geometryOwner   atomic.Pointer[attachedClient]
	appliedGeometry domain.Geometry // guarded by geometryMu
	// geometryClaimSeq is mutated while mu and geometryMu are held; its value
	// is copied into attachedClient.geometryClaim.
	geometryClaimSeq uint64
	createdAt        int64
	incarnation      domain.IncarnationID
	mruAt            atomic.Uint64
	coordinator      atomic.Pointer[renderCoordinator]
}

func (s *session) core() *sessionCore {
	if s == nil {
		return nil
	}
	return &s.sessionCore
}

func (s *session) captureRenderState(ac *attachedClient, req renderCaptureRequest) (*capturedRenderState, bool) {
	return captureLocalRenderState(s, ac, req)
}

// validTargetTabLocked validates a target tab index without changing any
// attachment view. Client-facing navigation updates attachment views through
// selectAttachmentTab.
func (s *session) validTargetTabLocked(tabIndex int) bool {
	if tabIndex < 0 {
		return true
	}
	return tabIndex < len(s.tabs)
}

func sessionsSnapshot(entries map[domain.SessionID]*session) []*session {
	sessions := make([]*session, 0, len(entries))
	for _, sess := range entries {
		if sess != nil {
			sessions = append(sessions, sess)
		}
	}
	return sessions
}
