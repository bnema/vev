package client

import "github.com/bnema/vev/internal/protocol"

// Route publication and daemon navigation seams of the attached foreground.
// The worker owns the stream, so the supervisor reaches the
// daemon only through these bounded channels: the newest route snapshot
// replaces an unsent one, and navigation failures are dropped rather than
// blocking when the worker is gone. In the other direction the worker hands
// every daemon navigation request to the supervisor, which resolves it
// through the broker catalogue exactly like a picker commit.

// attachmentReplyCapacity bounds queued navigation failures per foreground.
const attachmentReplyCapacity = 4

// demandRoutes wakes the supervisor to republish routes for this foreground.
func (f *attachmentForeground) demandRoutes() {
	if f == nil || f.host == nil || !f.authority.actionAuthorized(f) {
		return
	}
	select {
	case f.host.routeDemand <- struct{}{}:
	default:
	}
}

func (f *attachmentForeground) routeSnapshots() <-chan protocol.RecentRouteSnapshot {
	if f == nil {
		return nil
	}
	return f.routes
}

func (f *attachmentForeground) navigationReplies() <-chan protocol.ClientMessage {
	if f == nil {
		return nil
	}
	return f.replies
}

// requestNavigation hands one daemon navigation request to the supervisor.
// The newest request replaces one the supervisor has not taken yet.
func (f *attachmentForeground) requestNavigation(message protocol.ServerMessage) {
	if f == nil || f.host == nil || !f.authority.actionAuthorized(f) {
		return
	}
	for {
		select {
		case f.host.navigations <- message:
			return
		default:
		}
		select {
		case <-f.host.navigations:
		default:
		}
	}
}

// RouteDemands wakes the supervisor when the attached session changed.
func (h *attachmentHost) RouteDemands() <-chan struct{} {
	if h == nil {
		return nil
	}
	return h.routeDemand
}

// Navigations carries the daemon's navigation requests to the supervisor.
func (h *attachmentHost) Navigations() <-chan protocol.ServerMessage {
	if h == nil {
		return nil
	}
	return h.navigations
}

// takeNavigation returns a navigation request left behind by a worker that
// already settled, if any.
func (h *attachmentHost) takeNavigation() (protocol.ServerMessage, bool) {
	if h == nil {
		return nil, false
	}
	select {
	case message := <-h.navigations:
		return message, true
	default:
		return nil, false
	}
}

// publishRoutes hands the newest route snapshot to the current foreground's
// worker. It reports false when token no longer owns the foreground.
func (h *attachmentHost) publishRoutes(token AttachmentToken, snapshot protocol.RecentRouteSnapshot) bool {
	fg := h.currentForeground(token)
	if fg == nil {
		return false
	}
	for {
		select {
		case fg.routes <- snapshot:
			return true
		default:
		}
		select {
		case <-fg.routes:
		default:
		}
	}
}

// replyNavigation queues one navigation failure for the current worker.
func (h *attachmentHost) replyNavigation(token AttachmentToken, message protocol.ClientMessage) bool {
	fg := h.currentForeground(token)
	if fg == nil {
		return false
	}
	select {
	case fg.replies <- message:
		return true
	default:
		return false
	}
}

func (h *attachmentHost) currentForeground(token AttachmentToken) *attachmentForeground {
	if h == nil {
		return nil
	}
	fg := h.authority.foreground()
	if fg == nil || fg.token != token {
		return nil
	}
	return fg
}
