package client

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

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

// inPlaceSwitch is one supervisor-committed session on the daemon already
// serving the attachment. The worker asks that daemon to move the live
// attachment there instead of reconnecting. seq identifies this choice, so a
// result for an older one is never mistaken for it.
type inPlaceSwitch struct {
	seq    uint64
	target protocol.ExactSessionTarget
	tab    domain.TabStableID
}

// inPlaceOutcome is how one in-place switch ended.
type inPlaceOutcome uint8

const (
	// inPlaceArrived: the daemon moved the attachment to the target.
	inPlaceArrived inPlaceOutcome = iota + 1
	// inPlaceRefused: the daemon kept the source; reconnect to the target.
	inPlaceRefused
	// inPlaceSuperseded: a newer navigation replaced this one; drop it.
	inPlaceSuperseded
)

// inPlaceResult reports the outcome of the in-place switch seq.
type inPlaceResult struct {
	seq     uint64
	outcome inPlaceOutcome
}

// requestInPlace hands one in-place switch to the current foreground's worker.
// The newest request replaces an unsent one. It reports false when token no
// longer owns the foreground.
func (h *attachmentHost) requestInPlace(token AttachmentToken, request inPlaceSwitch) bool {
	fg := h.currentForeground(token)
	if fg == nil {
		return false
	}
	for {
		select {
		case fg.inPlace <- request:
			return true
		default:
		}
		select {
		case <-fg.inPlace:
		default:
		}
	}
}

// InPlaceResults carries the outcome of each in-place switch to the
// supervisor.
func (h *attachmentHost) InPlaceResults() <-chan inPlaceResult {
	if h == nil {
		return nil
	}
	return h.inPlaceResults
}

func (f *attachmentForeground) inPlaceSwitches() <-chan inPlaceSwitch {
	if f == nil {
		return nil
	}
	return f.inPlace
}

// reportInPlace hands the outcome of one in-place switch to the supervisor.
// The newest outcome replaces one the supervisor has not taken yet; it is
// always the latest choice, the only one the supervisor still waits for.
func (f *attachmentForeground) reportInPlace(seq uint64, outcome inPlaceOutcome) {
	if f == nil || f.host == nil || seq == 0 {
		return
	}
	result := inPlaceResult{seq: seq, outcome: outcome}
	for {
		select {
		case f.host.inPlaceResults <- result:
			return
		default:
		}
		select {
		case <-f.host.inPlaceResults:
		default:
		}
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
