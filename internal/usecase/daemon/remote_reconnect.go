package daemon

import (
	"context"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const remoteViewRetryDelay = 100 * time.Millisecond

func sleepRemoteViewRetry(ctx context.Context, clock ports.Clock) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if clock == nil {
		return true
	}
	timer := clock.NewTimer(remoteViewRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C():
		return true
	case <-ctx.Done():
		return false
	}
}

// reconnectRemoteView preserves the registered view and its local attachments
// while openRemoteView verifies a replacement link. It changes presentation
// state only when the failed exact link and reconnect generation are still
// current, so a later picker open, terminal retirement, or shutdown wins.
func (d *Daemon) reconnectRemoteView(view *remoteView, failed *remoteLink, reconnectGeneration uint64, target domain.RemoteSessionTarget, size domain.Size) {
	if d == nil || view == nil || failed == nil {
		return
	}
	ctx := d.serveCtx
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := d.openRemoteView(ctx, target, size)
	if err == nil {
		return
	}

	view.mu.Lock()
	current := !view.closed &&
		view.link == failed &&
		view.linkGeneration == failed.generation &&
		view.linkState == remoteViewLinkReconnecting &&
		view.reconnectGeneration == reconnectGeneration
	if !current {
		view.mu.Unlock()
		return
	}
	view.linkState = remoteViewLinkUnavailable
	view.reconnectGeneration++
	attachments := make([]*attachedClient, 0, len(view.attachments))
	for attachment := range view.attachments {
		attachments = append(attachments, attachment)
	}
	view.mu.Unlock()
	d.repaintRemoteViewAttachments(view, attachments)
}

// reconnectSizeLocked derives an outer geometry matching the retained remote
// content surface. The caller holds view.mu. Existing content is authoritative
// until a validated replacement publication adopts a newer attachment claim.
func reconnectSizeLocked(view *remoteView) domain.Size {
	if view != nil && view.screen != nil {
		cols, rows := view.screen.Frame.Width, view.screen.Frame.Height
		if cols > 0 && rows > 0 {
			return domain.Size{Cols: cols, Rows: rows + tabChromeRows}
		}
	}
	return defaultSize
}
