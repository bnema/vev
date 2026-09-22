package daemon

import (
	"context"
	"errors"
	"slices"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// remotePreviewRevalidate bounds how long a quiet watch trusts its last frame
// without a render wake: it re-captures to notice a moved tab or a dead target.
const remotePreviewRevalidate = 2 * time.Second

// remotePreviewWatch is one live preview stream. Its address keys the render
// coordinator subscription; the coordinator only ever signals wake.
type remotePreviewWatch struct {
	wake chan struct{}
	rc   *renderCoordinator
}

// notify is the coordinator callback: a non-blocking, lock-free signal. The
// watch goroutine owns every capture and send.
func (w *remotePreviewWatch) notify(renderWake) {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// subscribe follows the target session's coordinator. A headless target gets
// its own coordinator, which keeps the watched tab renderable while subscribed.
func (w *remotePreviewWatch) subscribe(d *Daemon, target *session) {
	var rc *renderCoordinator
	if target != nil {
		rc = d.ensureRenderCoordinator(target)
	}
	if rc == w.rc {
		return
	}
	w.unsubscribe()
	if rc != nil && rc.subscribePreviewFor(w, 1, w.notify) {
		w.rc = rc
	}
}

func (w *remotePreviewWatch) unsubscribe() {
	if w.rc != nil {
		w.rc.teardownPreviewFor(w, 1)
		w.rc = nil
	}
}

// serveRemotePreviewWatch streams the watched viewport until the client
// closes the connection, the daemon shuts down, or the target dies. Frames are
// captured on this goroutine, sent only when they changed, and spaced by at
// least the requested interval; a dead target ends the stream with one
// NoSuchTarget frame.
func (d *Daemon) serveRemotePreviewWatch(tr ports.ServerConnection, watch protocol.RemotePreviewWatch) {
	if protocol.ValidateRemotePreviewWatch(watch) != nil {
		_ = d.sendRemotePreview(tr, protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewMalformed})
		_ = tr.Close()
		return
	}
	base := d.hardCtx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancel(base)
	defer cancel()
	// The client cancels by closing or by sending anything at all.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		_, _ = tr.ReceiveClient()
		cancel()
	}()
	stopClose := context.AfterFunc(ctx, func() { _ = tr.Close() })
	defer func() {
		stopClose()
		_ = tr.Close()
		<-readerDone
	}()

	w := &remotePreviewWatch{wake: make(chan struct{}, 1)}
	defer w.unsubscribe()
	interval := min(max(watch.MinInterval, protocol.RemotePreviewWatchMinInterval), protocol.RemotePreviewWatchMaxInterval)
	var last protocol.RemotePreview
	for {
		// Subscribe before capturing, so output racing the capture still wakes
		// the next one.
		w.subscribe(d, d.remotePreviewSession(watch.Request.Target.SessionName, watch.Request.Target.LifecycleID))
		preview, err := d.captureRemotePreview(watch.Request)
		if errors.Is(err, errRemotePreviewNoSuchTarget) {
			_ = d.sendRemotePreview(tr, protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewNoSuchTarget})
			return
		}
		if err != nil {
			preview = protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewUnavailable}
		}
		if !sameRemotePreview(last, preview) {
			if d.sendRemotePreview(tr, preview) != nil {
				return
			}
			last = preview
		}
		if !d.waitRemotePreview(ctx, interval, nil) || !d.waitRemotePreview(ctx, remotePreviewRevalidate, w.wake) {
			return
		}
	}
}

// waitRemotePreview blocks for d or until wake fires. It reports false once
// the watch is cancelled.
func (d *Daemon) waitRemotePreview(ctx context.Context, delay time.Duration, wake <-chan struct{}) bool {
	timer := d.clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wake:
	case <-timer.C():
	}
	return ctx.Err() == nil
}

func (d *Daemon) sendRemotePreview(tr ports.ServerConnection, preview protocol.RemotePreview) error {
	if protocol.ValidateRemotePreview(preview) != nil {
		preview = protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewMalformed}
	}
	return d.boundedControlSend(tr, preview)
}

func sameRemotePreview(a, b protocol.RemotePreview) bool {
	return a.Version == b.Version && a.Status == b.Status && a.LifecycleID == b.LifecycleID && a.TabID == b.TabID &&
		a.Revision == b.Revision && a.Width == b.Width && a.Height == b.Height &&
		slices.EqualFunc(a.Cells, b.Cells, renderer.Cell.Equal)
}
