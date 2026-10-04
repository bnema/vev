package client

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/colorprofile"
	"github.com/bnema/vev/internal/usecase/ui"
)

// End-to-end attachment liveness.
//
// A remote link can die silently (laptop suspend, Wi-Fi change) long before the
// transport reports an error, leaving the last frame frozen. The attached pump
// therefore probes the serving daemon with the existing Ping/Pong pair: any
// server message proves the link, so a probe is only sent after the stream has
// been quiet for linkProbeIdle. When neither the probe nor anything else is
// answered within linkProbeSuspectAfter the link is suspect and a client-drawn
// notice is painted over the frozen frame. Probing continues at a fixed
// cadence. The first server message afterwards clears the suspicion and, when a
// notice was painted, asks the daemon for a full repaint, which erases it.
//
// This never decides a link is dead: when the stream really fails, the usual
// settle and resume path takes over.

const (
	// linkProbeIdle is how long the stream may stay quiet before a probe.
	linkProbeIdle = 3 * time.Second
	// linkProbeSuspectAfter is how long a probe may stay unanswered before the
	// link is declared suspect.
	linkProbeSuspectAfter = 3 * time.Second
	// linkProbeReping is the probe cadence while the link is suspect.
	linkProbeReping = 3 * time.Second
	// linkProbeJumpSlack is how far the wall clock may run ahead of an armed
	// timer before the gap is attributed to a suspend rather than scheduling.
	linkProbeJumpSlack = 2 * time.Second
	// linkProbeMinDelay keeps a rearm from spinning on a due deadline.
	linkProbeMinDelay = time.Millisecond

	linkSuspectNotice = "Host not responding…"
)

// attachmentLiveness is the probe state of one attached pump. It is owned by
// the pump goroutine and holds no lock. A nil value is an inert probe.
type attachmentLiveness struct {
	worker  *sessionAttachmentWorker
	fg      AttachmentForeground
	stream  ports.BrokerLogicalConnection
	overlay attachmentOverlayForeground
	clock   ports.Clock
	log     *slog.Logger
	size    func() domain.Size

	timer    ports.Timer
	armedAt  time.Time // wall reading (no monotonic part) when the timer was armed
	armedFor time.Duration

	lastRx   time.Time
	lastPing time.Time
	pinged   bool
	suspect  bool
	shown    bool
	since    time.Time
}

// newAttachmentLiveness returns the probe for a remote attachment, or nil for a
// local one: a local daemon link does not die silently.
func (w *sessionAttachmentWorker) newAttachmentLiveness(fg AttachmentForeground, stream ports.BrokerLogicalConnection, overlay attachmentOverlayForeground, size func() domain.Size) *attachmentLiveness {
	if w.cfg.Request.Local || supervisorNil(w.cfg.Clock) {
		return nil
	}
	log := w.cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	l := &attachmentLiveness{
		worker: w, fg: fg, stream: stream, overlay: overlay,
		clock: w.cfg.Clock, log: log, size: size,
	}
	now := l.clock.Now()
	l.lastRx = now
	l.arm(linkProbeIdle)
	return l
}

// c is the probe timer channel, nil when the probe is inert.
func (l *attachmentLiveness) c() <-chan time.Time {
	if l == nil || l.timer == nil {
		return nil
	}
	return l.timer.C()
}

func (l *attachmentLiveness) stop() {
	if l == nil {
		return
	}
	stopSupervisorTimer(l.timer)
	l.timer = nil
}

// arm replaces the timer with one due after d. A fresh timer per arm keeps the
// probe independent of Reset semantics.
func (l *attachmentLiveness) arm(d time.Duration) {
	stopSupervisorTimer(l.timer)
	d = max(d, linkProbeMinDelay)
	l.timer = l.clock.NewTimer(d)
	if supervisorNil(l.timer) {
		l.timer = nil
		return
	}
	l.armedAt = l.clock.Now().Round(0)
	l.armedFor = d
}

// received records one server message. It reports whether a painted notice
// must now be erased by a full repaint; the caller sends that request, unless
// one is already outstanding. A suspicion that never painted anything needs no
// repaint.
func (l *attachmentLiveness) received() (needsRepaint bool) {
	if l == nil {
		return false
	}
	now := l.clock.Now()
	l.lastRx = now
	wasPending := l.pinged
	l.pinged = false
	if !l.suspect {
		if wasPending {
			l.arm(linkProbeIdle)
		}
		return false
	}
	l.log.Info("client_link_recovered", "endpoint", l.worker.cfg.Request.Endpoint, "session", l.worker.cfg.Request.Target.SessionName, "suspect_for", now.Sub(l.since))
	needsRepaint = l.shown
	l.suspect, l.shown = false, false
	l.arm(linkProbeIdle)
	return needsRepaint
}

// overlayReleased runs after an overlay released the terminal and the daemon
// was asked for a repaint. The overlay box is gone and the repaint may never
// arrive on a dead link, so a suspect link redraws its notice right away.
func (l *attachmentLiveness) overlayReleased(state outputApplyState) error {
	if l == nil {
		return nil
	}
	l.shown = false
	if !l.suspect {
		return nil
	}
	return l.paint(state)
}

// fired runs when the probe timer elapses.
func (l *attachmentLiveness) fired(ctx context.Context, state outputApplyState) error {
	if l == nil {
		return nil
	}
	now := l.clock.Now()
	if now.Round(0).Sub(l.armedAt) > l.armedFor+linkProbeJumpSlack {
		// The wall clock outran the timer: the machine was suspended, so the
		// link is probably gone. Probe now and give the probe a fresh window.
		if err := l.ping(ctx, now); err != nil {
			return err
		}
		l.arm(linkProbeSuspectAfter)
		return nil
	}
	switch {
	case !l.pinged:
		if quiet := now.Sub(l.lastRx); quiet < linkProbeIdle {
			l.arm(linkProbeIdle - quiet)
			return nil
		}
		if err := l.ping(ctx, now); err != nil {
			return err
		}
		l.arm(linkProbeSuspectAfter)
	case !l.suspect:
		if waited := now.Sub(l.lastPing); waited < linkProbeSuspectAfter {
			l.arm(linkProbeSuspectAfter - waited)
			return nil
		}
		l.suspect, l.since = true, now
		l.log.Info("client_link_suspect", "endpoint", l.worker.cfg.Request.Endpoint, "session", l.worker.cfg.Request.Target.SessionName, "quiet_for", now.Sub(l.lastRx))
		if err := l.paint(state); err != nil {
			return err
		}
		l.arm(linkProbeReping)
	default:
		waited := now.Sub(l.lastPing)
		if waited >= linkProbeReping {
			if err := l.ping(ctx, now); err != nil {
				return err
			}
			waited = 0
		}
		if !l.shown {
			if err := l.paint(state); err != nil {
				return err
			}
		}
		l.arm(linkProbeReping - waited)
	}
	return nil
}

// ping sends the probe on the stream itself, so it shares the stream's flow
// control; a stalled send is covered by the broker's own heartbeat.
func (l *attachmentLiveness) ping(ctx context.Context, now time.Time) error {
	l.pinged, l.lastPing = true, now
	return l.worker.send(ctx, l.fg, l.stream, protocol.Ping{})
}

// resized repaints a visible notice for the new terminal size.
func (l *attachmentLiveness) resized(state outputApplyState) error {
	if l == nil || !l.suspect || !l.shown {
		return nil
	}
	return l.paint(state)
}

// paint draws the notice through the foreground's output lease, the same path
// the overlays use, so it never interleaves with attachment output. The
// overlay check happens under that lease: nothing is drawn while an overlay
// owns the terminal or without a usable overlay seam, and the next probe tick
// retries.
func (l *attachmentLiveness) paint(state outputApplyState) error {
	l.shown = false
	if l.overlay == nil {
		return nil
	}
	size := l.size()
	bounds := ui.ToastBounds(size, ui.Toast{Message: linkSuspectNotice, Anchor: domain.AnchorTopRight})
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return nil
	}
	var frame bytes.Buffer
	border := toastBorderSGRFor(domain.NoticeWarn, colorprofile.Profile(l.worker.cfg.Color))
	if err := writeClientToast(&frame, bounds, clientToastLines(bounds, linkSuspectNotice, border)); err != nil {
		return err
	}
	uiContext := state.uiContext(ports.UIContext{Generation: attachmentActionableGeneration(l.fg, l.fg.Token())}, ports.UIStatusAttached)
	written, err := l.overlay.overlayOutputIfIdle(uiContext, frame.Bytes())
	if err != nil {
		return fmt.Errorf("vev: publishing link notice: %w", err)
	}
	l.shown = written
	return nil
}
