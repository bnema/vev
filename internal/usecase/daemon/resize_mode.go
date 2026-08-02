package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
)

const (
	resizeStepCols = 2
	resizeStepRows = 1
)

func (d *Daemon) resizePane(target daemonActionTarget, axis layout.Axis, delta int) error {
	return d.mutateTargetLayout(target, true, func(candidate *layout.Tree, area domain.Rect) error {
		originalFocus := candidate.Focus
		candidate.Focus = target.pane.id
		if err := candidate.ResizeFocus(axis, delta, area); err != nil {
			return err
		}
		candidate.Focus = originalFocus
		return nil
	})
}

func (d *Daemon) equalizePanes(target daemonActionTarget) error {
	return d.mutateTargetLayout(target, false, func(candidate *layout.Tree, area domain.Rect) error {
		return candidate.Equalize(area)
	})
}

func resizeUserError(err error) error {
	switch {
	case errors.Is(err, layout.ErrNotInSplit):
		return domain.UserWarn(domain.NoticeLayoutTooSmall, "pane is not in a split", err)
	case errors.Is(err, layout.ErrTooSmall):
		return domain.UserWarn(domain.NoticeLayoutTooSmall, "pane cannot be resized further", err)
	default:
		return err
	}
}

func (d *Daemon) enterResizeMode(sess *session, ac *attachedClient) error {
	if sess == nil || ac == nil {
		return layout.ErrNotFound
	}
	target := resolveDaemonActionTargetForAttachment(sess, ac)
	if target.tab == nil || target.pane == nil {
		return layout.ErrNotFound
	}
	target.tab.mu.Lock()
	canResize := target.tab.tree != nil && target.tab.tree.CanResize()
	target.tab.mu.Unlock()
	if !canResize {
		return resizeUserError(layout.ErrNotInSplit)
	}
	d.exitCopyMode(ac)
	ac.initOverlays()
	ac.overlays.resizeMu.Lock()
	ac.overlays.resizeESC.stop()
	ac.overlays.resizeActive = true
	ac.overlays.resizePending = nil
	ac.overlays.resizeMu.Unlock()
	d.invalidateRender(sess, ac, true, "resize_mode.go")
	return nil
}

func (d *Daemon) handleResizeInput(ac *attachedClient, data []byte) {
	if ac == nil || ac.overlays == nil {
		return
	}
	rt := ac.overlays
	var requests []daemonActionRequest
	exit := false
	rt.resizeMu.Lock()
	if !rt.resizeActive {
		rt.resizePending = nil
		rt.resizeESC.stop()
		rt.resizeMu.Unlock()
		return
	}
	hadPending := len(rt.resizePending) != 0
	if rt.resizeESC.timer != nil {
		rt.resizeESC.stop()
	}
	retainEscape := !hadPending && len(data) != 0 && data[len(data)-1] == keys.ESC
	if retainEscape {
		data = data[:len(data)-1]
	}
	appendResize := func(axis layout.Axis, delta int) {
		requests = append(requests, daemonActionRequest{kind: daemonActionResizePane, axis: axis, delta: delta})
	}
	routeOverlayBytes(data, &rt.resizePending, overlayEvents{
		rune: func(r rune) {
			switch r {
			case 'h':
				appendResize(layout.Width, -resizeStepCols)
			case 'l':
				appendResize(layout.Width, resizeStepCols)
			case 'k':
				appendResize(layout.Height, -resizeStepRows)
			case 'j':
				appendResize(layout.Height, resizeStepRows)
			case '=':
				requests = append(requests, daemonActionRequest{kind: daemonActionEqualizePanes})
			case 'q':
				exit = true
			}
		},
		left:   func() { appendResize(layout.Width, -resizeStepCols) },
		right:  func() { appendResize(layout.Width, resizeStepCols) },
		up:     func() { appendResize(layout.Height, -resizeStepRows) },
		down:   func() { appendResize(layout.Height, resizeStepRows) },
		cancel: func() { exit = true },
		enter:  func() { exit = true },
	})
	if exit {
		rt.resizeActive = false
		rt.resizePending = nil
		rt.resizeESC.stop()
	} else if retainEscape {
		d.retainResizeESCLocked(ac)
	}
	rt.resizeMu.Unlock()

	sess := ac.currentSession()
	if sess == nil {
		return
	}
	for _, request := range requests {
		request.target = resolveDaemonActionTargetForAttachment(sess, ac)
		if err := (daemonActions{d: d}).Run(request); err != nil {
			d.reportError(sess, resizeUserError(err))
			continue
		}
		finishDaemonActionForClient(d, request, ac, "resize_mode.go")
	}
	if exit {
		d.invalidateRender(sess, ac, true, "resize_mode.go")
	}
}

func (d *Daemon) retainResizeESCLocked(ac *attachedClient) {
	rt := ac.overlays
	rt.resizePending = append(rt.resizePending[:0], keys.ESC)
	rt.resizeESC.retain(d.clock, keys.ESCDelay, func(timer ports.Timer) {
		rt.resizeMu.Lock()
		if rt.resizeESC.timer != timer {
			rt.resizeMu.Unlock()
			return
		}
		rt.resizePending = nil
		rt.resizeESC.timer = nil
		rt.resizeESC.done = nil
		active := rt.resizeActive
		rt.resizeActive = false
		rt.resizeMu.Unlock()
		if active {
			if sess := ac.currentSession(); sess != nil {
				d.invalidateRender(sess, ac, true, "resize_mode.go")
			}
		}
	})
}
