package daemon

import "context"

const maxAcceptedTabLayoutRetries = 3

// acceptedTabLayoutRetryToken binds a delayed tiled retry to the exact pane
// owner generations which accepted the failure. A moved pane cannot lend its
// old source worker authority over either the PTY or source publications.
type acceptedTabLayoutRetryToken struct {
	session *session
	tab     *tab
	members []resizeMember
}

func newAcceptedTabLayoutRetryToken(sess *session, tb *tab, members []resizeMember) acceptedTabLayoutRetryToken {
	return acceptedTabLayoutRetryToken{session: sess, tab: tb, members: append([]resizeMember(nil), members...)}
}

func (t acceptedTabLayoutRetryToken) current() bool {
	if t.session == nil || t.tab == nil || len(t.members) == 0 {
		return false
	}
	for i := range t.members {
		member := &t.members[i]
		if member.session != t.session || member.tab != t.tab || !resizeMemberOwnerCurrentLocked(member) {
			return false
		}
	}
	return true
}

// scheduleAcceptedTabLayoutRetry owns one bounded retry worker per tab. The
// worker is deduplicated, derives cancellation from the tab lifecycle, and
// suppresses repeat degradation notices after the accepted initial failure.
func (d *Daemon) scheduleAcceptedTabLayoutRetry(sess *session, tb *tab, failed ...[]resizeMember) bool {
	if d.clock == nil || tb == nil || tb.ctx == nil {
		return false
	}
	var members []resizeMember
	if len(failed) != 0 {
		members = failed[0]
	} else {
		tb.mu.Lock()
		plan := prepareTabLayoutLocked(sess, tb)
		tb.mu.Unlock()
		for i := range plan.members {
			member := plan.members[i]
			member.pane.mu.Lock()
			pending := member.pane.resizeRetry
			member.pane.mu.Unlock()
			if pending {
				members = append(members, member)
			}
		}
	}
	token := newAcceptedTabLayoutRetryToken(sess, tb, members)
	var ctx context.Context
	var cancel context.CancelFunc
	start := false
	if !d.publishResizeOwnerPostEffect(members, resizeOwnerPostRetrySchedule, func() {
		tb.layoutRetryMu.Lock()
		defer tb.layoutRetryMu.Unlock()
		if tb.layoutRetryRunning {
			return
		}
		ctx, cancel = context.WithCancel(tb.ctx)
		tb.layoutRetryRunning = true
		tb.layoutRetryCancel = cancel
		start = true
	}) {
		return false
	}
	if !start {
		return true
	}

	go func() {
		defer func() {
			cancel()
			tb.layoutRetryMu.Lock()
			tb.layoutRetryRunning = false
			tb.layoutRetryCancel = nil
			tb.layoutRetryMu.Unlock()
		}()
		for range maxAcceptedTabLayoutRetries {
			if !token.current() {
				return
			}
			timer := d.clock.NewTimer(minOutputRenderDeadline)
			if timer == nil {
				return
			}
			timerC := timer.C()
			if timerC == nil {
				timer.Stop()
				return
			}
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timerC:
			}
			if ctx.Err() != nil || !token.current() {
				return
			}
			result, ok := d.applyTabLayoutTransactionWithNotice(sess, tb, false, func() bool {
				return ctx.Err() == nil && token.current()
			})
			if !ok || !token.current() {
				return
			}
			if len(result.failed) == 0 {
				return
			}
			token = newAcceptedTabLayoutRetryToken(sess, tb, result.failed)
			if !token.current() {
				return
			}
			if !d.publishResizeOwnerPostEffect(result.members, resizeOwnerPostSnapshotDirty, func() {
				markSnapshotDirty(sess)
			}) {
				return
			}
			sess.mu.Lock()
			ac := sess.client
			sess.mu.Unlock()
			if ac != nil && !d.publishResizeOwnerInvalidation(result.members, sess, ac, nil, 0,
				renderInvalidation{class: invalidateUrgent, reset: true, producer: "transactional_resize.go"}) {
				return
			}
		}
	}()
	return true
}
