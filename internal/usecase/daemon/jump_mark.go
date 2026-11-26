package daemon

import (
	"sort"
	"time"

	"bnema.dev/vev/internal/domain"
	"bnema.dev/vev/internal/usecase/picker"
)

type markedTabTarget struct {
	session  *session
	tab      *tab
	target   picker.Target
	markedAt time.Time
}

// markedTabTargets captures local membership under daemon -> session locks.
// Ordering is independent of attention, activity, and session MRU state.
func (d *Daemon) markedTabTargets() []markedTabTarget {
	d.mu.Lock()
	defer d.mu.Unlock()
	var targets []markedTabTarget
	for _, sess := range d.sessions {
		sess.mu.Lock()
		for i, tb := range sess.tabs {
			if tb.marked {
				targets = append(targets, markedTabTarget{session: sess, tab: tb, markedAt: tb.markedAt, target: picker.Target{Session: sess.id, Incarnation: sess.incarnation, Name: sess.name, TabID: domain.TabStableID(tb.stableID), TabIndex: i}})
			}
		}
		sess.mu.Unlock()
	}
	sort.Slice(targets, func(i, j int) bool {
		a, b := targets[i], targets[j]
		if !a.markedAt.Equal(b.markedAt) {
			return a.markedAt.Before(b.markedAt)
		}
		if a.target.Name != b.target.Name {
			return a.target.Name < b.target.Name
		}
		return a.target.TabIndex < b.target.TabIndex
	})
	return targets
}

func (d *Daemon) jumpMark(sess *session, ac *attachedClient, effect *attachmentEffect) error {
	if sess == nil || ac == nil {
		return nil
	}
	targets := d.markedTabTargets()
	if len(targets) == 0 {
		d.publishAttachmentInfo(sess, ac, "No marked tabs")
		d.repaintForNotice(ac)
		return nil
	}
	current := sess.tabForAttachment(ac)
	next := 0
	for i, target := range targets {
		if target.session == sess && target.tab == current {
			if len(targets) == 1 {
				return nil
			}
			next = (i + 1) % len(targets)
			break
		}
	}
	target := targets[next]
	if target.session == sess {
		// Selection admits its own mutation and resolves the stable tab ID.
		if !sess.selectAttachmentTab(ac, target.target.TabID) {
			return nil
		}
		var lease *attachmentLease
		if effect != nil {
			lease = effect.lease
		}
		d.activateTabAfterResizeForLease(sess, sess.tabForAttachment(ac), false, ac, lease)
		d.invalidateRender(sess, ac, true, "jump-mark")
		return nil
	}
	if effect != nil {
		return d.switchActiveTargetForAttachmentGuarded(effect, target.target, sessionHandoffGuard{}, "jump-mark")
	}
	return d.switchToTargetGuardedForAttachment(sess, ac, target.target, sessionHandoffGuard{}, nil, "jump-mark")
}
