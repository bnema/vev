package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// backSessionForAttachment delegates previous-route ownership to the client
// once it has published a complete route snapshot. A live route on the current
// daemon uses the authenticated same-peer path; all other routes return to the
// client for close-and-dial navigation.
func (d *Daemon) backSessionForAttachment(effect *attachmentEffect) error {
	if d == nil || !effect.current() || effect.sess == nil || effect.ac == nil {
		return nil
	}
	snapshot := effect.ac.routeSnapshotCopy()
	if snapshot.Generation == 0 {
		if d.log != nil {
			d.log.Debug("back-session skipped: attachment has no published route snapshot")
		}
		return nil
	}
	if snapshot.Previous.Key == 0 || snapshot.Previous.Generation == 0 {
		return nil
	}
	if target, ok := effect.ac.routeAttentionTarget(snapshot.Previous); ok {
		targetSession, _, live := d.samePeerTarget(protocol.SamePeerSwitchRequest{Target: target})
		if live && targetSession != effect.sess {
			return offerSamePeerAttachTarget(effect, target)
		}
	}
	return d.sendRecentRouteNavigationActionForAttachment(effect, protocol.RouteNavigationAction{
		SnapshotGeneration: snapshot.Generation,
		Key:                snapshot.Previous.Key,
		Generation:         snapshot.Previous.Generation,
	})
}

func (d *Daemon) backSession(current *session, ac *attachedClient) {
	if d == nil || current == nil || ac == nil {
		return
	}
	token := captureAttachmentCapability(current, ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	if !admitted {
		d.reportError(current, domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", errAttachmentTransition))
		return
	}
	defer effect.End()
	if err := d.backSessionForAttachment(effect); err != nil {
		d.reportError(current, err)
	}
}
