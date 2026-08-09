package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// backSessionForAttachment delegates previous-route ownership to the client
// once it has published a complete route snapshot. There is no daemon-local
// previous-session fallback after the global-history cutover.
func (d *Daemon) backSessionForAttachment(token attachmentConnectionToken) error {
	if d == nil || token.sess == nil || token.ac == nil {
		return nil
	}
	snapshot := token.ac.routeSnapshotCopy()
	if snapshot.Generation == 0 {
		if d.log != nil {
			d.log.Debug("back-session skipped: attachment has no published route snapshot")
		}
		return nil
	}
	if snapshot.Previous.Key == 0 || snapshot.Previous.Generation == 0 {
		return nil
	}
	return d.sendRecentRouteNavigationActionForAttachment(token, ports.RouteNavigationAction{
		SnapshotGeneration: snapshot.Generation,
		Key:                snapshot.Previous.Key,
		Generation:         snapshot.Previous.Generation,
	})
}

func (d *Daemon) backSession(current *session, ac *attachedClient) {
	if d == nil || current == nil || ac == nil {
		return
	}
	token := attachmentToken(current, ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	if !admitted {
		d.reportError(current, domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", errAttachmentTransition))
		return
	}
	token.effect = effect
	defer effect.End()
	if err := d.backSessionForAttachment(token); err != nil {
		d.reportError(current, err)
	}
}
