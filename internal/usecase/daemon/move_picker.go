// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
)

// moveSourceLocator captures the exact source of one move at command time. The
// presenting client never supplies it: only the daemon resolves it, and only
// for the interaction that captured it.
type moveSessionLocator struct {
	ID          domain.SessionID
	Incarnation domain.IncarnationID
	Name        string
}

type moveSourceLocator struct {
	Session              moveSessionLocator
	TabID                domain.TabStableID
	PaneID               domain.PaneStableID
	Attachment           *attachedClient
	AttachmentCapability attachmentCapability
}

func (d *Daemon) movePickerSourceError(source moveSourceLocator) error {
	if d == nil || source.Session.ID == "" || source.TabID == "" {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Source session is no longer available.", errMovePaneInvalid)
	}
	sess := d.sessionByID(source.Session.ID)
	if sess == nil || sess.incarnation != source.Session.Incarnation {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Source session is no longer available.", errMovePaneInvalid)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if source.AttachmentCapability.ac != nil && !source.AttachmentCapability.currentInSessionLocked(sess) {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Source attachment is no longer active.", errMovePaneInvalid)
	}
	if source.Attachment != nil && !attachmentRegisteredLocked(sess, source.Attachment) {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Source attachment is no longer active.", errMovePaneInvalid)
	}
	tb := findMoveTabLocked(sess, source.TabID)
	if tb == nil {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Tab no longer exists.", errMovePaneInvalid)
	}
	if source.PaneID == "" {
		return nil
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if paneByStableIDLocked(tb, string(source.PaneID)) == nil {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Pane no longer exists.", errMovePaneInvalid)
	}
	return nil
}

// commitMovePickerSelection performs the move the owning source authorised.
func (d *Daemon) commitMovePickerSelection(intent protocol.PickerIntent, source moveSourceLocator, target picker.Target, beforeFollow ...func() error) error {
	var prepare func() error
	if len(beforeFollow) != 0 {
		prepare = beforeFollow[0]
	}
	destination := moveSessionLocator{ID: target.Session, Incarnation: target.Incarnation, Name: target.Name}
	switch intent {
	case protocol.PickerIntentMovePane:
		if target.TabID == "" {
			return errMovePaneInvalid
		}
		return d.movePane(movePaneRequest{
			Follow:               true,
			BeforeFollow:         prepare,
			Attachment:           source.Attachment,
			AttachmentCapability: source.AttachmentCapability,
			Source:               source.Session,
			SourceTabID:          source.TabID,
			SourcePaneID:         source.PaneID,
			Destination:          destination,
			DestinationTabID:     target.TabID,
		})
	case protocol.PickerIntentMoveTab:
		return d.moveTab(moveTabRequest{
			Follow:               true,
			BeforeFollow:         prepare,
			Attachment:           source.Attachment,
			AttachmentCapability: source.AttachmentCapability,
			Source:               source.Session,
			SourceTabID:          source.TabID,
			Destination:          destination,
		})
	default:
		return errMovePaneInvalid
	}
}

func (d *Daemon) previewTarget(target picker.Target, intent protocol.PickerIntent) (*session, *tab) {
	d.mu.Lock()
	sess := d.sessions[target.Session]
	if sess == nil {
		d.mu.Unlock()
		return nil, nil
	}
	sess.mu.Lock()
	d.mu.Unlock()
	defer sess.mu.Unlock()
	if sess.incarnation != target.Incarnation || !targetMatchesLifecycle(target, sess.name, sess.createdAt, sess.incarnation) {
		return nil, nil
	}
	if intent == protocol.PickerIntentMoveTab {
		if len(sess.tabs) == 0 {
			return nil, nil
		}
		return sess, sess.tabs[0]
	}
	if target.TabID != "" {
		for _, tb := range sess.tabs {
			if domain.TabStableID(tb.stableID) == target.TabID {
				return sess, tb
			}
		}
		return nil, nil
	}
	if target.TabIndex < 0 || target.TabIndex >= len(sess.tabs) {
		return nil, nil
	}
	return sess, sess.tabs[target.TabIndex]
}
