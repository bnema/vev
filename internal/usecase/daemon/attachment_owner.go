package daemon

import (
	"errors"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/vt"
)

// attachmentOwner is the sole mutable owner binding for an attached client.
// It is deliberately sealed to this package: only a local session or a
// daemon-owned remote view may own an attachment. Local-only operations must
// obtain a *session through localSession instead of assuming every owner has
// PTYs, persistence, or a tab tree.
type attachmentOwner interface {
	attachmentOwner()
}

func (*session) attachmentOwner() {}

// remoteViewID is allocated by the local daemon. It is distinct from the
// remote lifecycle identity: a remote lifecycle validates the remote endpoint,
// while this ID identifies the exact local object that owns attachments and
// asynchronous lifecycle work.
type remoteViewID uint64

// remoteViewKey identifies the one reusable local view for an exact remote
// session lifecycle. Display data and tab selection are intentionally absent:
// both are mutable presentation state, not remote-link identity.
type remoteViewKey struct {
	endpoint    string
	lifecycleID domain.SessionLifecycleID
	sessionName string
}

func remoteViewKeyForTarget(target domain.RemoteSessionTarget) (remoteViewKey, error) {
	if err := target.Validate(); err != nil {
		return remoteViewKey{}, err
	}
	return remoteViewKey{
		endpoint:    target.Endpoint,
		lifecycleID: target.LifecycleID,
		sessionName: target.SessionName,
	}, nil
}

// remoteView is intentionally attachment-facing only. Its private VT and
// presentation metadata are the remote content boundary: neither carries a
// local PTY, persistence, tab tree, or render coordinator. The remote-link
// lifecycle owns writes to them in Phase 4; local composition only snapshots
// them while holding this mutex.
type remoteView struct {
	id  remoteViewID
	key remoteViewKey

	mu            sync.Mutex
	closed        bool
	attachments   map[*attachedClient]struct{}
	screen        *vt.Screen
	metadata      ports.SessionMeta
	displayOrigin string
}

func (*remoteView) attachmentOwner() {}

func (v *remoteView) registerAttachmentLocked(ac *attachedClient) bool {
	if v == nil || v.closed || ac == nil {
		return false
	}
	if v.attachments == nil {
		v.attachments = make(map[*attachedClient]struct{})
	}
	if _, exists := v.attachments[ac]; exists {
		return false
	}
	v.attachments[ac] = struct{}{}
	return true
}

func (v *remoteView) unregisterAttachmentLocked(ac *attachedClient) bool {
	if v == nil || ac == nil {
		return false
	}
	if _, exists := v.attachments[ac]; !exists {
		return false
	}
	delete(v.attachments, ac)
	return true
}

func (v *remoteView) attachmentRegistered(ac *attachedClient) bool {
	if v == nil || ac == nil {
		return false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return false
	}
	_, registered := v.attachments[ac]
	return registered
}

// close retires attachment membership without transport I/O and returns the
// exact active attachments for retirement after the caller releases view.mu.
// The daemon first marks shutdown under its registry lock, then uses that
// snapshot to interrupt each blocked local-client transport.
func (v *remoteView) close() []*attachedClient {
	if v == nil {
		return nil
	}
	v.mu.Lock()
	attachments := make([]*attachedClient, 0, len(v.attachments))
	for ac := range v.attachments {
		attachments = append(attachments, ac)
	}
	v.closed = true
	clear(v.attachments)
	v.mu.Unlock()
	return attachments
}

// retireShutdownRemoteAttachment closes one exact local-client transport after
// a remote view is unpublished. It never performs I/O while holding daemon or
// remote-view locks; a future remote link is separately retired in Phase 4.
func (d *Daemon) retireShutdownRemoteAttachment(view *remoteView, ac *attachedClient, reason uint8) {
	if d == nil || view == nil || ac == nil {
		return
	}
	if sameAttachmentOwner(ac.currentAttachmentOwner(), view) {
		ac.connectionGeneration.Add(1)
		ac.setAttachmentOwner(nil)
	}
	ac.clearPreviousSession()
	transport := ac.transport()
	d.boundedSend(ac, frameDetached(reason))
	_ = ac.closeCapturedTransport(ac.revokeTransport(transport))
}

func attachmentOwnerRegistered(owner attachmentOwner, ac *attachedClient) bool {
	switch owner := owner.(type) {
	case *session:
		return attachmentRegistered(owner, ac)
	case *remoteView:
		return owner.attachmentRegistered(ac)
	default:
		return false
	}
}

func normalizeAttachmentOwner(owner attachmentOwner) attachmentOwner {
	switch typed := owner.(type) {
	case *session:
		if typed == nil {
			return nil
		}
	case *remoteView:
		if typed == nil {
			return nil
		}
	}
	return owner
}

func localSession(owner attachmentOwner) *session {
	sess, _ := normalizeAttachmentOwner(owner).(*session)
	return sess
}

func sameAttachmentOwner(a, b attachmentOwner) bool {
	a, b = normalizeAttachmentOwner(a), normalizeAttachmentOwner(b)
	return a != nil && a == b
}

func attachmentOwnerName(owner attachmentOwner) string {
	switch owner := normalizeAttachmentOwner(owner).(type) {
	case *session:
		owner.mu.Lock()
		defer owner.mu.Unlock()
		return owner.name
	case *remoteView:
		return owner.key.sessionName
	default:
		return ""
	}
}

// attachmentOwnerRegisteredByDaemonLocked verifies the owner has not been
// removed or replaced in its concrete registry. Caller holds d.mu.
func (d *Daemon) attachmentOwnerRegisteredByDaemonLocked(owner attachmentOwner) bool {
	switch owner := normalizeAttachmentOwner(owner).(type) {
	case *session:
		return owner.core() != nil && d.sessions[owner.core().id] == owner
	case *remoteView:
		return d.remoteViews[owner.id] == owner && d.remoteViewsByKey[owner.key] == owner.id
	default:
		return false
	}
}

func (d *Daemon) registerRemoteViewLocked(view *remoteView) error {
	if d == nil || view == nil || view.key.endpoint == "" ||
		view.key.lifecycleID == (domain.SessionLifecycleID{}) || view.key.sessionName == "" {
		return errors.New("invalid remote view")
	}
	if d.remoteViews == nil {
		d.remoteViews = make(map[remoteViewID]*remoteView)
	}
	if d.remoteViewsByKey == nil {
		d.remoteViewsByKey = make(map[remoteViewKey]remoteViewID)
	}
	if existingID, exists := d.remoteViewsByKey[view.key]; exists {
		if d.remoteViews[existingID] == view {
			return nil
		}
		return errors.New("remote view key is already registered")
	}
	if view.id == 0 {
		d.nextRemoteViewID++
		view.id = d.nextRemoteViewID
	} else if d.remoteViews[view.id] != nil {
		return errors.New("remote view ID is already registered")
	} else if view.id > d.nextRemoteViewID {
		d.nextRemoteViewID = view.id
	}
	d.remoteViews[view.id] = view
	d.remoteViewsByKey[view.key] = view.id
	return nil
}

func (d *Daemon) remoteViewByKeyLocked(key remoteViewKey) *remoteView {
	if d == nil {
		return nil
	}
	return d.remoteViews[d.remoteViewsByKey[key]]
}

func (d *Daemon) unregisterRemoteViewLocked(view *remoteView) bool {
	if d == nil || view == nil || d.remoteViews[view.id] != view || d.remoteViewsByKey[view.key] != view.id {
		return false
	}
	delete(d.remoteViews, view.id)
	delete(d.remoteViewsByKey, view.key)
	return true
}
