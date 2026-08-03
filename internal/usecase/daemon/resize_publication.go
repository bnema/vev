package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type resizeCommitPublication struct {
	members     []resizeMember
	session     *session
	attachment  *attachedClient
	lease       *attachmentLease
	epoch       uint64
	connection  attachmentConnectionToken
	coordinator *renderCoordinator
	observer    ports.RuntimeObserver
	mark        ports.RuntimeMark
}

func (p resizeCommitPublication) current() bool {
	if !resizeMembersOwnerCurrent(p.members) {
		return false
	}
	if p.attachment == nil {
		return true
	}
	// Attached resize publication is admitted only through a live attachment effect ticket.
	if p.connection.effect == nil {
		return false
	}
	if p.connection.effect.ended.Load() || p.connection.sess != p.session ||
		p.connection.ac != p.attachment || p.connection.lease != p.lease ||
		p.connection.generation != p.attachment.connectionGeneration.Load() ||
		!p.attachment.transportSnapshotCurrent(p.connection.transport) ||
		p.attachment.currentSession() != p.session || p.coordinator == nil {
		return false
	}
	registered := attachmentRegistered(p.session, p.attachment)
	return registered && p.coordinator.resizeCurrentForLease(p.epoch, p.attachment, p.lease, false)
}

// emit performs observer work only after every resize-owner fence and
// attachment sendMu are released. The generation-bound publication is checked
// once more so a move that wins after geometry publication but before observer
// admission suppresses stale source telemetry.
func (p resizeCommitPublication) emit() bool {
	if !p.current() {
		return false
	}
	if p.observer != nil {
		p.observer.ObserveRuntime(p.mark)
	}
	return true
}

// publishResizeCommit linearizes attachment geometry and an immutable telemetry
// reservation with pane ownership. sendMu is acquired before resize fences, so
// the canonical rule never acquires an attachment lock while a pane fence is
// held. Observer callbacks run only after both lock families are released.
func (d *Daemon) publishResizeCommit(members []resizeMember, sess *session, ac *attachedClient, lease *attachmentLease, epoch uint64, ticket *attachmentEffectTicket, size domain.Size) bool {
	if ac != nil && ticket == nil {
		return false
	}
	d.observeBeforeResizeOwnerPostEffect(resizeOwnerPostCommitPublication)
	if ac != nil {
		ac.sendMu.Lock()
		if d.afterResizeCommitSendLocked != nil {
			d.afterResizeCommitSendLocked()
		}
	}
	fences := acquireResizeOwnerPostEffectFences(members)
	if fences == nil {
		if ac != nil {
			ac.sendMu.Unlock()
		}
		return false
	}
	publication := resizeCommitPublication{
		members:    append([]resizeMember(nil), members...),
		session:    sess,
		attachment: ac,
		lease:      lease,
		epoch:      epoch,
		observer:   d.runtimeObserver,
		mark:       ports.NewRuntimeMark("daemon", ports.RuntimeResizeCommitted, 0, true),
	}
	if ticket != nil {
		publication.connection = ticket.connectionToken()
		publication.coordinator = sess.renderCoordinator()
	}
	if !publication.current() {
		fences.Release()
		if ac != nil {
			ac.sendMu.Unlock()
		}
		return false
	}
	if ac != nil {
		ac.size = size
	}
	fences.Release()
	if ac != nil {
		ac.sendMu.Unlock()
	}
	return publication.emit()
}
