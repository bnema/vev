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
	effect      *attachmentEffect
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
	// Attached resize publication retains the exact admitted effect; lifecycle
	// identity cannot change until that effect ends.
	if !p.effect.current() || p.effect.sess != p.session ||
		p.effect.ac != p.attachment || p.effect.lease != p.lease || p.coordinator == nil {
		return false
	}
	return p.coordinator.resizeCurrentForLease(p.epoch, p.attachment, p.lease, false)
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
func (d *Daemon) publishResizeCommit(members []resizeMember, sess *session, ac *attachedClient, lease *attachmentLease, epoch uint64, ticket *attachmentEffect, size domain.Size) bool {
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
		publication.effect = ticket
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
		ac.setSize(size)
	}
	fences.Release()
	if ac != nil {
		ac.sendMu.Unlock()
	}
	return publication.emit()
}
