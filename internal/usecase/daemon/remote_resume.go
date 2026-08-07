package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// resumeParkedRemoteView claims and republishes a parked local-client binding
// for one exact remote view. It deliberately owns only the local-client
// connection lifecycle: remote-link transport and reconnect work remain
// separate Phase 4 responsibilities.
//
// handled is false when the resume credential does not identify a remote view,
// allowing the existing local-session resume route to handle it. Once a remote
// parked entry is observed, the function is authoritative for that credential
// and returns a fail-closed error on every validation failure.
func (d *Daemon) resumeParkedRemoteView(h ports.Hello, tr ports.Transport) (view *remoteView, ac *attachedClient, handled bool, err error) {
	if d == nil || h.ResumeToken == 0 || tr == nil || !h.Size.Valid() {
		return nil, nil, false, nil
	}

	// A reconnect can race the old local transport's Recv failure between its
	// pre-detach parking publication and final parked entry. Once that marker
	// identifies a remote view, this path remains authoritative for the token
	// and waits for its exact publication rather than falling into local-session
	// resume logic.
	seenRemoteCredential := false
	var parked *parkedAttachment
	for {
		d.mu.Lock()
		parked = d.parked[h.ResumeToken]
		if parked != nil {
			view, handled = normalizeAttachmentOwner(parked.owner).(*remoteView)
			d.mu.Unlock()
			if !handled {
				return nil, nil, false, nil
			}
			break
		}
		pending := d.parking[h.ResumeToken]
		pendingView, pendingRemote := (*remoteView)(nil), false
		if pending != nil {
			pendingView, pendingRemote = normalizeAttachmentOwner(pending.owner).(*remoteView)
		}
		d.mu.Unlock()
		if !pendingRemote {
			if seenRemoteCredential {
				return nil, nil, true, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
			}
			return nil, nil, false, nil
		}
		if pendingView == nil || pending.ac == nil || pending.ac.clientID != h.ClientID {
			return nil, nil, true, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
		}
		seenRemoteCredential = true
		if !d.waitParkingInFlight(h) {
			return nil, nil, true, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
		}
	}
	ac = parked.ac
	if ac == nil {
		return nil, nil, true, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}

	if d.beforeResumeParkedSendMu != nil {
		d.beforeResumeParkedSendMu()
	}
	ac.sendMu.Lock()
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		ac.sendMu.Unlock()
		return nil, nil, true, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
	}
	if d.parked[h.ResumeToken] != parked || parked.claimed || !d.attachmentOwnerRegisteredByDaemonLocked(view) {
		d.mu.Unlock()
		ac.sendMu.Unlock()
		return nil, nil, true, errResumeTokenLifecycleRace
	}
	if ac.clientID != h.ClientID {
		d.mu.Unlock()
		ac.sendMu.Unlock()
		return nil, nil, true, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}

	// Keep the previous credential claimed until Welcome commits. A failed
	// handshake restores this exact parked owner through abortResumeClaim.
	parked.claimed = true
	ac.resumeClaimToken = h.ResumeToken
	ac.rebaseOutput()
	if ac.output != nil {
		ac.output.maxOutstanding = uint64(normalizeOutputWindow(h.MaxOutputInFlight))
		ac.output.maxOutstandingAtomic.Store(ac.output.maxOutstanding)
	}
	ac.replaceTransport(tr)
	ac.setSize(h.Size)
	ac.resumeToken = d.nextResumeTokenLocked()
	ac.parked = false
	d.mu.Unlock()
	ac.sendMu.Unlock()

	if _, transitionErr := d.resumeRemoteAttachment(view, ac, ac.transportSnapshot()); transitionErr != nil {
		d.abortResumeClaim(ac)
		return nil, nil, true, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}
	// The successful first local composition resizes the private VT from the
	// new attachment window. Deferring it until after Welcome preserves a
	// parked view's content when this handshake fails.
	d.log.Info("client resumed", "session", attachmentOwnerName(view))
	return view, ac, true, nil
}

// resumeRemoteAttachment republishes a parked attachment under the exact
// remote owner. The gate is frozen across membership and owner publication so
// no stale local-client effect can observe the temporary unowned state.
func (d *Daemon) resumeRemoteAttachment(view *remoteView, ac *attachedClient, transport transportSnapshot) (attachmentConnectionToken, error) {
	if d == nil || view == nil || ac == nil || transport.transport == nil {
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	frozen := freezeAttachmentEffectGates(ac)
	defer frozen.unfreeze()
	if !frozen.acquired || !frozen.drained {
		return attachmentConnectionToken{}, errAttachmentTransition
	}

	d.mu.Lock()
	if d.closing || !d.attachmentOwnerRegisteredByDaemonLocked(view) {
		d.mu.Unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	view.mu.Lock()
	_, registered := view.attachments[ac]
	current := !view.closed &&
		ac.currentAttachmentOwner() == nil &&
		ac.transportSnapshotCurrent(transport) &&
		!registered
	if !current || !view.registerAttachmentLocked(ac) {
		view.mu.Unlock()
		d.mu.Unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	generation := ac.connectionGeneration.Add(1)
	ac.setAttachmentOwner(view)
	published := attachmentConnectionToken{
		owner:      view,
		ac:         ac,
		generation: generation,
		transport:  transport,
	}
	ac.publishFrozenAttachmentCapability(published)
	view.mu.Unlock()
	d.mu.Unlock()
	d.activateRemoteView(view)
	return published, nil
}

// resumeLiveRemoteView handles the narrow reconnect race where the old local
// transport still owns a remote attachment when the same resume credential
// arrives. It parks that exact binding first, then reuses the parked resume
// publication so the old and replacement transports cannot both own the view.
func (d *Daemon) resumeLiveRemoteView(h ports.Hello, tr ports.Transport) (view *remoteView, ac *attachedClient, handled bool, err error) {
	if d == nil || h.ResumeToken == 0 || tr == nil || !h.Size.Valid() {
		return nil, nil, false, nil
	}

	var clientMismatch bool
	d.mu.Lock()
	for _, candidateView := range d.remoteViews {
		candidateView.mu.Lock()
		for candidate := range candidateView.attachments {
			if !candidate.resumeCapable || candidate.resumeToken != h.ResumeToken {
				continue
			}
			if candidate.clientID != h.ClientID {
				clientMismatch = true
			} else {
				view, ac = candidateView, candidate
			}
			break
		}
		candidateView.mu.Unlock()
		if view != nil || clientMismatch {
			break
		}
	}
	closing := d.closing
	d.mu.Unlock()
	if view == nil && !clientMismatch {
		return nil, nil, false, nil
	}
	if closing {
		return nil, nil, true, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
	}
	if clientMismatch {
		return nil, nil, true, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
	}

	oldTransport := ac.transport()
	token := attachmentOwnerToken(view, ac, oldTransport)
	if token.ac != nil {
		_ = d.clientGoneRemote(view, token, false)
	}
	view, ac, handled, err = d.resumeParkedRemoteView(h, tr)
	if handled || err != nil {
		return view, ac, handled, err
	}
	return nil, nil, true, &protoErr{ports.ErrNoSuchSession, "resume token is no longer valid"}
}

func remoteViewMatchesTarget(view *remoteView, target domain.RemoteSessionTarget) bool {
	if view == nil || target.Validate() != nil {
		return false
	}
	return view.key.endpoint == target.Endpoint &&
		view.key.lifecycleID == target.LifecycleID &&
		view.key.sessionName == target.SessionName
}
