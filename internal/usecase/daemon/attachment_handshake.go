package daemon

import (
	"context"

	"github.com/bnema/vev/internal/ports"
)

// completeAttachmentHandshake publishes one already-routed attachment. Its
// owner is deliberately generic so a resumed remote view receives the same
// local-client Welcome/output lifecycle without being manufactured as a local
// session.
func (d *Daemon) completeAttachmentHandshake(handshakeCtx context.Context, stopHandshakeTransport, finishHandshake func(), tr ports.Transport, owner attachmentOwner, ac *attachedClient) {
	failAttachment := func() {
		d.failHandshakeAttachmentOwner(owner, ac, tr)
	}
	if ac == nil || normalizeAttachmentOwner(owner) == nil || handshakeCtx.Err() != nil {
		failAttachment()
		return
	}

	expected := ac.transportSnapshot()
	welcomeToken := attachmentOwnerToken(owner, ac, tr)
	welcomeTicket, admitted := ac.beginAttachmentEffect(welcomeToken)
	if expected.transport != tr || !admitted || welcomeToken.ac == nil {
		if admitted {
			welcomeTicket.End()
		}
		if !d.abortResumeClaim(ac) {
			failAttachment()
		}
		return
	}
	if handshakeCtx.Err() != nil {
		welcomeTicket.End()
		failAttachment()
		return
	}
	if err := boundedHandshakeOperation(handshakeCtx, tr, func() error {
		return ac.sendExpectedTransportForAttachment(expected, frameWelcomeForAttachment(owner, ac), welcomeTicket)
	}); err != nil {
		welcomeTicket.End()
		failAttachment()
		return
	}
	// Release Welcome's effect before discovering post-handshake authority so a
	// replacement blocked behind the send can publish its generation and lease.
	welcomeTicket.End()

	postWelcomeToken, postWelcomeTicket, admitted := ac.beginCurrentAttachmentOwnerEffect(owner, tr)
	if !admitted || postWelcomeToken.ac == nil {
		if postWelcomeTicket != nil {
			postWelcomeTicket.End()
		}
		failAttachment()
		return
	}
	sess := localSession(owner)
	var lease *attachmentLease
	var rc *renderCoordinator
	if sess != nil {
		rc = sess.renderCoordinator()
		if rc != nil {
			lease = rc.attachmentLease(ac)
		}
	}
	if sess != nil && postWelcomeToken.ac.renderMode == ports.RenderModeProxiedContent {
		meta, metaErr := frameSessionMeta(sess, postWelcomeToken.ac, 1)
		if metaErr != nil {
			postWelcomeTicket.End()
			failAttachment()
			return
		}
		if err := boundedHandshakeOperation(handshakeCtx, tr, func() error {
			return postWelcomeToken.ac.sendExpectedTransportForAttachment(expected, meta, postWelcomeTicket)
		}); err != nil {
			postWelcomeTicket.End()
			failAttachment()
			return
		}
		postWelcomeToken.ac.sendMu.Lock()
		postWelcomeToken.ac.proxiedMetaRevision = 1
		postWelcomeToken.ac.sendMu.Unlock()
	}
	if lease != nil && (rc == nil || !rc.markAttachmentReady(lease)) {
		postWelcomeTicket.End()
		failAttachment()
		return
	}
	postWelcomeTicket.End()

	paintToken := attachmentOwnerToken(owner, ac, tr)
	painted := make(chan bool, 1)
	if err := boundedHandshakeOperation(handshakeCtx, tr, func() error {
		painted <- d.firstPaintForTransition(paintToken)
		return nil
	}); err != nil {
		failAttachment()
		return
	}
	if !<-painted || handshakeCtx.Err() != nil {
		failAttachment()
		return
	}
	if !d.commitResumeClaim(ac) {
		failAttachment()
		return
	}
	stopHandshakeTransport()
	finishHandshake()
	d.runConnLoop(ac)
	_ = tr.Close()
}
