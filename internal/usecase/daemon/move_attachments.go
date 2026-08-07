package daemon

func moveAttachmentTokenCurrentLocked(token attachmentConnectionToken, sess *session) bool {
	return sameAttachmentOwner(token.owner, sess) && token.ac != nil && token.generation == token.ac.connectionGeneration.Load() &&
		sameAttachmentOwner(token.ac.currentAttachmentOwner(), sess) && token.ac.transportSnapshotCurrent(token.transport) &&
		attachmentRegisteredLocked(sess, token.ac)
}

// sameMoveAttachmentsLocked reports whether the source membership is unchanged
// since admission. Caller holds the source session lock.
func sameMoveAttachmentsLocked(sess *session, admitted []*attachedClient) bool {
	if sess == nil || len(sess.attachments) != len(admitted) {
		return false
	}
	for _, ac := range admitted {
		if !attachmentRegisteredLocked(sess, ac) {
			return false
		}
	}
	return true
}

// detachMoveAttachmentsLocked invalidates every attachment of a source session
// that became empty. Moves never transfer attachment ownership to the target;
// each connection is retired independently after the shared content move.
// Caller holds the source session lock and the affected attachment gates are
// frozen.
func detachMoveAttachmentsLocked(sess *session, transports map[*attachedClient]transportSnapshot) []detachedAttachmentSnapshot {
	if sess == nil || len(sess.attachments) == 0 {
		return nil
	}
	attachments := sess.snapshotAttachmentsLocked()
	retired := make([]detachedAttachmentSnapshot, 0, len(attachments))
	for _, ac := range attachments {
		retired = append(retired, detachedAttachmentSnapshot{ac: ac, transport: transports[ac]})
		ac.connectionGeneration.Add(1)
		ac.setSession(nil)
		ac.invalidateFrozenAttachmentCapability()
	}
	clear(sess.attachments)
	return retired
}
