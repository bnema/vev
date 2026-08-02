package daemon

// frozenMoveAttachmentRetirement is proof that every source snatched client
// retired by a final move has had its exact role-effect gate frozen and drained.
// It is prepared while the source session lock is held, before topology writes.
type frozenMoveAttachmentRetirement struct {
	clients []*attachedClient
}

func sameMoveSnatchedLocked(sess *session, admitted []*attachedClient) bool {
	return sess != nil && len(admitted) == 0
}

func prepareFrozenMoveAttachmentRetirementLocked(sess *session, admitted []*attachedClient, frozen frozenRoleEffectGates) (frozenMoveAttachmentRetirement, bool) {
	if !sameMoveSnatchedLocked(sess, admitted) {
		return frozenMoveAttachmentRetirement{}, false
	}
	if len(admitted) == 0 {
		return frozenMoveAttachmentRetirement{}, true
	}
	if !frozen.acquired || !frozen.drained {
		return frozenMoveAttachmentRetirement{}, false
	}
	for _, ac := range admitted {
		if !frozen.contains(ac) {
			return frozenMoveAttachmentRetirement{}, false
		}
	}
	return frozenMoveAttachmentRetirement{clients: append([]*attachedClient(nil), admitted...)}, true
}
