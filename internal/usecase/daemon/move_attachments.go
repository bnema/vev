package daemon

// frozenMoveAttachmentRetirement is proof that every source snatched client
// retired by a final move has had its exact role-effect gate frozen and drained.
// It is prepared while the source session lock is held, before topology writes.
type frozenMoveAttachmentRetirement struct {
	clients []*attachedClient
}

func snapshotMoveSnatchedLocked(sess *session) []*attachedClient {
	clients := make([]*attachedClient, 0, len(sess.snatched))
	for ac := range sess.snatched {
		clients = append(clients, ac)
	}
	return clients
}

func sameMoveSnatchedLocked(sess *session, admitted []*attachedClient) bool {
	if sess == nil || len(sess.snatched) != len(admitted) {
		return false
	}
	for _, ac := range admitted {
		if _, ok := sess.snatched[ac]; !ok {
			return false
		}
	}
	return true
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
