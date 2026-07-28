package daemon

// acceptedTabLayoutRetryToken binds a delayed tiled retry to the exact pane
// owner generations which accepted the failure. A moved pane cannot lend its
// old source worker authority over either the PTY or source publications.
type acceptedTabLayoutRetryToken struct {
	session *session
	tab     *tab
	members []resizeMember
}

func newAcceptedTabLayoutRetryToken(sess *session, tb *tab, members []resizeMember) acceptedTabLayoutRetryToken {
	return acceptedTabLayoutRetryToken{session: sess, tab: tb, members: append([]resizeMember(nil), members...)}
}

func (t acceptedTabLayoutRetryToken) current() bool {
	if t.session == nil || t.tab == nil || len(t.members) == 0 {
		return false
	}
	for i := range t.members {
		member := &t.members[i]
		if member.session != t.session || member.tab != t.tab || !resizeMemberOwnerCurrent(member) {
			return false
		}
	}
	return true
}
