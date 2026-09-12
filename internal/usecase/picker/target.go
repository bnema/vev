package picker

import "github.com/bnema/vev/internal/domain"

// Target is the resolved navigation destination behind one opaque picker key.
// It stays source-private: only the owning daemon resolves keys to targets and
// commits them; a presenting client never sees one.
type Target struct {
	Session           domain.SessionID
	Incarnation       domain.IncarnationID
	Name              string
	RemoteKey         *domain.RemoteSessionKey
	RemoteTarget      *domain.RemoteSessionTarget
	RemoteHost        string
	UnavailableReason string
	TabID             domain.TabStableID
	TabIndex          int
	Stopped           bool
	// ExpectedCreatedAt optionally pins this target to a particular named
	// session lifecycle. Callers that obtain a snapshot outside the daemon use
	// it to reject a same-name replacement at commit time.
	ExpectedCreatedAt *int64
}
