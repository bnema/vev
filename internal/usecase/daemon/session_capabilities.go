package daemon

import "errors"

// errSessionCannotYieldMoves rejects move-picker entry for sessions whose
// tabs and panes cannot leave the session (no local session sets this today;
// proxy sessions will).
var errSessionCannotYieldMoves = errors.New("session does not support moving tabs or panes")

// sessionCapabilities declares what cross-session operations a session
// supports. Fields are negated so the zero value describes a fully capable
// local session — literal &session{...} constructions (tests, restore paths)
// keep today's behavior without opting in.
//
// Set once at construction, never mutated: reading needs no lock.
type sessionCapabilities struct {
	cannotAcceptMoves bool
	cannotYieldMoves  bool
}

func (c sessionCapabilities) yieldsMoves() bool { return !c.cannotYieldMoves }

func (s *session) capabilities() sessionCapabilities { return s.caps }
