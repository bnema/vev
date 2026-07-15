package palette

import (
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
)

// ResultKind identifies the immutable target represented by a palette row.
type ResultKind uint8

const (
	ResultKindCommand ResultKind = iota
	ResultKindActiveSession
	ResultKindStoppedSession
)

// Result is a searchable palette target. Command and session accessors make
// the target's capabilities explicit without callers needing type assertions.
type Result interface {
	Kind() ResultKind
	DisplayText() string
	SearchText() string
	CommandInfo() (command.Command, bool)
	Session() (SessionResult, bool)
}

// CommandResult is a static command palette target.
type CommandResult struct {
	command command.Command
}

func NewCommandResult(cmd command.Command) CommandResult     { return CommandResult{command: cmd} }
func (r CommandResult) Kind() ResultKind                     { return ResultKindCommand }
func (r CommandResult) DisplayText() string                  { return r.command.Code }
func (r CommandResult) SearchText() string                   { return r.command.Code }
func (r CommandResult) Command() command.Command             { return r.command }
func (r CommandResult) CommandInfo() (command.Command, bool) { return r.command, true }
func (r CommandResult) Session() (SessionResult, bool)       { return SessionResult{}, false }

// SessionResult is an immutable named-session target. An active session has a
// SessionID; a stopped session intentionally does not.
type SessionResult struct {
	name      string
	createdAt time.Time
	sessionID domain.SessionID
	active    bool
}

func NewActiveSessionResult(name string, createdAt time.Time, sessionID domain.SessionID) SessionResult {
	return SessionResult{name: name, createdAt: createdAt, sessionID: sessionID, active: true}
}

func NewStoppedSessionResult(name string, createdAt time.Time) SessionResult {
	return SessionResult{name: name, createdAt: createdAt}
}

func (r SessionResult) Kind() ResultKind {
	if r.active {
		return ResultKindActiveSession
	}
	return ResultKindStoppedSession
}
func (r SessionResult) Name() string         { return r.name }
func (r SessionResult) CreatedAt() time.Time { return r.createdAt }
func (r SessionResult) SessionID() (domain.SessionID, bool) {
	return r.sessionID, r.active
}
func (r SessionResult) DisplayText() string                  { return sessionDisplayPrefix + r.name }
func (r SessionResult) SearchText() string                   { return r.name }
func (r SessionResult) CommandInfo() (command.Command, bool) { return command.Command{}, false }
func (r SessionResult) Session() (SessionResult, bool)       { return r, true }

const sessionDisplayPrefix = "Switch to session "

// CommandResults converts static commands to typed palette targets.
func CommandResults(commands []command.Command) []Result {
	results := make([]Result, len(commands))
	for i, cmd := range commands {
		results[i] = NewCommandResult(cmd)
	}
	return results
}
