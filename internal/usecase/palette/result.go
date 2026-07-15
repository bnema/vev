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

// Result is an immutable palette target. Its kind is the sole discriminator
// for its private command or session payload.
type Result struct {
	kind    ResultKind
	command command.Command
	session sessionPayload
}

type sessionPayload struct {
	name      string
	createdAt time.Time
	sessionID domain.SessionID
}

// NewCommandResult creates a static command palette target.
func NewCommandResult(cmd command.Command) Result {
	return Result{kind: ResultKindCommand, command: cmd}
}

// NewActiveSessionResult creates an immutable active named-session target.
func NewActiveSessionResult(name string, createdAt time.Time, sessionID domain.SessionID) Result {
	return Result{
		kind:    ResultKindActiveSession,
		session: sessionPayload{name: name, createdAt: createdAt, sessionID: sessionID},
	}
}

// NewStoppedSessionResult creates an immutable stopped named-session target.
func NewStoppedSessionResult(name string, createdAt time.Time) Result {
	return Result{
		kind:    ResultKindStoppedSession,
		session: sessionPayload{name: name, createdAt: createdAt},
	}
}

func (r Result) Kind() ResultKind { return r.kind }

func (r Result) DisplayText() string {
	if r.kind == ResultKindCommand {
		return r.command.Code
	}
	return sessionDisplayPrefix + r.session.name
}

func (r Result) SearchText() string {
	if r.kind == ResultKindCommand {
		return r.command.Code
	}
	return r.session.name
}

// Command returns the command payload only for command results.
func (r Result) Command() (command.Command, bool) {
	return r.command, r.kind == ResultKindCommand
}

// SessionName returns the session name only for session results.
func (r Result) SessionName() (string, bool) {
	return r.session.name, r.kind != ResultKindCommand
}

// SessionCreatedAt returns the session creation time only for session results.
func (r Result) SessionCreatedAt() (time.Time, bool) {
	return r.session.createdAt, r.kind != ResultKindCommand
}

// SessionID returns the lifecycle identity only for active session results.
func (r Result) SessionID() (domain.SessionID, bool) {
	return r.session.sessionID, r.kind == ResultKindActiveSession
}

const sessionDisplayPrefix = "Switch to session "

// CommandResults converts static commands to immutable palette targets.
func CommandResults(commands []command.Command) []Result {
	results := make([]Result, len(commands))
	for i, cmd := range commands {
		results[i] = NewCommandResult(cmd)
	}
	return results
}
