package palette

import (
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/command"
)

// ResultKind identifies the immutable target represented by a palette row.
type ResultKind uint8

const (
	ResultKindCommand ResultKind = iota
	ResultKindActiveSession
	ResultKindStoppedSession
	ResultKindRemoteSession
	ResultKindRecentRoute
)

// Result is an immutable palette target. Its kind is the sole discriminator
// for its private command or session payload.
type Result struct {
	kind          ResultKind
	command       command.Command
	session       sessionPayload
	remoteSession remoteSessionPayload
	route         routePayload
}

type sessionPayload struct {
	name      string
	createdAt time.Time
	target    ports.ExactSessionTarget
}

type remoteSessionPayload struct {
	key               domain.RemoteSessionKey
	target            domain.RemoteSessionTarget
	unavailableReason string
}

type routePayload struct {
	label  string
	action ports.RouteNavigationAction
}

// NewCommandResult creates a static command palette target.
func NewCommandResult(cmd command.Command) Result {
	return Result{kind: ResultKindCommand, command: cmd}
}

// NewActiveSessionResult creates an immutable active named-session target.
func NewActiveSessionResult(target ports.ExactSessionTarget, createdAt time.Time) Result {
	return Result{
		kind:    ResultKindActiveSession,
		session: sessionPayload{name: target.SessionName, createdAt: createdAt, target: target},
	}
}

// NewStoppedSessionResult creates an immutable stopped named-session target.
func NewStoppedSessionResult(target ports.ExactSessionTarget, createdAt time.Time) Result {
	return Result{
		kind:    ResultKindStoppedSession,
		session: sessionPayload{name: target.SessionName, createdAt: createdAt, target: target},
	}
}

// NewRemoteSessionResult creates an immutable catalog-backed remote target.
func NewRemoteSessionResult(key domain.RemoteSessionKey, target domain.RemoteSessionTarget, unavailableReason string) Result {
	return Result{
		kind: ResultKindRemoteSession,
		remoteSession: remoteSessionPayload{
			key: key, target: target, unavailableReason: unavailableReason,
		},
	}
}

// NewRecentRouteResult creates an immutable client-ledger route target.
func NewRecentRouteResult(label string, action ports.RouteNavigationAction) Result {
	return Result{kind: ResultKindRecentRoute, route: routePayload{label: label, action: action}}
}

func (r Result) Kind() ResultKind { return r.kind }

func (r Result) DisplayText() string {
	if r.kind == ResultKindCommand {
		return r.command.Code
	}
	if r.kind == ResultKindRemoteSession {
		return activeSessionDisplayPrefix + r.remoteSession.key.Display()
	}
	if r.kind == ResultKindRecentRoute {
		return activeSessionDisplayPrefix + r.route.label
	}
	return r.sessionDisplayPrefix() + r.session.name
}

func (r Result) sessionDisplayPrefix() string {
	if r.kind == ResultKindStoppedSession {
		return stoppedSessionDisplayPrefix
	}
	return activeSessionDisplayPrefix
}

func (r Result) SearchText() string {
	return r.DisplayText()
}

// Command returns the command payload only for command results.
func (r Result) Command() (command.Command, bool) {
	return r.command, r.kind == ResultKindCommand
}

// SessionName returns the session name only for session results.
func (r Result) SessionName() (string, bool) {
	return r.session.name, r.kind == ResultKindActiveSession || r.kind == ResultKindStoppedSession
}

// SessionCreatedAt returns the session creation time only for session results.
func (r Result) SessionCreatedAt() (time.Time, bool) {
	return r.session.createdAt, r.kind == ResultKindActiveSession || r.kind == ResultKindStoppedSession
}

// SessionTarget returns the exact lifecycle UUID and name for local active and
// stopped session results.
func (r Result) SessionTarget() (ports.ExactSessionTarget, bool) {
	return r.session.target, r.kind == ResultKindActiveSession || r.kind == ResultKindStoppedSession
}

// RemoteSessionTarget returns the structured catalog target only for a remote
// session result.
func (r Result) RemoteSessionTarget() (domain.RemoteSessionTarget, bool) {
	return r.remoteSession.target, r.kind == ResultKindRemoteSession
}

// RemoteSessionKey returns the structured catalog identity only for a remote
// session result.
func (r Result) RemoteSessionKey() (domain.RemoteSessionKey, bool) {
	return r.remoteSession.key, r.kind == ResultKindRemoteSession
}

// RemoteSessionUnavailableReason returns the catalog availability reason only
// for a remote session result.
func (r Result) RemoteSessionUnavailableReason() (string, bool) {
	return r.remoteSession.unavailableReason, r.kind == ResultKindRemoteSession
}

// RouteNavigationAction returns the exact client-ledger target only for a
// recent-route result.
func (r Result) RouteNavigationAction() (ports.RouteNavigationAction, bool) {
	return r.route.action, r.kind == ResultKindRecentRoute
}

const (
	activeSessionDisplayPrefix  = "Switch to session "
	stoppedSessionDisplayPrefix = "Resume session "
)

// CommandResults converts static commands to immutable palette targets.
func CommandResults(commands []command.Command) []Result {
	results := make([]Result, len(commands))
	for i, cmd := range commands {
		results[i] = NewCommandResult(cmd)
	}
	return results
}
