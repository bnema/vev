package palette

import (
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
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
	ResultKindCreateSessionDestination
)

// CreateSessionDestinationKind identifies where a CNS result creates a session.
type CreateSessionDestinationKind uint8

const (
	CreateSessionOnServingDaemon CreateSessionDestinationKind = iota + 1
	CreateSessionOnLocalRoute
	CreateSessionOnRemoteHost
)

// Result is an immutable palette target. Its kind is the sole discriminator
// for its private command, local-session, remote-session, or route payload.
type Result struct {
	kind              ResultKind
	command           command.Command
	session           sessionPayload
	remoteSession     remoteSessionPayload
	route             routePayload
	createDestination createSessionDestinationPayload
}

type sessionPayload struct {
	display   string
	createdAt time.Time
	target    protocol.ExactSessionTarget
}

type remoteSessionPayload struct {
	key               domain.RemoteSessionKey
	target            domain.RemoteSessionTarget
	unavailableReason string
}

type routePayload struct {
	name   string
	label  string
	action protocol.RouteNavigationAction
}

type createSessionDestinationPayload struct {
	name          string
	kind          CreateSessionDestinationKind
	displayOrigin string
	endpoint      string
	route         protocol.RouteRef
	snapshotGen   uint64
}

// NewCommandResult creates a static command palette target.
func NewCommandResult(cmd command.Command) Result {
	return Result{kind: ResultKindCommand, command: cmd}
}

// NewActiveSessionResult creates an immutable active named-session target.
func NewActiveSessionResult(target protocol.ExactSessionTarget, createdAt time.Time) Result {
	return NewActiveSessionResultWithDisplayOrigin(target, createdAt, "")
}

// NewActiveSessionResultWithDisplayOrigin creates an active named-session target
// qualified for presentation through a remote attachment origin.
func NewActiveSessionResultWithDisplayOrigin(target protocol.ExactSessionTarget, createdAt time.Time, displayOrigin string) Result {
	return Result{
		kind:    ResultKindActiveSession,
		session: newSessionPayload(target, createdAt, displayOrigin),
	}
}

// NewStoppedSessionResult creates an immutable stopped named-session target.
func NewStoppedSessionResult(target protocol.ExactSessionTarget, createdAt time.Time) Result {
	return NewStoppedSessionResultWithDisplayOrigin(target, createdAt, "")
}

// NewStoppedSessionResultWithDisplayOrigin creates a stopped named-session target
// qualified for presentation through a remote attachment origin.
func NewStoppedSessionResultWithDisplayOrigin(target protocol.ExactSessionTarget, createdAt time.Time, displayOrigin string) Result {
	return Result{
		kind:    ResultKindStoppedSession,
		session: newSessionPayload(target, createdAt, displayOrigin),
	}
}

func newSessionPayload(target protocol.ExactSessionTarget, createdAt time.Time, displayOrigin string) sessionPayload {
	display := target.SessionName
	if displayOrigin != "" {
		display = domain.RemoteSessionDisplay(target.SessionName, displayOrigin)
	}
	return sessionPayload{display: display, createdAt: createdAt, target: target}
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
func NewRecentRouteResult(name, label string, action protocol.RouteNavigationAction) Result {
	return Result{kind: ResultKindRecentRoute, route: routePayload{name: name, label: label, action: action}}
}

// NewCreateSessionDestination creates an immutable destination template. The
// palette binds the current validated CNS name without parsing rendered text.
func NewCreateSessionDestination(kind CreateSessionDestinationKind, displayOrigin, endpoint string, route protocol.RouteRef, snapshotGeneration uint64) Result {
	return Result{kind: ResultKindCreateSessionDestination, createDestination: createSessionDestinationPayload{
		kind: kind, displayOrigin: displayOrigin, endpoint: endpoint, route: route, snapshotGen: snapshotGeneration,
	}}
}

func (r Result) withCreateSessionName(name string) Result {
	r.createDestination.name = name
	return r
}

func (r Result) Kind() ResultKind { return r.kind }

func (r Result) sameTarget(other Result) bool {
	if r.kind != other.kind {
		return false
	}
	switch r.kind {
	case ResultKindCommand:
		return r.command.Slug == other.command.Slug
	case ResultKindActiveSession, ResultKindStoppedSession:
		return r.session.target == other.session.target
	case ResultKindRemoteSession:
		return r.remoteSession.key == other.remoteSession.key && r.remoteSession.target == other.remoteSession.target
	case ResultKindRecentRoute:
		return r.route.action == other.route.action
	case ResultKindCreateSessionDestination:
		return r.createDestination.kind == other.createDestination.kind &&
			r.createDestination.endpoint == other.createDestination.endpoint &&
			r.createDestination.route == other.createDestination.route &&
			r.createDestination.snapshotGen == other.createDestination.snapshotGen
	default:
		return false
	}
}

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
	if r.kind == ResultKindCreateSessionDestination {
		return r.createSessionDestinationDisplay()
	}
	return r.sessionDisplayPrefix() + r.session.display
}

func (r Result) createSessionDestinationDisplay() string {
	destination := r.createDestination
	if destination.name == "" {
		if destination.kind == CreateSessionOnLocalRoute {
			return "Create session locally…"
		}
		return "Create session on " + destination.displayOrigin + "…"
	}
	text := "Create session “" + destination.name + "”"
	if destination.kind != CreateSessionOnLocalRoute {
		text += " on " + destination.displayOrigin
	}
	return text
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

// searchTerms returns the user-facing identity and qualified label without the
// action prefix. The offset maps term rune positions back into DisplayText.
func (r Result) searchTerms() (identity, label string, offset int, ok bool) {
	switch r.kind {
	case ResultKindCommand:
		return r.command.Code, r.command.Code, 0, true
	case ResultKindActiveSession, ResultKindStoppedSession:
		return r.session.target.SessionName, r.session.display, utf8.RuneCountInString(r.sessionDisplayPrefix()), true
	case ResultKindRemoteSession:
		return r.remoteSession.key.Name, r.remoteSession.key.Display(), utf8.RuneCountInString(activeSessionDisplayPrefix), true
	case ResultKindRecentRoute:
		return r.route.name, r.route.label, utf8.RuneCountInString(activeSessionDisplayPrefix), true
	default:
		return "", "", 0, false
	}
}

// Command returns the command payload only for command results.
func (r Result) Command() (command.Command, bool) {
	return r.command, r.kind == ResultKindCommand
}

// SessionName returns the session name only for session results.
func (r Result) SessionName() (string, bool) {
	return r.session.target.SessionName, r.kind == ResultKindActiveSession || r.kind == ResultKindStoppedSession
}

// SessionCreatedAt returns the session creation time only for session results.
func (r Result) SessionCreatedAt() (time.Time, bool) {
	return r.session.createdAt, r.kind == ResultKindActiveSession || r.kind == ResultKindStoppedSession
}

// SessionTarget returns the exact lifecycle UUID and name for local active and
// stopped session results.
func (r Result) SessionTarget() (protocol.ExactSessionTarget, bool) {
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
func (r Result) RouteNavigationAction() (protocol.RouteNavigationAction, bool) {
	return r.route.action, r.kind == ResultKindRecentRoute
}

// CreateSessionDestination returns structured creation authority only for CNS
// destination rows. Labels are never parsed back into these values.
func (r Result) CreateSessionDestination() (name string, kind CreateSessionDestinationKind, displayOrigin, endpoint string, route protocol.RouteRef, ok bool) {
	d := r.createDestination
	return d.name, d.kind, d.displayOrigin, d.endpoint, d.route, r.kind == ResultKindCreateSessionDestination
}

// CreateSessionSnapshotGeneration returns the route snapshot generation bound
// to a local destination. Other destination kinds carry zero.
func (r Result) CreateSessionSnapshotGeneration() (uint64, bool) {
	return r.createDestination.snapshotGen, r.kind == ResultKindCreateSessionDestination
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
