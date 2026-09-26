package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys/kittykey"
)

// Autonomous client supervisor.
//
// The supervisor is the terminal-owning owner of one
// autonomous client process. It enters raw mode once, before it ever reaches
// for the broker, and owns a single terminal input lifetime for the whole run.
// While the broker connection is down it renders the picker and keeps
// reconnecting. One terminal input pump serves both picker and attachment;
// the supervisor coordinates their lifetimes without decoding session output.
//
// Connectivity is one attempt at a time. An attempt connects, subscribes, and
// waits for the first committed publication before it is Ready; readiness
// resets the equal-jitter exponential retry cadence (100ms doubling to a 2s
// cap). A transient connect, subscribe, or snapshot failure, and the loss of an
// established connection observed through BrokerService.Done/Err while no
// logical stream is held, both retire the old subscription and service, wait
// the cadence, and reconnect under a fresh generation. A failure that can never
// succeed by retrying (an incompatible carriage or a conflicting policy) is
// surfaced once and stops automatic reconnection; only the domain's terminal
// codes end the process.
//
// A connection attempt is joined before Run returns. Since the connector
// contract requires Connect to honor cancellation, a completion that arrives
// after the supervisor has begun terminating is still drained and its service
// and subscription are closed, so a late superseded success never leaks. Raw
// mode is restored exactly once on every exit path, terminal EOF and process
// cancellation included.
//
// Parent cancellation settles the run: an attempt that settles at the same
// instant as cancellation is retired instead of adopted, and the run ends with
// ctx.Err without publishing a retry cadence, rendering ConnectivityRetryWait,
// or notifying a connectivity failure.

// Supervisor retry cadence bounds. The exponential cap doubles from the initial
// interval to the maximum; the equal-jitter interval is then half the cap plus
// a uniform fraction of the other half.
const (
	supervisorRetryInitial = 100 * time.Millisecond
	supervisorRetryMax     = 2 * time.Second
)

// Presentation is what the autonomous client is presenting on the terminal.
type Presentation uint8

const (
	// PresentPicker shows the session picker.
	PresentPicker Presentation = iota + 1
	// PresentConnecting shows the attach transition while connecting.
	PresentConnecting
	// PresentAttached shows a live attachment.
	PresentAttached
	// PresentTerminating is the terminal state: raw mode is being restored and
	// the process is leaving.
	PresentTerminating
	// PresentAttachedPicker composes the client picker over a live attachment.
	// The logical attachment stays attached: its output
	// is applied and acknowledged but not written while the picker owns the
	// terminal and its input. Cancel returns to the same attachment without
	// reconnecting.
	PresentAttachedPicker
)

func (p Presentation) String() string {
	switch p {
	case PresentPicker:
		return "picker"
	case PresentConnecting:
		return "connecting"
	case PresentAttached:
		return "attached"
	case PresentTerminating:
		return "terminating"
	case PresentAttachedPicker:
		return "attached_picker"
	default:
		return "unknown"
	}
}

// Connectivity is the broker-connection lifecycle.
type Connectivity uint8

const (
	// ConnectivityDisconnected holds no broker connection: either before the
	// first attempt, after a non-retryable failure, or while terminating.
	ConnectivityDisconnected Connectivity = iota + 1
	// ConnectivityConnectingBroker is one in-flight connect/subscribe/first
	// publication attempt.
	ConnectivityConnectingBroker
	// ConnectivityReady holds an established connection with its first
	// publication committed.
	ConnectivityReady
	// ConnectivityRetryWait waits the retry cadence before the next attempt.
	ConnectivityRetryWait
)

func (c Connectivity) String() string {
	switch c {
	case ConnectivityDisconnected:
		return "disconnected"
	case ConnectivityConnectingBroker:
		return "connecting_broker"
	case ConnectivityReady:
		return "ready"
	case ConnectivityRetryWait:
		return "retry_wait"
	default:
		return "unknown"
	}
}

// State is the immutable projection of the supervisor.
type State struct {
	Presentation Presentation
	Connectivity Connectivity
	// Attempt counts consecutive broker-connection failures since the last
	// Ready. It is zero before the first attempt and after Ready, and drives
	// the equal-jitter exponential backoff.
	Attempt uint64
	// Generation identifies the current connection attempt. It increases before
	// every attempt, so a settled attempt carries the generation that produced
	// it. Run joins each attempt before it advances, so every completion today
	// carries the current generation: the comparison in adoptAttempt is a
	// defensive invariant for a future concurrent driver, not a path this
	// driver can currently reach.
	Generation uint64
	// Err is the most recent visible connectivity failure, nil while healthy
	// and before any failure.
	Err error
	// ReadyLost is set only when an established ready generation was lost and
	// cleared by the replacement's committed publication.
	ReadyLost bool
}

// supervisorEventKind is one input to the pure supervisor reducer.
type supervisorEventKind uint8

const (
	// supervisorBeginAttempt starts one connect/subscribe attempt.
	supervisorBeginAttempt supervisorEventKind = iota + 1
	// supervisorReady marks the first committed publication of the current
	// attempt.
	supervisorReady
	// supervisorTransientFailure is a connect, subscribe, or first-publication
	// failure that is worth retrying.
	supervisorTransientFailure
	// supervisorBrokerLoss is the loss of an established connection observed
	// through BrokerService.Done/Err while no logical stream is held.
	supervisorBrokerLoss
	// supervisorAttachBegin marks the admission of one committed attachment
	// stream: the picker released input and the supervisor is presenting the
	// connecting state until the initial publication is committed.
	supervisorAttachBegin
	// supervisorAttached marks a committed initial publication: the worker
	// proved the full output frame was written, flushed,
	// and UI-committed. Welcome alone never produces it.
	supervisorAttached
	// supervisorAttachEnded returns a settled attachment to the picker without
	// restarting the process. event.err carries the typed failure, if any.
	supervisorAttachEnded
	// supervisorNonRetryable is a failure that retrying can never fix.
	supervisorNonRetryable
	// supervisorNavigationSettled ends a pending initial navigation's
	// Connecting presentation when no attachment took over the terminal.
	supervisorNavigationSettled
	// supervisorTerminal ends the process: terminal EOF, process cancellation,
	// or a terminal broker failure.
	supervisorTerminal
	// supervisorOverlayOpened composes the picker over the live attachment.
	supervisorOverlayOpened
	// supervisorOverlayClosed returns from the picker overlay to the same live
	// attachment.
	supervisorOverlayClosed
)

type supervisorEvent struct {
	kind supervisorEventKind
	err  error
	// navigating marks a supervisorBeginAttempt made while an initial
	// navigation is still pending: the user asked for a session, not the
	// picker, so the attempt presents Connecting instead of flashing the
	// picker until the navigation settles.
	navigating bool
}

// reduceSupervisor is the pure connectivity/presentation reducer. It is
// side-effect free so the state machine can be exhaustively table-tested; the
// driver applies it under a mutex and renders the result.
func reduceSupervisor(state State, event supervisorEvent) State {
	switch event.kind {
	case supervisorBeginAttempt:
		state.Presentation = PresentPicker
		if event.navigating {
			state.Presentation = PresentConnecting
		}
		state.Connectivity = ConnectivityConnectingBroker
		state.Generation++
		state.Err = nil
	case supervisorReady:
		state.Connectivity = ConnectivityReady
		state.Attempt = 0
		state.Err = nil
	case supervisorTransientFailure:
		state.Connectivity = ConnectivityRetryWait
		state.Attempt++
		state.Err = event.err
	case supervisorBrokerLoss:
		state.ReadyLost = state.Connectivity == ConnectivityReady
		state.Connectivity = ConnectivityRetryWait
		state.Attempt++
		state.Err = event.err
	case supervisorAttachBegin:
		// Connecting is the honest presentation until the committed initial
		// publication: the broker connection is still ready and the attempt
		// cadence is untouched.
		state.Presentation = PresentConnecting
		state.Err = nil
	case supervisorAttached:
		state.Presentation = PresentAttached
		state.Err = nil
	case supervisorAttachEnded:
		// A settled attachment always returns to the picker in the same
		// process, attached or not; a stream that fails after attachment must
		// never leave the attached presentation showing.
		state.Presentation = PresentPicker
		state.Err = event.err
	case supervisorNonRetryable:
		// The run stays on the picker with the visible error; a pending
		// initial navigation can no longer present Connecting.
		state.Presentation = PresentPicker
		state.Connectivity = ConnectivityDisconnected
		state.Err = event.err
	case supervisorNavigationSettled:
		// An initial navigation that ended without an attachment (a resolved
		// picker, or a loss before it could be taken) leaves Connecting.
		if state.Presentation == PresentConnecting {
			state.Presentation = PresentPicker
		}
	case supervisorOverlayOpened:
		// Only a committed attachment can host the overlay: a connecting
		// foreground has not proven its first frame yet.
		if state.Presentation == PresentAttached {
			state.Presentation = PresentAttachedPicker
			state.Err = nil
		}
	case supervisorOverlayClosed:
		if state.Presentation == PresentAttachedPicker {
			state.Presentation = PresentAttached
		}
	case supervisorTerminal:
		state.Presentation = PresentTerminating
		state.Connectivity = ConnectivityDisconnected
		state.Err = event.err
	}
	return state
}

// LifecycleNoticeKind is one bounded autonomous lifecycle transition.
type LifecycleNoticeKind uint8

const (
	LifecycleNoticeDetachToPicker LifecycleNoticeKind = iota + 1
	LifecycleNoticeDetachAndExit
	LifecycleNoticeSessionEnded
	LifecycleNoticeDestinationFailed
	LifecycleNoticeBrokerLost
	LifecycleNoticeBrokerReconnected
	LifecycleNoticeOutcomeUnknown
	// LifecycleNoticeSelectionUnavailable reports a local refusal: the picker
	// could not resolve the committed selection against the current catalogue,
	// or initial navigation had nothing to resolve. No destination was dialed,
	// so reporting it as a destination failure would name the wrong cause.
	LifecycleNoticeSelectionUnavailable
)

// LifecycleNotice carries classification only; adapter diagnostics and free
// text never cross this presentation seam.
type LifecycleNotice struct {
	Kind LifecycleNoticeKind
}

// SupervisorConfig supplies the autonomous client's dependencies. The
// connector, terminal, and clock are required; render, notify, and jitter are
// injectable seams with safe defaults. Render, notify, and jitter must not
// block: the supervisor drives them from its single run goroutine.
// SpinnerPresentation advances a transition frame on the serialized control
// path, never from a competing terminal writer goroutine.
type SpinnerPresentation interface {
	Spinner() <-chan time.Time
	AdvanceSpinner(State)
	StopSpinner()
}

type SupervisorConfig struct {
	// Logger receives bounded client lifecycle diagnostics. Optional.
	Logger *slog.Logger
	// Connector establishes the broker connection. Required.
	Connector ports.BrokerConnector
	// Terminal is the controlling terminal the supervisor owns. Required.
	Terminal ports.Terminal
	// Clock supplies the retry timers. Required; use ports.Clock, not the wall
	// clock.
	Clock ports.Clock
	// Clipboard is the client-side image clipboard for remote attachments.
	Clipboard ports.ClipboardReader
	// Render asks the composition to paint the current state. It is called for
	// visible transitions, after picker catalogue publications, and for every
	// resize invalidation the supervisor's serialized waits consume. The
	// composition paints only for the states PickerPresentation admits, which is
	// exactly PresentPicker, using Picker.Render and Picker.RenderNotice and
	// sampling Terminal.Geometry at render time so a resize repaint observes the
	// latest size. PresentConnecting is deliberately not admitted: an admitted
	// attachment foreground owns the terminal writer from Begin, before the
	// attached marker and the initial publication, so painting then would
	// overwrite session output. An attached foreground still owns the terminal
	// and a terminating process is leaving it, so neither is ever painted over
	// by a resize invalidation. Optional; the default renders nothing.
	Render  func(State)
	Spinner SpinnerPresentation
	// Notify surfaces a connectivity failure without leaving the picker.
	// Optional; the default notifies nothing.
	Notify func(State, error)
	// NotifyLifecycle reports typed, bounded lifecycle transitions.
	NotifyLifecycle func(LifecycleNotice)
	// Jitter returns an equal-jitter fraction in [0,1). It is the only
	// randomness seam so tests can pin the retry cadence exactly. Optional; the
	// default is a process-local random source.
	Jitter func() float64
	// UI optionally enables supervisor-owned action admission. Each attachment
	// is bound only after it owns the pump claim; no UI preserves the prior
	// publication-only behavior with no admitted automation.
	UI *UI
	// Picker is the client-owned picker. When set, the
	// supervisor folds every broker publication into it and hands it every
	// terminal read from the same single input lifetime used for EOF detection,
	// so the picker never starts a second reader. When nil the supervisor drops
	// terminal input through the same pump. Optional.
	Picker pickerHost
	// AttachmentEnvironment is composition-owned process context copied into
	// each attachment Hello. The use case deliberately does not inspect the
	// process environment; omitted fields retain their protocol zero values.
	AttachmentEnvironment AttachmentEnvironment
	SessionEnvironment    SessionEnvironment
	// LifecycleActions carries explicit attachment lifecycle requests. A nil
	// channel disables external lifecycle actions. The actions are typed so
	// detach-to-picker can never be confused with detach-and-exit.
	LifecycleActions <-chan AttachmentLifecycleAction
	// InitialNavigation is the closed, one-shot navigation intent performed
	// after the first committed catalogue publication. Its zero value is
	// InitialNavigationPicker, which opens nothing, so a configuration that
	// omits it is safe. No-argument composition sets the ephemeral local
	// creation explicitly. The supervisor copies the value; it never shares a
	// mutable pointer with the composition.
	InitialNavigation InitialNavigation
	// ResolveInitialNavigation optionally translates a composition-owned CLI
	// target (an attach name or a remote endpoint) into an exact navigation
	// identity. It is a translation of a target into identity, not an owner of
	// navigation: it performs no I/O and is consulted at most once, against a
	// defensive copy of the first committed publication of the adopted
	// connection. When set, InitialNavigation must be exactly zero.
	ResolveInitialNavigation InitialNavigationResolver
}

// AttachmentEnvironment is the composition seam for client environment data
// carried by attachment Hello messages.
type AttachmentEnvironment struct {
	TermEnv   string
	Cwd       string
	TrueColor bool
	// ProbeTerminal enables the one bounded capability probe of a real outer
	// terminal (Kitty graphics and keyboard). Virtual terminals leave it off.
	ProbeTerminal bool
	// KittyKeyboard enables the kitty keyboard protocol when the probe finds
	// it, which makes Ctrl+1..9 recent-session switching available.
	KittyKeyboard bool
}

// AttachmentLifecycleAction is an explicit process/attachment decision.
type AttachmentLifecycleActionKind uint8

const (
	AttachmentDetachToPicker AttachmentLifecycleActionKind = iota + 1
	AttachmentDetachAndExit
)

// AttachmentLifecycleAction targets exactly one granted attachment.
type AttachmentLifecycleAction struct {
	Token AttachmentToken
	Kind  AttachmentLifecycleActionKind
}

// Supervisor owns one autonomous client process: raw mode, one terminal input
// lifetime, and the broker-connectivity lifecycle.
type Supervisor struct {
	cfg    SupervisorConfig
	logger *slog.Logger

	// pendingPickerKey retains a commit observed while connecting. It is
	// revalidated against the adopted service just like a ready-phase commit.
	pendingPickerKey string
	preview          previewManager

	mu    sync.Mutex
	state State

	// attachments is the single foreground host every committed attachment
	// runs through. The supervisor owns it for the whole run
	// and retains close authority over each stream it admits.
	attachments *attachmentHost
	// nextAttachment identifies one attachment run within the connection
	// attempt generation. Logical stream IDs are allocated by the broker
	// service, the single allocator on its own connection, so the supervisor
	// keeps no allocator of its own.
	nextAttachment atomic.Uint64
	// navigation is the supervisor's private copy of the composition's intent,
	// consumed exactly once. navigationConsumed is set before the intent is
	// resolved, so a refusal, a cancellation, a timeout, or a reconnection can
	// never re-arm it.
	navigation         InitialNavigation
	navigationConsumed bool
	// clientID is the stable client identity carried in every Hello this
	// supervisor sends, across reconnects and attachments.
	clientID [16]byte
	// theme retains the terminal-reported colors across attachments, so a
	// replacement attachment restores them before its own palette query ends.
	theme terminalThemeState
	// capabilities is the outer terminal's probed and enabled capabilities,
	// written once in Run before any attachment starts.
	capabilities terminalCapabilities
	// readySub is the adopted connection's subscription while the ready phase
	// runs, so the picker overlay over a live attachment keeps folding broker
	// publications. It is only touched from the run goroutine.
	readySub ports.BrokerSubscription
	// pendingSwap is the request the picker overlay committed to another
	// target while an attachment was live. It is only touched from the run
	// goroutine and consumed by runResolvedAttachment.
	pendingSwap *pickerAttachmentTarget
	// resume is the exact target a lost attachment reconnects to, set by
	// settleAttachment and consumed by runResolvedAttachment. It is only
	// touched from the run goroutine.
	resume *pickerAttachmentTarget
	// resuming is true while runResolvedAttachment reconnects a lost
	// attachment; transport-class failures of those attempts are recorded in
	// resumeErr instead of being presented, so a retry stays on Connecting.
	// resumeErr also carries the loss that started the resume. Both are only
	// touched from the run goroutine.
	resuming  bool
	resumeErr error
	// kills runs the picker's `x` operations off the run goroutine.
	kills pickerKills
	// routes is the client route ledger published to the serving daemon;
	// routesSent is the attachment that received its latest snapshot. Both are
	// only touched from the run goroutine.
	routes     *routeLedger
	routesSent AttachmentToken
}

// NewSupervisor validates the required dependencies and returns a supervisor
// parked on the picker with no connection. It does not touch the terminal; Run
// enters raw mode.
func NewSupervisor(cfg SupervisorConfig) (*Supervisor, error) {
	if err := cfg.SessionEnvironment.Validate(); err != nil {
		return nil, fmt.Errorf("client: invalid session environment: %w", err)
	}
	cfg.SessionEnvironment = cfg.SessionEnvironment.Clone()
	if supervisorNil(cfg.Connector) {
		return nil, errors.New("vev: supervisor requires a broker connector")
	}
	if supervisorNil(cfg.Terminal) {
		return nil, errors.New("vev: supervisor requires a terminal")
	}
	if supervisorNil(cfg.Clock) {
		return nil, errors.New("vev: supervisor requires a clock")
	}
	if cfg.Render == nil {
		cfg.Render = func(State) {}
	}
	if supervisorNil(cfg.Picker) {
		// Normalize a typed-nil picker so Run's nil check and the input
		// lifetime's consumer check are both safe.
		cfg.Picker = nil
	}
	if cfg.Jitter == nil {
		cfg.Jitter = rand.Float64
	}
	if cfg.ResolveInitialNavigation != nil {
		if cfg.InitialNavigation != (InitialNavigation{}) {
			return nil, errors.New("vev: supervisor accepts either an initial navigation or a resolver, not both")
		}
	} else if err := cfg.InitialNavigation.Validate(); err != nil {
		return nil, fmt.Errorf("vev: supervisor initial navigation: %w", err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	supervisor := &Supervisor{
		cfg:        cfg,
		logger:     logger,
		state:      State{Presentation: PresentPicker, Connectivity: ConnectivityDisconnected},
		clientID:   newClientID(),
		navigation: cfg.InitialNavigation,
		preview:    previewManager{clock: cfg.Clock},
	}
	// The host is the supervisor's existing foreground grant. It owns no raw
	// mode and starts no reader: the supervisor keeps its one terminal input
	// lifetime, so the attachment path adds neither a second reader nor a
	// second writer. The optional UI binds to this host's existing pump claim;
	// it never starts another terminal reader.
	supervisor.attachments = newAttachmentHost(attachmentHostConfig{
		Terminal:   cfg.Terminal,
		Clock:      cfg.Clock,
		Actions:    cfg.LifecycleActions,
		ActionUI:   cfg.UI,
		OnAttached: func(AttachmentToken) { supervisor.transition(supervisorEvent{kind: supervisorAttached}) },
	})
	return supervisor, nil
}

// initialNavigationPending reports whether an initial navigation other than
// the picker is still armed. A resolver may still resolve to the picker; the
// navigation then settles back to it without an attachment.
func (s *Supervisor) initialNavigationPending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.navigationConsumed {
		return false
	}
	return s.cfg.ResolveInitialNavigation != nil || s.navigation.Kind != InitialNavigationPicker
}

// State returns the current immutable projection.
func (s *Supervisor) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Run owns the terminal for exactly its lifetime. It enters raw mode before it
// connects to the broker, starts the single terminal input reader, and then
// drives connect/subscribe/retry until the terminal reaches EOF, the process
// context is cancelled, or a terminal broker failure settles the process. Raw
// mode is restored exactly once on every exit path.
func (s *Supervisor) Run(ctx context.Context) (retErr error) {
	restore, err := s.cfg.Terminal.EnterRaw()
	if err != nil {
		return fmt.Errorf("vev: entering raw mode: %w", err)
	}
	if restore == nil {
		restore = func() error { return nil }
	}
	restoreTerminal := sync.OnceValue(restore)
	defer func() {
		if rerr := restoreTerminal(); rerr != nil && retErr == nil {
			retErr = fmt.Errorf("vev: restoring terminal: %w", rerr)
		}
	}()

	input := startTerminalInputLifetimeWith(s.cfg.Terminal.In(), s.cfg.Picker, func(pump *terminalInputPump) {
		s.detectTerminalCapabilities(ctx, pump)
	})
	defer input.stop()
	s.attachments.setInput(input.pump)
	s.attachments.startGeometry(ctx)
	defer s.attachments.stopGeometry()

	// The active connection is owned across loop iterations. retire is
	// idempotent so every exit path closes exactly the connection it adopted.
	var (
		service ports.BrokerService
		sub     ports.BrokerSubscription
	)
	retire := func() {
		s.retirePickerKill()
		s.preview.close(s.cfg.Picker)
		s.readySub = nil
		if sub != nil {
			sub.Close()
			sub = nil
		}
		if service != nil {
			_ = service.Close()
			service = nil
		}
	}
	defer retire()

	for {
		// The generation is claimed before the connector runs, so the attempt is
		// always attributable to the state that started it and a completion can be
		// matched against the generation that produced it.
		s.transition(supervisorEvent{kind: supervisorBeginAttempt, navigating: s.initialNavigationPending()})
		generation := s.State().Generation
		attemptCtx, cancelAttempt := context.WithCancel(ctx)
		outcome := make(chan supervisorAttempt, 1)
		go func() { outcome <- s.connectAttempt(attemptCtx, generation) }()

		result, terminated, termErr := s.awaitAttempt(ctx, input, outcome)
		if terminated {
			// Connect must honor cancellation, so joining the attempt here both
			// retires a late superseded success and leaves no goroutine behind.
			cancelAttempt()
			late := <-outcome
			late.retire()
			s.transition(supervisorEvent{kind: supervisorTerminal, err: termErr})
			return termErr
		}
		cancelAttempt()
		adopted, cause := s.adoptAttempt(ctx, result)
		if !adopted {
			// The cancelled or superseded attempt is retired rather than adopted, so
			// it cannot drive the state of a newer attempt and a cancelled run never
			// publishes a retry cadence, renders ConnectivityRetryWait, or notifies a
			// connectivity failure for it.
			result.retire()
			if cause != nil {
				s.transition(supervisorEvent{kind: supervisorTerminal, err: cause})
				return cause
			}
			continue
		}

		if result.err != nil {
			if supervisorTerminalFailure(result.err) {
				s.transition(supervisorEvent{kind: supervisorTerminal, err: result.err})
				return result.err
			}
			if !supervisorRetryable(result.err) {
				// Retrying can never fix this. Return to the picker with the
				// visible error and stop reconnecting; the process still owns
				// the terminal until the user exits or it is cancelled.
				s.transition(supervisorEvent{kind: supervisorNonRetryable, err: result.err})
				s.notify(result.err)
				terminalErr := s.awaitTermination(ctx, input)
				s.transition(supervisorEvent{kind: supervisorTerminal, err: terminalErr})
				return terminalErr
			}
			if terminated, termErr := s.retryFailure(ctx, input, supervisorTransientFailure, result.err); terminated {
				return termErr
			}
			continue
		}

		service, sub = result.service, result.sub
		s.readySub = sub
		if s.cfg.Picker != nil {
			// Render the first committed publication immediately, before waiting
			// for the next one; the picker never shows a stale empty catalogue
			// while an established connection already has state.
			s.cfg.Picker.ApplySnapshot(service.Snapshot())
			s.renderCurrent()
		}
		wasReadyLost := s.State().ReadyLost
		s.transition(supervisorEvent{kind: supervisorReady})
		s.refreshPreview(service)
		if wasReadyLost {
			s.notifyLifecycle(LifecycleNoticeBrokerReconnected)
			s.mu.Lock()
			s.state.ReadyLost = false
			s.mu.Unlock()
		}
		if terminated, termErr := s.runInitialNavigation(ctx, input, service); terminated {
			retire()
			s.transition(supervisorEvent{kind: supervisorTerminal, err: termErr})
			return termErr
		}
		// A navigation left armed (the broker was lost while it waited for an
		// observation) keeps Connecting: the next attempt takes it again.
		if !s.initialNavigationPending() {
			s.transition(supervisorEvent{kind: supervisorNavigationSettled})
		}

		// Ready phase: fold publications, admit committed attachments one at a
		// time, and return to the picker after each. A committed attachment
		// never replaces the broker connection; only a broker loss leaves this
		// loop, and cancellation or terminal EOF ends the run.
		var lossErr error
	ready:
		for {
			ready := s.awaitReady(ctx, input, service, sub)
			if ready.terminated {
				retire()
				s.transition(supervisorEvent{kind: supervisorTerminal, err: ready.termErr})
				return ready.termErr
			}
			if ready.commitKey == "" {
				lossErr = ready.lossErr
				break ready
			}
			if terminated, termErr := s.runCommittedAttachment(ctx, input, service, ready.commitKey); terminated {
				retire()
				s.transition(supervisorEvent{kind: supervisorTerminal, err: termErr})
				return termErr
			}
		}
		retire()
		if cause := ctx.Err(); cause != nil {
			// Parent cancellation wins over a loss that settled at the same instant,
			// exactly as it does at the attempt settle. The connection was already
			// released above, so the loss is simply never turned into a retry
			// cadence, a ConnectivityRetryWait render, or a notification for a run
			// that was already cancelled.
			s.transition(supervisorEvent{kind: supervisorTerminal, err: cause})
			return cause
		}
		s.notifyLifecycle(LifecycleNoticeBrokerLost)
		if terminated, termErr := s.retryFailure(ctx, input, supervisorBrokerLoss, normalizeUnavailable(lossErr)); terminated {
			return termErr
		}
	}
}

// supervisorAttempt is the settled outcome of one connect/subscribe/first
// publication attempt. generation is the attempt's generation, claimed before
// the connector ran, so the completion is always attributable to the attempt
// that produced it (see adoptAttempt's generation fence).
type supervisorAttempt struct {
	generation uint64
	service    ports.BrokerService
	sub        ports.BrokerSubscription
	err        error
}

// retire closes whatever the attempt produced. It is safe on a partially built
// or empty attempt, and it treats a typed-nil service or subscription as absent
// rather than calling into it.
func (a supervisorAttempt) retire() {
	if !supervisorNil(a.sub) {
		a.sub.Close()
	}
	if !supervisorNil(a.service) {
		_ = a.service.Close()
	}
}

// adoptAttempt decides whether one settled attempt may be adopted, and reports the
// parent cancellation that must settle the run instead. The decision is made under
// the state lock, atomically with respect to the state it consults, so an attempt
// that settled after the parent cancelled the run is never adopted even though
// awaitAttempt's select may have taken its outcome over an already-ready
// ctx.Done. The generation fence is enforced at the same decision point: a
// superseded attempt is rejected too. It is a defensive invariant, not a
// currently reachable production path, because Run joins each attempt before it
// advances; it is kept so a future driver that admits more than one attempt at a
// time can never let a late completion drive the state of a newer attempt.
func (s *Supervisor) adoptAttempt(ctx context.Context, result supervisorAttempt) (adopted bool, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cause := ctx.Err(); cause != nil {
		return false, cause
	}
	if result.generation != s.state.Generation {
		return false, nil
	}
	return true, nil
}

// retryFailure records one retryable connectivity failure, surfaces it, and
// waits the cadence for the next attempt. It is the shared tail of the transient
// connect/subscribe failure and the established-connection loss paths, so both
// select the same observable transition, notification, and backoff.
func (s *Supervisor) retryFailure(ctx context.Context, input *terminalInputLifetime, kind supervisorEventKind, err error) (bool, error) {
	if cause := ctx.Err(); cause != nil {
		s.transition(supervisorEvent{kind: supervisorTerminal, err: cause})
		return true, cause
	}
	s.transition(supervisorEvent{kind: kind, err: err})
	s.notify(err)
	if terminated, termErr := s.waitBackoff(ctx, input); terminated {
		s.transition(supervisorEvent{kind: supervisorTerminal, err: termErr})
		return true, termErr
	}
	return false, nil
}

// connectAttempt connects once, subscribes, and waits for the first committed
// publication. It returns a live service and subscription only when the
// connection is Ready; every failure closes whatever it opened before
// returning, so a caller has nothing to clean up on the error path. Every
// connect, subscribe, or first-publication failure is stamped with generation
// and normalized to a typed unavailable failure so the run loop never observes
// an untyped cause.
func (s *Supervisor) connectAttempt(ctx context.Context, generation uint64) supervisorAttempt {
	s.logger.Debug("client_broker_connect_begin", "generation", generation)
	service, err := s.cfg.Connector.Connect(ctx)
	if err != nil {
		failure := normalizeUnavailable(err)
		s.logger.Warn("client_broker_connect_failed", "generation", generation, "code", brokerErrorCode(failure), "error", failure, "cause", errors.Unwrap(failure))
		return supervisorAttempt{generation: generation, err: failure}
	}
	if supervisorNil(service) {
		return supervisorAttempt{generation: generation, err: ports.BrokerError{
			Code: ports.BrokerErrorUnavailable,
			Text: "broker connector returned no service",
		}}
	}
	sub, err := service.Subscribe()
	if err != nil {
		_ = service.Close()
		failure := normalizeUnavailable(err)
		s.logger.Warn("client_broker_subscribe_failed", "generation", generation, "code", brokerErrorCode(failure), "error", failure, "cause", errors.Unwrap(failure))
		return supervisorAttempt{generation: generation, err: failure}
	}
	if supervisorNil(sub) {
		// A service that reports no subscription cannot ever become Ready, so it
		// is a typed unavailable failure rather than a nil dereference on the
		// first Changed() call.
		_ = service.Close()
		return supervisorAttempt{generation: generation, err: ports.BrokerError{
			Code: ports.BrokerErrorUnavailable,
			Text: "broker service returned no subscription",
		}}
	}
	for {
		if service.Snapshot().Epoch != 0 {
			return supervisorAttempt{generation: generation, service: service, sub: sub}
		}
		select {
		case <-sub.Changed():
			continue
		case <-service.Done():
			cause := normalizeUnavailable(service.Err())
			s.logger.Warn("client_broker_initial_publication_lost", "generation", generation, "code", brokerErrorCode(cause), "error", cause, "cause", errors.Unwrap(cause))
			sub.Close()
			_ = service.Close()
			return supervisorAttempt{generation: generation, err: cause}
		case <-ctx.Done():
			sub.Close()
			_ = service.Close()
			return supervisorAttempt{generation: generation, err: ctx.Err()}
		}
	}
}

// awaitAttempt waits for the in-flight attempt to settle, or for the run to be
// terminated first.
func (s *Supervisor) awaitAttempt(ctx context.Context, input *terminalInputLifetime, outcome <-chan supervisorAttempt) (supervisorAttempt, bool, error) {
	for {
		select {
		case result := <-outcome:
			return result, false, nil
		case <-s.pickerOps():
			if s.takePickerClose() {
				return supervisorAttempt{}, true, nil
			}
		case <-ctx.Done():
			return supervisorAttempt{}, true, ctx.Err()
		case err := <-input.EOF():
			return supervisorAttempt{}, true, terminalReadCause(err)
		case <-s.presentationInvalidation():
			s.renderResizeInvalidation()
		case <-s.spinnerTick():
			s.cfg.Spinner.AdvanceSpinner(s.State())
		}
	}
}

// readyOutcome is the result of one wait inside the established-connection
// phase: either the broker connection was lost, the run was terminated, or a
// committed picker selection is ready to be attached.
type readyOutcome struct {
	// commitKey is the exact catalogue key captured with a commit decision. A
	// non-empty value names one attachment to admit; the ready loop never
	// resolves anything else.
	commitKey string
	// lossErr is the broker service's stable terminal cause when the
	// established connection was lost.
	lossErr error
	// terminated reports that the run itself must end, with termErr carrying
	// the process cancellation or terminal-read cause.
	terminated bool
	termErr    error
}

// awaitReady waits for an established connection to be lost, for a committed
// picker selection, or for the run to be terminated first. Publications are
// folded into the picker as they arrive; a commit decision wakes the loop so
// the supervisor can admit exactly the row the user committed. It opens no
// stream and starts no terminal work itself.
func (s *Supervisor) awaitReady(ctx context.Context, input *terminalInputLifetime, service ports.BrokerService, sub ports.BrokerSubscription) readyOutcome {
	if key := s.pendingPickerKey; key != "" {
		s.pendingPickerKey = ""
		return readyOutcome{commitKey: key}
	}
	var changed <-chan struct{}
	var ops <-chan struct{}
	if s.cfg.Picker != nil {
		if !supervisorNil(sub) {
			changed = sub.Changed()
		}
		ops = s.cfg.Picker.OpsReady()
	}
	notices := noticeWake{clock: s.cfg.Clock}
	defer notices.stop()
	for {
		select {
		case <-notices.arm(s.cfg.Picker):
			s.renderCurrent()
		case <-service.Done():
			return readyOutcome{lossErr: service.Err()}
		case <-changed:
			s.cfg.Picker.ApplySnapshot(service.Snapshot())
			s.refreshPreview(service)
			s.renderCurrent()
		case <-s.preview.changed():
			if s.preview.publish(s.cfg.Picker) {
				s.renderCurrent()
			}
		case outcome := <-s.kills.results():
			s.finishPickerKill(service, outcome)
		case <-ops:
			op, key := s.cfg.Picker.TakeOp()
			if op.kill && !op.commit && !op.close {
				s.startPickerKill(service, key)
				s.renderCurrent()
				continue
			}
			if !op.close && !op.commit {
				s.refreshPreview(service)
				s.renderCurrent()
				continue
			}
			if op.close {
				if op.exit {
					// Ctrl+C is the explicit exit. With no attachment to
					// return to, it is the only close that ends the process.
					s.preview.close(s.cfg.Picker)
					return readyOutcome{terminated: true}
				}
				// Escape and q cancel; with no attachment there is nothing to
				// cancel back to, so the picker stays and says how to leave.
				s.notifyPicker(pickerExitHint)
				s.renderCurrent()
				continue
			}
			if op.commit && key != "" {
				s.preview.close(s.cfg.Picker)
				return readyOutcome{commitKey: key}
			}
		case <-ctx.Done():
			return readyOutcome{terminated: true, termErr: ctx.Err()}
		case err := <-input.EOF():
			return readyOutcome{terminated: true, termErr: terminalReadCause(err)}
		case <-s.presentationInvalidation():
			s.refreshPreview(service)
			s.renderResizeInvalidation()
		}
	}
}

func (s *Supervisor) refreshPreview(service ports.BrokerService) {
	if s == nil || s.cfg.Picker == nil {
		return
	}
	geometry, err := s.cfg.Terminal.Geometry()
	if err != nil || !geometry.Valid() {
		s.preview.close(s.cfg.Picker)
		return
	}
	s.preview.refresh(service, s.cfg.Picker, geometry.Size)
}

// pickerOps is nil when no picker owns presentation decisions.
func (s *Supervisor) pickerOps() <-chan struct{} {
	if s.cfg.Picker == nil {
		return nil
	}
	return s.cfg.Picker.OpsReady()
}

// takePickerClose honors the explicit exit while offline without losing a
// racing commit. A plain cancel (Escape, q) has no attachment to return to and
// never ends the process.
func (s *Supervisor) takePickerClose() bool {
	op, key := s.cfg.Picker.TakeOp()
	if op.commit && key != "" {
		s.pendingPickerKey = key
	}
	if op.close && !op.exit {
		s.notifyPicker(pickerExitHint)
		s.renderCurrent()
	}
	if op.kill && !op.commit && !op.close && key != "" {
		// A kill needs the broker's control stream; it is refused, not queued.
		s.notifyPicker(pickerKillRefused("couldn't kill: broker unavailable"))
		s.renderCurrent()
	}
	return op.close && op.exit
}

// pickerPresentationHost is the optional presentation surface of the real
// picker. Scripted pickers may omit it.
type pickerPresentationHost interface {
	Notify(n domain.Notification)
	invalidatePresentation()
}

// notifyPicker shows one bounded client-local notice when the picker
// supports it.
func (s *Supervisor) notifyPicker(n domain.Notification) {
	if host, ok := s.cfg.Picker.(pickerPresentationHost); ok {
		host.Notify(n)
	}
}

// noticeDeadliner reports when the picker's notice stack next changes.
type noticeDeadliner interface {
	noticeDeadline() (time.Time, bool)
}

// noticeWake wakes the ready loop when a picker notice expires, so the stack
// advances and freed cells are blanked without waiting for other input.
type noticeWake struct {
	clock ports.Clock
	timer ports.Timer
	due   time.Time
}

// arm returns a channel that fires at the next notice deadline, or nil when
// no notice will change on its own.
func (w *noticeWake) arm(picker pickerHost) <-chan time.Time {
	host, ok := picker.(noticeDeadliner)
	if !ok || supervisorNil(w.clock) {
		return nil
	}
	due, ok := host.noticeDeadline()
	if !ok {
		w.stop()
		return nil
	}
	if w.timer == nil || !due.Equal(w.due) {
		w.stop()
		w.timer = w.clock.NewTimer(max(due.Sub(w.clock.Now()), 0))
		w.due = due
	}
	return w.timer.C()
}

func (w *noticeWake) stop() {
	stopSupervisorTimer(w.timer)
	w.timer = nil
	w.due = time.Time{}
}

// pickerExitHint tells an unattached user how to leave the picker.
var pickerExitHint = domain.Notification{Code: domain.NoticeUser, Severity: domain.NoticeInfo, Message: "no session attached: press Ctrl+C to quit"}

// invalidatePickerPresentation forces the next picker frame to redraw the
// whole box after another owner wrote the terminal.
func (s *Supervisor) invalidatePickerPresentation() {
	if host, ok := s.cfg.Picker.(pickerPresentationHost); ok {
		host.invalidatePresentation()
	}
}

// awaitTermination parks the supervisor on the picker after a non-retryable
// failure until the terminal ends or the process is cancelled. A clean terminal
// EOF returns nil; every other cause is reported.
func (s *Supervisor) awaitTermination(ctx context.Context, input *terminalInputLifetime) error {
	for {
		select {
		case <-s.pickerOps():
			if s.takePickerClose() {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		case err := <-input.EOF():
			return terminalReadCause(err)
		case <-s.presentationInvalidation():
			s.renderResizeInvalidation()
		}
	}
}

// waitBackoff waits one equal-jitter retry interval for the current failure
// count, or settles on cancellation or terminal EOF. The timer is always
// stopped and drained, so no timer outlives the attempt transition.
func (s *Supervisor) waitBackoff(ctx context.Context, input *terminalInputLifetime) (bool, error) {
	delay := supervisorBackoffDelay(s.State().Attempt, s.cfg.Jitter)
	if delay <= 0 {
		return false, nil
	}
	timer := s.cfg.Clock.NewTimer(delay)
	defer stopSupervisorTimer(timer)
	for {
		select {
		case <-s.pickerOps():
			if s.takePickerClose() {
				return true, nil
			}
		case <-timer.C():
			return false, nil
		case <-ctx.Done():
			return true, ctx.Err()
		case err := <-input.EOF():
			return true, terminalReadCause(err)
		case <-s.presentationInvalidation():
			s.renderResizeInvalidation()
		case <-s.spinnerTick():
			s.cfg.Spinner.AdvanceSpinner(s.State())
		}
	}
}

// supervisorBackoffDelay is the equal-jitter exponential interval for one
// failure count: the cap doubles from supervisorRetryInitial to
// supervisorRetryMax, and the interval is half the cap plus a uniform fraction
// of the other half, so attempts spread across [cap/2, cap). It is pure so the
// cadence can be tested exactly.
func supervisorBackoffDelay(attempt uint64, jitter func() float64) time.Duration {
	if attempt == 0 {
		return 0
	}
	limit := supervisorRetryInitial
	for i := uint64(1); i < attempt && limit < supervisorRetryMax; i++ {
		limit *= 2
	}
	if limit > supervisorRetryMax {
		limit = supervisorRetryMax
	}
	half := limit / 2
	fraction := jitter()
	if !(fraction >= 0) {
		// NaN is unordered: both fraction < 0 and fraction > 1 are false for
		// it, so an explicit non-negative test maps NaN and every negative value
		// to the minimum fraction. Without this, a NaN would reach the duration
		// conversion and yield an undefined interval instead of the documented
		// half-cap minimum.
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	return half + time.Duration(fraction*float64(half))
}

// terminalReadCause maps a terminal read end to the Run result: a clean EOF is
// an orderly exit (nil), any other read failure is reported.
func terminalReadCause(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// stopSupervisorTimer stops and drains a timer, supporting buffered timer
// adapters with pre-Go-1.23 reset semantics.
func stopSupervisorTimer(timer ports.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
}

// supervisorTerminalFailure reports whether a broker failure must end the
// process. Only the domain's terminal codes do; every other failure returns the
// client to the picker.
func supervisorTerminalFailure(err error) bool {
	var typed ports.BrokerError
	return errors.As(err, &typed) && typed.Terminal()
}

// supervisorRetryable reports whether a failure is worth another attempt after
// backoff. An incompatible carriage or a conflicting policy can never succeed
// by retrying, so it is surfaced once and automatic reconnection stops; every
// other failure is treated as transient.
func supervisorRetryable(err error) bool {
	var typed ports.BrokerError
	if errors.As(err, &typed) {
		switch typed.Code {
		case ports.BrokerErrorIncompatible, ports.BrokerErrorConflictingPolicy:
			return false
		}
	}
	return true
}

// normalizeUnavailable presents one connect, subscribe, or connection-loss
// failure as a typed ports.BrokerError. A cause that is already a BrokerError is
// returned unchanged, so its own code (for example incompatible or conflicting
// policy) still selects the non-retryable path; every untyped failure, including
// a nil cause, becomes an unavailable BrokerError that preserves the original
// error as its local-only Cause.
func brokerErrorCode(err error) string {
	var typed ports.BrokerError
	if errors.As(err, &typed) {
		return typed.Code.String()
	}
	return "unknown"
}

func normalizeUnavailable(err error) error {
	if err == nil {
		return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker connection lost"}
	}
	var typed ports.BrokerError
	if errors.As(err, &typed) {
		return err
	}
	var admission ports.BrokerAdmissionError
	if errors.As(err, &admission) {
		return err
	}
	return ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker connection lost", Cause: err}
}

// notify surfaces a connectivity failure through the optional notifier.
func (s *Supervisor) notifyLifecycle(kind LifecycleNoticeKind) {
	if s != nil {
		s.logger.Debug("client_lifecycle_notice", "kind", kind)
	}
	if s != nil && s.cfg.NotifyLifecycle != nil {
		s.cfg.NotifyLifecycle(LifecycleNotice{Kind: kind})
	}
}

func (s *Supervisor) notify(err error) {
	if err != nil {
		s.logger.Warn("client_connectivity_failure", "state", s.State().Connectivity.String(), "code", brokerErrorCode(err), "error", err, "cause", errors.Unwrap(err))
	}
	if s.cfg.Notify == nil || err == nil {
		return
	}
	s.cfg.Notify(s.State(), err)
}

// transition applies one reducer event under the supervisor mutex and renders
// the resulting state outside it, so a renderer that reads State back never
// deadlocks.
//
// A presentation change also publishes the new presentation through the UI
// owner inside the same fence, so a capture observes the presentation the
// supervisor actually entered instead of an older attachment context. The
// publication is deliberately limited to the unattached presentations the
// supervisor owns: Picker and Connecting carry the run's handle and status with
// every session field zeroed, while Attached is published by the admitted
// attachment foreground only, after its first frame was written, flushed, and
// committed, carrying the real action generation. Terminating closes the UI
// service and publishes nothing.
func (s *Supervisor) transition(event supervisorEvent) {
	s.mu.Lock()
	previous := s.state.Presentation
	s.state = reduceSupervisor(s.state, event)
	state := s.state
	s.mu.Unlock()
	if state.Presentation != previous || event.kind == supervisorBeginAttempt || event.kind == supervisorTransientFailure || event.kind == supervisorBrokerLoss {
		s.logger.Debug("client_state_transition", "from", previous.String(), "to", state.Presentation.String(), "connectivity", state.Connectivity.String(), "attempt", state.Attempt, "generation", state.Generation, "error", event.err, "cause", errors.Unwrap(event.err))
	}
	if state.Presentation != previous {
		s.publishPresentation(state.Presentation)
	}
	if s.cfg.Spinner != nil && state.Presentation != PresentConnecting && state.Connectivity != ConnectivityRetryWait && state.Connectivity != ConnectivityConnectingBroker {
		s.cfg.Spinner.StopSpinner()
	}
	if s.cfg.Render != nil {
		s.cfg.Render(state)
	}
}

func (s *Supervisor) spinnerTick() <-chan time.Time {
	if s.cfg.Spinner == nil {
		return nil
	}
	return s.cfg.Spinner.Spinner()
}

// publishPresentation publishes one supervisor-owned presentation through the
// optional UI owner. Attached and terminating are refused: Attached belongs to
// the admitted attachment foreground, and terminating ends the service.
func (s *Supervisor) publishPresentation(presentation Presentation) {
	if s == nil || s.cfg.UI == nil {
		return
	}
	switch presentation {
	case PresentPicker:
		_ = s.cfg.UI.publishPresentation(ports.UIStatusPicker)
	case PresentConnecting:
		_ = s.cfg.UI.publishPresentation(ports.UIStatusConnecting)
	}
}

// renderCurrent repaints presentation data that changed without a supervisor
// state transition, notably a picker catalogue publication.
func (s *Supervisor) renderCurrent() {
	if s == nil || s.cfg.Render == nil {
		return
	}
	s.cfg.Render(s.State())
}

// presentationInvalidation returns the coalesced resize invalidation the
// supervisor's serialized waits select on, or nil when there is no attachment
// host to collect resizes. A nil signal is simply never ready, so a supervisor
// with no host (the zero value, or one whose host was detached) parks on its
// remaining arms instead of dereferencing a nil host. Selecting on a nil
// channel is safe in both directions. The returned channel is receive-only: the
// supervisor only ever consumes an invalidation, and the attachment settlement
// wait deliberately does not select on it at all, so an invalidation raised
// while a foreground is admitted stays buffered for the next picker state.
func (s *Supervisor) presentationInvalidation() <-chan struct{} {
	if s == nil || s.attachments == nil {
		return nil
	}
	return s.attachments.presentationUpdate
}

// renderResizeInvalidation runs only on the supervisor's serialized control
// path, so resize collection cannot write the terminal and an attached
// foreground is never painted over. The composition's renderer is invoked only
// for a presentation PickerPresentation admits, which is exactly PresentPicker;
// it samples Terminal.Geometry at render time. A pre-attachment connecting
// presentation is refused here for the same reason as an attached one: the
// admitted foreground already owns the same terminal writer.
func (s *Supervisor) renderResizeInvalidation() {
	if s == nil || s.cfg.Render == nil {
		return
	}
	state := s.State()
	if PickerPresentation(state) {
		s.cfg.Render(state)
	}
}

// terminalInputLifetime owns the single read of the controlling terminal. It is
// started once and never replaced: a bare io.Reader cannot be interrupted, so
// the goroutine may outlive Run when stdin never reaches EOF, exactly like the
// attach client's input pump. It never closes caller-owned input. When a picker
// consumer is supplied each read is handed to it from this one
// reader, so the picker never starts a second reader; without a consumer the
// bytes are discarded when no consumer is supplied.
type terminalInputLifetime struct {
	eof      chan error
	once     sync.Once
	consumer pickerInputConsumer
	pump     *terminalInputPump

	mu       sync.Mutex
	pickerID uint64
}

// startTerminalInputLifetime starts the single terminal read. A nil reader is
// treated as an immediate orderly EOF. A nil consumer discards read bytes; a
// non-nil consumer owns them for the picker presentation.
func startTerminalInputLifetime(in io.Reader, consumer pickerInputConsumer) *terminalInputLifetime {
	return startTerminalInputLifetimeWith(in, consumer, nil)
}

// startTerminalInputLifetimeWith runs beforeConsumers on the started pump
// before the picker may claim it, so a terminal probe sees its responses
// first and hands every other byte on to the picker.
func startTerminalInputLifetimeWith(in io.Reader, consumer pickerInputConsumer, beforeConsumers func(*terminalInputPump)) *terminalInputLifetime {
	lifetime := &terminalInputLifetime{eof: make(chan error, 1), consumer: consumer}
	if supervisorNil(in) {
		lifetime.finish(io.EOF)
		return lifetime
	}
	lifetime.pump = newTerminalInputPump(in)
	lifetime.pump.start()
	if beforeConsumers != nil {
		beforeConsumers(lifetime.pump)
	}
	if consumer != nil {
		lifetime.acquirePicker()
	}
	go lifetime.runPicker()
	go func() {
		<-lifetime.pump.exited
		// Publish the cause the reader actually ended with. The reader enqueues
		// its final result for the consumer, but this watcher can win that race,
		// and reporting a read failure as an orderly EOF would return a clean
		// status for a terminal that failed.
		lifetime.finish(lifetime.pump.cause())
	}()
	return lifetime
}

func (l *terminalInputLifetime) acquirePicker() {
	if l == nil || l.pump == nil || l.consumer == nil {
		return
	}
	l.mu.Lock()
	if l.pickerID != 0 {
		l.mu.Unlock()
		return
	}
	if id, ok := l.pump.tryClaim(); ok {
		l.pickerID = id
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	// A finalizing attachment still owns the claim. Retry when that claim is
	// actually released rather than permanently abandoning picker input.
	go func() {
		select {
		case <-l.pump.claimChanged:
			l.acquirePicker()
		case <-l.pump.done:
		}
	}()
}

func (l *terminalInputLifetime) releasePicker() {
	if l == nil || l.pump == nil {
		return
	}
	l.mu.Lock()
	id := l.pickerID
	l.pickerID = 0
	l.mu.Unlock()
	if id != 0 {
		l.pump.ack(id)
		// Drop picker-owned bytes that were queued but never presented before
		// changing owner class; they are not session input.
		l.pump.dropOwned(id)
		l.pump.revoke(id)
	}
}

func (l *terminalInputLifetime) runPicker() {
	if l == nil || l.pump == nil {
		return
	}
	for {
		l.mu.Lock()
		id := l.pickerID
		l.mu.Unlock()
		if id == 0 {
			select {
			case <-l.pump.claimChanged:
			case <-l.pump.done:
				return
			}
			continue
		}
		result, ok := l.pump.take(context.Background(), id)
		if !ok {
			select {
			case <-l.pump.readyFor(id):
			case <-l.pump.claimChanged:
			case <-l.pump.done:
				return
			}
			continue
		}
		if len(result.data) != 0 {
			l.consumer.ConsumeTerminalRead(result.data)
		}
		l.pump.ack(id)
		if result.err != nil {
			l.finish(result.err)
			return
		}
	}
}

func (l *terminalInputLifetime) stop() {
	if l == nil || l.pump == nil {
		return
	}
	l.pump.stop()
	l.pump.suspend()
}

// finish publishes the terminal cause exactly once.
func (l *terminalInputLifetime) finish(err error) {
	l.once.Do(func() { l.eof <- err })
}

// EOF is closed-result channel delivering the terminal read cause exactly once.
func (l *terminalInputLifetime) EOF() <-chan error { return l.eof }

// supervisorNil reports whether a dependency is nil in any shape. A typed nil
// satisfies a plain != nil comparison, so the guard would defer the failure to
// the first method call, which panics; the reflection check keeps it total.
func supervisorNil(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// detectTerminalCapabilities probes the outer terminal once and enables the
// kitty keyboard protocol when it is supported and wanted. It runs in Run
// before any input consumer or attachment exists.
func (s *Supervisor) detectTerminalCapabilities(ctx context.Context, pump *terminalInputPump) {
	env := s.cfg.AttachmentEnvironment
	if !env.ProbeTerminal {
		return
	}
	caps := probeTerminalCapabilities(ctx, s.cfg.Terminal, s.cfg.Clock, pump)
	if caps.KittyKeyboard && env.KittyKeyboard {
		if err := s.cfg.Terminal.EnableKittyKeyboard(kittykey.OuterFlags); err != nil {
			s.logger.Warn("enabling kitty keyboard protocol failed", "err", err)
			caps.KittyKeyboard = false
		}
	} else {
		caps.KittyKeyboard = false
	}
	s.capabilities = caps
}
