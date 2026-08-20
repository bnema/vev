// Package client implements vev's thin attach client: it speaks the framed
// IPC protocol to a daemon, pumping terminal input, resize events, and PTY
// output between the controlling terminal and the session.
//
// The package depends only on ports (transport, terminal, wire codecs) and
// domain, never on a concrete transport or terminal implementation; the app
// layer wires those in.
package client

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev-vt/protocol/terminalquery"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/theme"
)

// ProtocolError is a session- or protocol-level failure reported by the
// daemon before attach could proceed (a Hello rejected with an ErrorMsg).
// The app layer prints it to the user.
type ProtocolError struct {
	Code uint16
	Text string
}

func (e *ProtocolError) Error() string {
	if e.Text == "" {
		return fmt.Sprintf("vev: daemon rejected attach (code %d)", e.Code)
	}
	return fmt.Sprintf("vev: %s", e.Text)
}

// DetachedError reports that the daemon detached this client for a reason
// other than an ordinary user-initiated detach.
type DetachedError struct {
	Reason uint8
	Text   string
}

func (e *DetachedError) Error() string { return "vev: " + e.Text }

// terminalThemeState retains the latest terminal-reported colors across
// transport attempts so a replacement attachment can restore the daemon theme
// without relying on another OSC response.
type terminalThemeState struct {
	mu    sync.Mutex
	theme ports.Theme
}

func (s *terminalThemeState) setTrueColor(enabled bool) {
	s.mu.Lock()
	s.theme.TrueColor = enabled
	s.mu.Unlock()
}

func (s *terminalThemeState) update(update func(*ports.Theme)) ports.Theme {
	s.mu.Lock()
	defer s.mu.Unlock()
	update(&s.theme)
	return s.theme
}

func (s *terminalThemeState) snapshot() ports.Theme {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.theme
}

// milestones tracks lifecycle progress so a failure can report which stages
// were never reached — the key diagnostic when an attach dies early.
type milestones struct {
	dialed      bool
	helloSent   bool
	welcomed    bool
	rawEntered  bool
	firstOutput bool
	detached    bool
}

func (m milestones) missing() []string {
	var out []string
	if !m.dialed {
		out = append(out, "dialed")
	}
	if !m.helloSent {
		out = append(out, "hello_sent")
	}
	if !m.welcomed {
		out = append(out, "welcomed")
	}
	if !m.rawEntered {
		out = append(out, "raw_entered")
	}
	if !m.firstOutput {
		out = append(out, "first_output")
	}
	if !m.detached {
		out = append(out, "detached")
	}
	return out
}

// processClientID identifies this client process across links. It is kept in
// memory only; a fresh process gets a fresh ID.
var processClientID = newClientID()

func newClientID() [16]byte {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic("crypto/rand failed generating client ID: " + err.Error())
	}
	return id
}

// stdinBufSize is the read buffer for the terminal input pump.
const stdinBufSize = 4096

// sendQueueDepth bounds the buffered frames awaiting the single sender
// goroutine, decoupling the input/resize pumps from transport back-pressure.
const sendQueueDepth = 64

// preWelcomeTimeout remains the focused-test name for the shared handshake
// budget, which now also covers connect and initial publication.
const preWelcomeTimeout = ports.HandshakeTimeout

// remoteHostLearnerShutdownTimeout bounds how long Run waits for async remote
// host learning after terminal restoration. Learning is best-effort; a stalled
// learner must not block shell prompt return indefinitely.
const remoteHostLearnerShutdownTimeout = time.Second

const kittyCapabilityProbeTimeout = 150 * time.Millisecond

var reconnectSleep = sleepReconnect
var reconnectSleepWithResize = sleepReconnectWithResizeEvents

const (
	statusReconnect = "\r\x1b[2Kreconnecting…"
	statusClear     = "\r\x1b[2K"
)

// Dependencies supplies the collaborators required by a Runner.
type AttachHandoffFunc func(ports.AttachTarget) (ports.Dialer, AttachRequest, error)

type Dependencies struct {
	Dialer            ports.Dialer
	Terminal          ports.Terminal
	Clock             ports.Clock
	Clipboard         ports.ClipboardReader
	Logger            *slog.Logger
	RuntimeObserver   ports.SerializedRuntimeObserver
	RemoteHostLearner ports.RemoteHostLearner
	// AttachHandoff keeps one Runner and one terminal/input ownership across a
	// local daemon's structured remote handoff. It is nil for direct CLI attach.
	AttachHandoff AttachHandoffFunc
	// Remote selects client-side carriage presentation only; it never enters
	// the daemon-facing session request.
	Remote bool
	// Origin is explicit composition metadata for the initial route. A zero
	// value is normalized to local for direct package callers and old tests.
	Origin    ports.RouteOrigin
	OriginKey string
}

// AttachRequest identifies the session for one client run. Remote is
// client-only presentation metadata. RemoteTarget is an optional exact picker
// handoff and is serialized into Hello only when present.
type AttachRequest struct {
	Intent                 uint8
	SessionName            string
	Remote                 bool
	Origin                 ports.RouteOrigin
	OriginKey              string
	RemoteTarget           *domain.RemoteSessionTarget
	ExactTarget            *ports.ExactSessionTarget
	PreferredTabID         domain.TabStableID
	HostLabel              string
	EnvironmentPolicy      ports.EnvironmentPolicy
	NavigationCapabilities ports.NavigationCapabilities
	StartupOverlay         ports.StartupOverlay
}

// attachRoute captures the dialer, request, and resume token needed to
// restore a previous attachment route after a navigation handoff.
type attachRoute struct {
	dialer      ports.Dialer
	request     AttachRequest
	resumeToken uint64
}

type Runner struct {
	dialer            ports.Dialer
	term              ports.Terminal
	clock             ports.Clock
	clipboard         ports.ClipboardReader
	logger            *slog.Logger
	runtimeObserver   ports.SerializedRuntimeObserver
	remoteHostLearner ports.RemoteHostLearner
	attachHandoff     AttachHandoffFunc
	remote            bool
	origin            ports.RouteOrigin
	ledger            *routeLedger
	routeFailure      *ports.RouteNavigationFailure

	inputMu sync.Mutex
	input   *terminalInputPump

	capabilityMu        sync.Mutex
	kittyDirectGraphics *bool
	probeCapabilities   bool
}

// NewRunner constructs a client runner. A nil logger uses the process default.
func NewRunner(deps Dependencies) *Runner {
	log := deps.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Runner{
		dialer:            deps.Dialer,
		term:              deps.Terminal,
		clock:             deps.Clock,
		clipboard:         deps.Clipboard,
		logger:            log,
		runtimeObserver:   deps.RuntimeObserver,
		remoteHostLearner: deps.RemoteHostLearner,
		attachHandoff:     deps.AttachHandoff,
		remote:            deps.Remote,
		origin:            normalizeRouteOrigin(deps.Origin, deps.Remote),
		probeCapabilities: true,
		ledger:            newRouteLedger(),
	}
}

// terminalInput returns the Runner-owned sole reader. It survives sequential
// Run calls because a caller-owned reader may remain blocked after an attach
// exits and therefore cannot safely be replaced by another reader.
func (r *Runner) terminalInput() *terminalInputPump {
	r.inputMu.Lock()
	defer r.inputMu.Unlock()

	if r.input == nil || r.input.hasExited() || r.input.isClosed() {
		r.input = newTerminalInputPump(r.term.In())
		r.input.start()
	}
	r.input.resume()
	return r.input
}

func (r *Runner) kittyProbeEnabled() bool {
	if r == nil || !r.probeCapabilities {
		return false
	}
	term := strings.ToLower(strings.TrimSpace(os.Getenv("TERM")))
	return term == "xterm-kitty"
}

// probeKittyDirectGraphics performs one bounded probe for the outer terminal.
// It claims the existing lifecycle input pump instead of creating a second
// reader, removes only its two responses, and requeues unrelated bytes for the
// attach scanner in their original order.
func (r *Runner) probeKittyDirectGraphics(ctx context.Context, input *terminalInputPump) bool {
	if r == nil || input == nil || !r.kittyProbeEnabled() {
		return false
	}
	r.capabilityMu.Lock()
	if r.kittyDirectGraphics != nil {
		value := *r.kittyDirectGraphics
		r.capabilityMu.Unlock()
		return value
	}
	r.capabilityMu.Unlock()

	consumer := input.claim()
	defer input.revoke(consumer)
	probe := &terminalquery.Probe{}
	var replay []byte
	query := terminalquery.KittyGraphicsQuery + terminalquery.DeviceAttributesQuery
	log := r.logger
	if log == nil {
		log = slog.Default()
	}
	if _, err := r.term.Out().Write([]byte(query)); err != nil {
		log.Warn("kitty graphics probe write failed", "err", err)
		return r.rememberKittyDirectGraphics(false)
	}
	if err := r.term.Flush(); err != nil {
		log.Warn("kitty graphics probe flush failed", "err", err)
		return r.rememberKittyDirectGraphics(false)
	}

	clock := r.clock
	if clock == nil {
		clock = systemClock{}
	}
	timer := clock.NewTimer(kittyCapabilityProbeTimeout)
	defer timer.Stop()
	for !probe.Ready() {
		select {
		case <-ctx.Done():
			replay = append(replay, probe.Finish()...)
			goto done
		case <-timer.C():
			replay = append(replay, probe.Finish()...)
			goto done
		case <-input.ready:
			result, ok := input.take(ctx, consumer)
			if !ok {
				if ctx.Err() != nil {
					replay = append(replay, probe.Finish()...)
					goto done
				}
				continue
			}
			replay = append(replay, probe.Feed(result.data)...)
			input.ack(consumer)
			if result.err != nil {
				replay = append(replay, probe.Finish()...)
				goto done
			}
		}
	}
	replay = append(replay, probe.Finish()...)

done:
	if len(replay) != 0 {
		// The lease is no longer delivering after ack. Preserve before revoke so
		// the replacement attach consumes these bytes before another OS read.
		input.preserveResidual(consumer, replay)
	}
	return r.rememberKittyDirectGraphics(probe.Ready())
}

func (r *Runner) rememberKittyDirectGraphics(value bool) bool {
	r.capabilityMu.Lock()
	if r.kittyDirectGraphics == nil {
		r.kittyDirectGraphics = new(bool)
		*r.kittyDirectGraphics = value
	}
	value = *r.kittyDirectGraphics
	r.capabilityMu.Unlock()
	return value
}

func validateAttachRequest(request AttachRequest) error {
	if request.Origin != 0 {
		if err := request.Origin.Validate(); err != nil {
			return fmt.Errorf("vev: invalid route origin: %w", err)
		}
	}
	if request.OriginKey != "" {
		if err := ports.ValidateRouteLabel(request.OriginKey, false); err != nil {
			return fmt.Errorf("vev: invalid route origin key: %w", err)
		}
	}
	if request.ExactTarget != nil {
		if err := request.ExactTarget.Validate(); err != nil {
			return fmt.Errorf("vev: invalid exact target: %w", err)
		}
		if request.SessionName != request.ExactTarget.SessionName {
			return errors.New("vev: exact target session mismatch")
		}
		if request.RemoteTarget != nil && (request.RemoteTarget.LifecycleID != request.ExactTarget.LifecycleID || request.RemoteTarget.SessionName != request.ExactTarget.SessionName) {
			return errors.New("vev: exact target does not match remote target")
		}
	}
	if err := (SessionTarget{Intent: request.Intent, SessionName: request.SessionName}).validate(); err != nil {
		return fmt.Errorf("vev: invalid session target: %w", err)
	}
	if err := ports.ValidateNavigation(request.NavigationCapabilities, request.StartupOverlay, request.RemoteTarget != nil || request.EnvironmentPolicy == ports.EnvironmentPolicyDaemonOwned); err != nil {
		return fmt.Errorf("vev: invalid navigation route: %w", err)
	}
	if request.RemoteTarget == nil {
		if request.EnvironmentPolicy == ports.EnvironmentPolicyClientOwned {
			return nil
		}
		if request.Remote && request.EnvironmentPolicy == ports.EnvironmentPolicyDaemonOwned {
			return nil
		}
		return errors.New("vev: attach without remote target requires a matching environment policy")
	}
	if err := request.RemoteTarget.Validate(); err != nil {
		return fmt.Errorf("vev: invalid remote attach target: %w", err)
	}
	if request.SessionName != request.RemoteTarget.SessionName {
		return errors.New("vev: remote attach target session mismatch")
	}
	if request.EnvironmentPolicy != ports.EnvironmentPolicyDaemonOwned {
		return errors.New("vev: remote attach target requires daemon-owned environment")
	}
	if request.Intent != ports.IntentAttach && request.Intent != ports.IntentResume {
		return errors.New("vev: remote attach target requires attach or resume")
	}
	return nil
}

// Run connects and runs the attach client. It owns the terminal lifecycle
// above attach attempts so raw mode remains active while a live client process
// redials a lost link.
func (r *Runner) Run(ctx context.Context, request AttachRequest) (retErr error) {
	request = cloneAttachRequest(request)
	if request.Origin == 0 {
		request.Origin = r.origin
	}
	if err := validateAttachRequest(request); err != nil {
		return err
	}
	ms := milestones{}

	defer func() {
		if retErr != nil {
			r.logger.Error("attach ended with error", "err", retErr, "missing_milestones", ms.missing())
		}
	}()

	var restore func() error
	rawEntered := false
	enterRaw := func() error {
		if rawEntered {
			return nil
		}
		var err error
		restore, err = r.term.EnterRaw()
		if err != nil {
			return fmt.Errorf("vev: entering raw mode: %w", err)
		}
		rawEntered = true
		ms.rawEntered = true
		return nil
	}

	resumeToken := uint64(0)
	attemptRequest := request
	dialer := r.dialer
	var homeRoute *attachRoute
	var returnRoute *attachRoute
	returnResumeFallback := false
	homeNavigationPending := false
	returnNavigationPending := false
	routeNavigationPending := false
	routeNavigationResumeFallback := false
	var routeNavigationSelection *routeNavigationSelection
	var routeNavigationFallback *attachRoute
	var routeNavigationAction *ports.RouteNavigationAction
	backoff := defaultReconnectBackoff.initial
	themeState := &terminalThemeState{}
	var rememberOnce sync.Once
	var rememberWG sync.WaitGroup
	var learnerStarted bool
	rememberRemoteHost := func() {
		if r.remoteHostLearner == nil {
			return
		}
		rememberOnce.Do(func() {
			learnerStarted = true
			rememberWG.Go(func() {
				if err := r.remoteHostLearner.RememberRemoteHost(); err != nil {
					r.logger.Warn("remembering remote host failed", "err", err)
				}
			})
		})
	}
	defer func() {
		if !learnerStarted {
			return
		}
		done := make(chan struct{})
		go func() {
			rememberWG.Wait()
			close(done)
		}()
		timer := r.clock.NewTimer(remoteHostLearnerShutdownTimeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C():
			r.logger.Warn("remote host learner stalled past shutdown timeout")
		}
	}()
	defer func() {
		if rawEntered {
			if rerr := restore(); rerr != nil && retErr == nil {
				retErr = fmt.Errorf("vev: restoring terminal: %w", rerr)
			}
		}
	}()
	remote := request.Remote || r.remote
	reconnect := &reconnectUI{
		term:       r.term,
		remote:     remote,
		rawEntered: &rawEntered,
		stage:      reconnectStageOfflineRetrying,
	}
	transition := newTransitionUI(r.term, r.clock, &rawEntered)
	defer transition.stop()
	restoreReturnRoute := func(route attachRoute) {
		returnNavigationPending = true
		dialer = route.dialer
		attemptRequest = route.request
		if route.resumeToken != 0 {
			attemptRequest.Intent = ports.IntentResume
		} else {
			attemptRequest.Intent = ports.IntentAttach
		}
		attemptRequest.StartupOverlay = ports.StartupOverlayNone
		attemptRequest.NavigationCapabilities &^= ports.NavigationCapabilityBack | ports.NavigationCapabilityHomePicker
		resumeToken = route.resumeToken
		returnResumeFallback = route.resumeToken != 0
		remote = syncReconnectRemote(reconnect, attemptRequest.Remote || r.remote)
		backoff = defaultReconnectBackoff.initial
	}
	enterHomePicker := func() {
		dialer = homeRoute.dialer
		attemptRequest = homeRoute.request
		attemptRequest.Intent = ports.IntentAttach
		attemptRequest.NavigationCapabilities = ports.NavigationCapabilityBack
		attemptRequest.StartupOverlay = ports.StartupOverlaySessionPicker
		attemptRequest.RemoteTarget = nil
		attemptRequest.EnvironmentPolicy = ports.EnvironmentPolicyClientOwned
		attemptRequest.Remote = homeRoute.request.Remote || r.remote
		resumeToken = 0
		remote = syncReconnectRemote(reconnect, homeRoute.request.Remote || r.remote)
		backoff = defaultReconnectBackoff.initial
	}
	var input *terminalInputPump
	defer func() {
		if input != nil {
			input.suspend()
		}
	}()
	originalEnterRaw := enterRaw
	enterRaw = func() error {
		if err := originalEnterRaw(); err != nil {
			return err
		}
		if input == nil {
			input = r.terminalInput()
		}
		return nil
	}

	runTransientHomePicker := func(attemptCtx context.Context) attachResult {
		if homeRoute == nil {
			return attachResult{err: errors.New("vev: home route unavailable")}
		}
		request := cloneAttachRequest(homeRoute.request)
		request.Intent = ports.IntentAttach
		request.NavigationCapabilities = ports.NavigationCapabilityBack
		request.StartupOverlay = ports.StartupOverlaySessionPicker
		request.RemoteTarget = nil
		request.EnvironmentPolicy = ports.EnvironmentPolicyClientOwned
		request.Remote = false
		if err := validateAttachRequest(request); err != nil {
			return attachResult{err: err}
		}
		handshakeCtx, timedOut, finishHandshake := newHandshakeContext(attemptCtx, r.clock)
		transport, err := boundedDial(handshakeCtx, homeRoute.dialer)
		if err != nil {
			err = handshakeContextError(attemptCtx, timedOut, err)
			finishHandshake()
			return attachResult{err: err}
		}
		connection, err := NewSessionConnection(transport, SessionTarget{Intent: request.Intent, SessionName: request.SessionName})
		if err != nil {
			finishHandshake()
			_ = transport.Close()
			return attachResult{err: err}
		}
		stopHandshakeTransport := watchHandshakeTransport(handshakeCtx, transport)
		localReconnect := &reconnectUI{term: r.term, rawEntered: &rawEntered}
		result := (&attachAttempt{
			runner: r, dialer: homeRoute.dialer, connection: connection,
			handshakeCtx: handshakeCtx, handshakeTimedOut: timedOut,
			finishHandshake: finishHandshake, stopHandshakeTransport: stopHandshakeTransport,
			request: request, clientID: processClientID, milestones: &ms,
			themeState: themeState, enterRaw: enterRaw, reconnect: localReconnect,
			terminalInput:   func() *terminalInputPump { return input },
			transientPicker: true, returnAttachTargets: true,
		}).run(attemptCtx)
		stopHandshakeTransport()
		finishHandshake()
		if !result.transportClosed {
			_ = connection.Close()
		}
		return result
	}

	for {
		if err := validateAttachRequest(attemptRequest); err != nil {
			return err
		}
		handshakeCtx, timedOut, finishHandshake := newHandshakeContext(ctx, r.clock)
		transport, err := boundedDialWithTransition(handshakeCtx, dialer, transition)
		if err != nil {
			err = handshakeContextError(ctx, timedOut, err)
			finishHandshake()
			if resumeToken == 0 || ctx.Err() != nil {
				if clearErr := reconnect.clear(); clearErr != nil {
					return errors.Join(err, clearErr)
				}
				return err
			}
			r.logger.Warn("reconnect dial failed", "err", err, "backoff", backoff)
			if drawErr := reconnect.draw(); drawErr != nil {
				return errors.Join(err, drawErr)
			}
			slept, sleepErr := reconnect.sleep(ctx, r.clock, backoff)
			if sleepErr != nil {
				return errors.Join(err, sleepErr)
			}
			if !slept {
				if clearErr := reconnect.clear(); clearErr != nil {
					return errors.Join(ctx.Err(), clearErr)
				}
				return ctx.Err()
			}
			backoff = nextReconnectBackoff(backoff, defaultReconnectBackoff.max)
			attemptRequest.Intent = ports.IntentResume
			continue
		}
		ms.dialed = true
		if transport == nil {
			finishHandshake()
			return errors.New("vev: dialer returned nil transport")
		}
		stopHandshakeTransport := watchHandshakeTransport(handshakeCtx, transport)
		connection, err := NewSessionConnection(transport, SessionTarget{
			Intent:      attemptRequest.Intent,
			SessionName: attemptRequest.SessionName,
		})
		if err != nil {
			stopHandshakeTransport()
			finishHandshake()
			_ = transport.Close()
			return err
		}

		var linkEvents <-chan ports.LinkEvent
		if reporter, ok := connection.Transport().(ports.LinkStateReporter); ok {
			linkEvents = reporter.LinkEvents()
		}
		result := (&attachAttempt{
			runner:                   r,
			dialer:                   dialer,
			connection:               connection,
			remote:                   remote,
			handshakeCtx:             handshakeCtx,
			handshakeTimedOut:        timedOut,
			finishHandshake:          finishHandshake,
			stopHandshakeTransport:   stopHandshakeTransport,
			request:                  attemptRequest,
			resumeToken:              resumeToken,
			routeNavigationSelection: routeNavigationSelection,
			clientID:                 processClientID,
			milestones:               &ms,
			themeState:               themeState,
			enterRaw:                 enterRaw,
			reconnect:                reconnect,
			transition:               transition,
			linkEvents:               linkEvents,
			rememberRemoteHost:       rememberRemoteHost,
			terminalInput: func() *terminalInputPump {
				return input
			},
			openHomePicker: runTransientHomePicker,
		}).run(ctx)
		stopHandshakeTransport()
		finishHandshake()
		if result.welcomed {
			backoff = defaultReconnectBackoff.initial
		}
		if !result.transportClosed {
			_ = connection.Close()
		}
		// Apply metadata from every processed frame before dispatching its
		// navigation action. A committed identity can arrive immediately before
		// a home/back action and must become the route request's authority first.
		if result.resumeToken != 0 {
			resumeToken = result.resumeToken
		}
		if result.sessionName != "" {
			attemptRequest.SessionName = result.sessionName
		}
		if result.committedIdentity != nil {
			identity := *result.committedIdentity
			if attemptRequest.ExactTarget != nil && *attemptRequest.ExactTarget != identity.Target {
				attemptRequest.PreferredTabID = ""
			}
			if remoteTarget := attemptRequest.RemoteTarget; remoteTarget != nil &&
				(remoteTarget.LifecycleID != identity.Target.LifecycleID || remoteTarget.SessionName != identity.Target.SessionName) {
				attemptRequest.RemoteTarget = nil
			}
			attemptRequest.ExactTarget = &identity.Target
		}
		if result.routePosition != nil && attemptRequest.ExactTarget != nil &&
			*attemptRequest.ExactTarget == result.routePosition.Target {
			attemptRequest.PreferredTabID = result.routePosition.ActiveTabID
			attemptRequest.RemoteTarget = nil
		}
		if result.err == nil && result.welcomed && routeNavigationPending {
			routeNavigationPending = false
			routeNavigationSelection = nil
			routeNavigationResumeFallback = false
			routeNavigationFallback = nil
			routeNavigationAction = nil
		}
		if result.routeAction != nil {
			if r.ledger == nil {
				return errors.New("vev: route ledger unavailable")
			}
			selection, valid := r.ledger.navigationSelection(*result.routeAction)
			if !valid {
				return errors.New("vev: stale route navigation action")
			}
			if selection.noOp {
				continue
			}
			action := *result.routeAction
			routeNavigationAction = &action
			routeNavigationFallback = &attachRoute{dialer: selection.prior.dialer, request: selection.prior.request, resumeToken: selection.prior.resumeToken}
			dialer = selection.selected.dialer
			attemptRequest = cloneAttachRequest(selection.selected.request)
			if selection.selected.resumeToken != 0 {
				attemptRequest.Intent = ports.IntentResume
			} else {
				attemptRequest.Intent = ports.IntentAttach
			}
			// Back is transient to the picker overlay. Re-derive the home-picker
			// capability from the selected route instead of trusting request
			// metadata, which may have been cleared after a daemon-side switch.
			attemptRequest.StartupOverlay = ports.StartupOverlayNone
			attemptRequest.NavigationCapabilities = 0
			if homeRoute != nil && selection.selected.presentation.kind == ports.RouteKindRemote {
				attemptRequest.NavigationCapabilities = ports.NavigationCapabilityHomePicker
			}
			resumeToken = selection.selected.resumeToken
			routeNavigationPending = true
			routeNavigationSelection = &selection
			routeNavigationResumeFallback = selection.selected.resumeToken != 0
			remote = syncReconnectRemote(reconnect, attemptRequest.Remote || r.remote)
			backoff = defaultReconnectBackoff.initial
			continue
		}
		if result.action != 0 {
			switch result.action {
			case ports.NavigationOpenHomePicker:
				// The daemon only sends this action after accepting a Hello that
				// advertised Home support. The durable client-side prerequisite is
				// the captured route itself; request metadata may have been rebased by
				// committed identity or cursor updates since that Hello.
				if homeRoute == nil {
					return errors.New("vev: stale home navigation action")
				}
				returnRequest := attemptRequest
				if result.sessionName != "" {
					returnRequest.SessionName = result.sessionName
				}
				returnRoute = &attachRoute{dialer: dialer, request: returnRequest, resumeToken: result.resumeToken}
				homeNavigationPending = true
				enterHomePicker()
				continue
			case ports.NavigationBack:
				// As with Home, the daemon accepted Back in Hello before sending
				// this action. The retained concrete route is the durable client
				// prerequisite; request metadata may have been rebased meanwhile.
				if returnRoute == nil {
					return errors.New("vev: stale return navigation action")
				}
				route := *returnRoute
				returnRoute = nil
				homeNavigationPending = false
				returnNavigationPending = true
				restoreReturnRoute(route)
				continue
			default:
				return errors.New("vev: unsupported navigation action")
			}
		}
		if result.target != nil {
			if result.target.RemoteTarget != nil || result.target.Endpoint != "" {
				if homeRoute == nil && attemptRequest.RemoteTarget == nil {
					routeRequest := attemptRequest
					if result.sessionName != "" {
						routeRequest.SessionName = result.sessionName
					}
					routeRequest.Intent = ports.IntentAttach
					routeRequest.NavigationCapabilities = 0
					routeRequest.StartupOverlay = ports.StartupOverlayNone
					homeRoute = &attachRoute{dialer: dialer, request: routeRequest}
				}
			}
			var (
				nextDialer  ports.Dialer
				nextRequest AttachRequest
				handoffErr  error
			)
			if result.target.Endpoint == "" {
				if r.ledger == nil {
					return errors.New("vev: route ledger unavailable")
				}
				baseRequest := attemptRequest
				nextDialer = dialer
				if result.homePickerTarget {
					if homeRoute == nil {
						return errors.New("vev: home route unavailable for picker target")
					}
					nextDialer = homeRoute.dialer
					baseRequest = homeRoute.request
				}
				nextRequest = r.ledger.samePeerHandoff(baseRequest, *result.target)
			} else {
				if r.attachHandoff == nil {
					return &AttachTargetError{Target: *result.target}
				}
				nextDialer, nextRequest, handoffErr = r.attachHandoff(*result.target)
				if handoffErr != nil {
					return handoffErr
				}
			}
			if result.target.ExactTarget != nil {
				nextRequest.ExactTarget = result.target.ExactTarget
			}
			if homeRoute != nil && (nextRequest.RemoteTarget != nil || nextRequest.EnvironmentPolicy == ports.EnvironmentPolicyDaemonOwned) {
				nextRequest.NavigationCapabilities |= ports.NavigationCapabilityHomePicker
			}
			if attemptRequest.StartupOverlay == ports.StartupOverlaySessionPicker {
				returnRoute = nil
				returnResumeFallback = false
			}
			nextRequest = cloneAttachRequest(nextRequest)
			if err := validateAttachRequest(nextRequest); err != nil {
				return fmt.Errorf("vev: invalid route handoff request: %w", err)
			}
			if nextDialer == nil {
				return errors.New("vev: route handoff returned nil dialer")
			}
			dialer = nextDialer
			attemptRequest = nextRequest
			remote = syncReconnectRemote(reconnect, attemptRequest.Remote || r.remote)
			resumeToken = 0
			backoff = defaultReconnectBackoff.initial
			continue
		}
		if result.err == nil {
			if result.welcomed && returnNavigationPending {
				returnNavigationPending = false
				returnResumeFallback = false
			}
			if clearErr := reconnect.clear(); clearErr != nil {
				return clearErr
			}
			return nil
		}
		if result.welcomed {
			homeNavigationPending = false
		}
		if homeNavigationPending && returnRoute != nil {
			route := *returnRoute
			returnRoute = nil
			homeNavigationPending = false
			returnNavigationPending = true
			restoreReturnRoute(route)
			continue
		}
		if routeNavigationPending && routeNavigationResumeFallback && resumeNeedsExactAttach(result.err) {
			attemptRequest.Intent = ports.IntentAttach
			resumeToken = 0
			routeNavigationResumeFallback = false
			continue
		}
		if routeNavigationPending && routeNavigationFallback != nil {
			if routeNavigationAction != nil {
				action := *routeNavigationAction
				r.routeFailure = &ports.RouteNavigationFailure{Key: action.Key, Generation: action.Generation, Code: routeFailureCode(result.err)}
				routeNavigationAction = nil
			}
			route := *routeNavigationFallback
			routeNavigationFallback = nil
			routeNavigationPending = false
			routeNavigationSelection = nil
			routeNavigationResumeFallback = false
			returnNavigationPending = true
			restoreReturnRoute(route)
			continue
		}
		if returnResumeFallback && resumeNeedsExactAttach(result.err) {
			attemptRequest.Intent = ports.IntentAttach
			resumeToken = 0
			returnResumeFallback = false
			continue
		}
		if returnNavigationPending && !returnResumeFallback && homeRoute != nil {
			returnRoute = nil
			returnNavigationPending = false
			enterHomePicker()
			continue
		}
		if !shouldReconnect(result.err) || resumeToken == 0 || ctx.Err() != nil {
			if clearErr := reconnect.clear(); clearErr != nil {
				return errors.Join(result.err, clearErr)
			}
			return result.err
		}
		r.logger.Warn("reconnecting after attach error", "err", result.err, "backoff", backoff)
		if drawErr := reconnect.drawStage(reconnectStageSSH); drawErr != nil {
			return errors.Join(result.err, drawErr)
		}
		slept, sleepErr := reconnect.sleep(ctx, r.clock, backoff)
		if sleepErr != nil {
			return errors.Join(result.err, sleepErr)
		}
		if !slept {
			if clearErr := reconnect.clear(); clearErr != nil {
				return errors.Join(ctx.Err(), clearErr)
			}
			return ctx.Err()
		}
		backoff = nextReconnectBackoff(backoff, defaultReconnectBackoff.max)
		attemptRequest.Intent = ports.IntentResume
	}
}

// reconnectUI owns reconnect presentation while Runner owns the lifecycle.
// Its methods run only on Runner's goroutine, preserving the terminal's
// single-writer rule.
type reconnectUI struct {
	term       ports.Terminal
	remote     bool
	rawEntered *bool
	showing    bool
	stage      reconnectStage
}

func syncReconnectRemote(reconnect *reconnectUI, remote bool) bool {
	if reconnect != nil {
		reconnect.remote = remote
	}
	return remote
}

func (u *reconnectUI) redraw(size domain.Size) error {
	if !*u.rawEntered || !u.remote {
		return nil
	}
	_, err := drawReconnectToastStage(u.term.Out(), size, u.stage)
	if err != nil {
		return fmt.Errorf("drawing reconnect toast: %w", err)
	}
	if err := u.term.Flush(); err != nil {
		return fmt.Errorf("flushing reconnect toast: %w", err)
	}
	u.showing = true
	return nil
}

func (u *reconnectUI) drawStage(stage reconnectStage) error {
	u.stage = stage
	if u.showing && u.remote {
		size, err := u.term.Size()
		if err != nil {
			return fmt.Errorf("reading terminal size for reconnect toast: %w", err)
		}
		return u.redraw(size)
	}
	return u.draw()
}

func (u *reconnectUI) draw() error {
	if !*u.rawEntered || u.showing {
		return nil
	}
	if u.remote {
		size, err := u.term.Size()
		if err != nil {
			return fmt.Errorf("reading terminal size for reconnect toast: %w", err)
		}
		return u.redraw(size)
	}
	if _, err := u.term.Out().Write([]byte(statusReconnect)); err != nil {
		return fmt.Errorf("writing reconnect status: %w", err)
	}
	if err := u.term.Flush(); err != nil {
		return fmt.Errorf("flushing reconnect status: %w", err)
	}
	u.showing = true
	return nil
}

func (u *reconnectUI) clear() error {
	if !u.showing {
		return nil
	}
	if !u.remote {
		if _, err := u.term.Out().Write([]byte(statusClear)); err != nil {
			return fmt.Errorf("clearing reconnect status: %w", err)
		}
		if err := u.term.Flush(); err != nil {
			return fmt.Errorf("flushing reconnect status clear: %w", err)
		}
	}
	u.showing = false
	return nil
}

func (u *reconnectUI) sleep(ctx context.Context, clk ports.Clock, d time.Duration) (bool, error) {
	if u.remote && u.showing {
		return reconnectSleepWithResize(ctx, clk, d, u.term.ResizeEvents(), u.redraw)
	}
	return reconnectSleep(ctx, clk, d), nil
}

func sleepReconnect(ctx context.Context, clk ports.Clock, d time.Duration) bool {
	t := clk.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return true
	case <-ctx.Done():
		return false
	}
}

func sleepReconnectWithResizeEvents(ctx context.Context, clk ports.Clock, d time.Duration, resizeEvents <-chan domain.Size, onResize func(domain.Size) error) (bool, error) {
	t := clk.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-t.C():
			return true, nil
		case <-ctx.Done():
			return false, nil
		case size, ok := <-resizeEvents:
			if !ok {
				resizeEvents = nil
				continue
			}
			if onResize != nil {
				if err := onResize(size); err != nil {
					return false, err
				}
			}
		}
	}
}

type attachAttempt struct {
	runner                   *Runner
	dialer                   ports.Dialer
	connection               *SessionConnection
	remote                   bool
	transport                ports.Transport
	handshakeCtx             context.Context
	handshakeTimedOut        <-chan struct{}
	finishHandshake          func()
	stopHandshakeTransport   func()
	request                  AttachRequest
	resumeToken              uint64
	routeNavigationSelection *routeNavigationSelection
	clientID                 [16]byte
	milestones               *milestones
	themeState               *terminalThemeState
	enterRaw                 func() error
	reconnect                *reconnectUI
	transition               *transitionUI
	linkEvents               <-chan ports.LinkEvent
	rememberRemoteHost       func()
	terminalInput            func() *terminalInputPump
	openHomePicker           func(context.Context) attachResult
	transientPicker          bool
	returnAttachTargets      bool
}

type attachResult struct {
	resumeToken       uint64
	sessionName       string
	committedIdentity *ports.CommittedRouteIdentity
	routePosition     *ports.RoutePosition
	welcomed          bool
	transportClosed   bool
	target            *ports.AttachTarget
	action            ports.NavigationAction
	homePickerTarget  bool
	routeAction       *ports.RouteNavigationAction
	err               error
}

// AttachTargetError requests that the composition root replace the current
// transport with the selected endpoint. The daemon never opens that endpoint.
type AttachTargetError struct {
	Target ports.AttachTarget
}

func (e *AttachTargetError) Error() string {
	if e == nil {
		return "client: attach target handoff"
	}
	return fmt.Sprintf("client: attach target %q/%q", e.Target.Endpoint, e.Target.Session)
}

// paletteSlot validates the scanner's signed slot before narrowing it for the
// wire accumulator. Keep this boundary even though the in-tree Scanner also
// validates slots: callbacks are an interface boundary.
func paletteSlot(slot int) (uint8, bool) {
	if slot < 0 || slot > 15 {
		return 0, false
	}
	return uint8(slot), true
}

// DetectTrueColor reports whether TERM/COLORTERM advertise direct color support.
func DetectTrueColor(termEnv, colorTerm string) bool {
	return ports.DetectTrueColor(termEnv, colorTerm)
}

func requestedOutputWindow(transport ports.Transport) uint8 {
	if _, ok := transport.(ports.DatagramTransport); ok {
		return 1
	}
	return 8
}

func (a *attachAttempt) run(ctx context.Context) attachResult {
	transport := a.transport
	if a.connection != nil {
		transport = a.connection.Transport()
	}
	request := a.request
	term := a.runner.term
	clk := a.runner.clock
	intent := request.Intent
	name := request.SessionName
	resumeToken := a.resumeToken
	clientID := a.clientID
	ms := a.milestones
	themeState := a.themeState
	enterRaw := a.enterRaw
	reconnect := a.reconnect
	transition := a.transition
	linkEvents := a.linkEvents
	remote := a.remote || a.request.Remote
	clipboard := a.runner.clipboard
	log := a.runner.logger
	observer := a.runner.runtimeObserver
	handshakeCtx := a.handshakeCtx
	handshakeTimedOut := a.handshakeTimedOut
	finishHandshake := a.finishHandshake
	stopHandshakeTransport := a.stopHandshakeTransport
	if handshakeCtx == nil {
		handshakeCtx, handshakeTimedOut, finishHandshake = newHandshakeContext(ctx, clk)
		stopHandshakeTransport = watchHandshakeTransport(handshakeCtx, transport)
	}
	handshakeFinished := false
	endHandshake := func() {
		if handshakeFinished {
			return
		}
		handshakeFinished = true
		if stopHandshakeTransport != nil {
			stopHandshakeTransport()
		}
		if finishHandshake != nil {
			finishHandshake()
		}
	}
	defer endHandshake()
	handshakeFailure := func(stage string, err error) attachResult {
		if handshakeTimedOut != nil {
			err = handshakeContextError(ctx, handshakeTimedOut, err)
		}
		return attachResult{transportClosed: handshakeCtx.Err() != nil, err: fmt.Errorf("vev: %s: %w", stage, err)}
	}
	sendHandshake := func(operation func() error) error {
		err := boundedHandshakeOperationWithTransition(handshakeCtx, transport, operation, transition)
		if err != nil {
			return handshakeContextError(ctx, handshakeTimedOut, err)
		}
		return nil
	}
	// 1. Handshake: probe the direct outer terminal before Hello when its
	// environment identifies Kitty. The probe uses the lifecycle input pump;
	// unsupported and silent terminals fail closed without delaying attach past
	// the bounded probe deadline.
	kittyDirectGraphics := false
	if a.runner != nil && a.runner.kittyProbeEnabled() && a.terminalInput != nil {
		if err := enterRaw(); err != nil {
			return attachResult{err: err}
		}
		kittyDirectGraphics = a.runner.probeKittyDirectGraphics(handshakeCtx, a.terminalInput())
	}
	// 1. Handshake: send Hello with our geometry and TERM.
	geometry, err := term.Geometry()
	if err != nil {
		return attachResult{err: fmt.Errorf("vev: reading terminal geometry: %w", err)}
	}
	geometry = geometry.NormalizePixels()
	size := geometry.Size
	cwd, err := os.Getwd()
	if err != nil {
		cwd = ""
	}
	termEnv := os.Getenv("TERM")
	colorTerm := os.Getenv("COLORTERM")
	trueColor := DetectTrueColor(termEnv, colorTerm)
	themeState.setTrueColor(trueColor)
	exactTarget := request.ExactTarget
	if exactTarget == nil && request.RemoteTarget != nil {
		exactTarget = &ports.ExactSessionTarget{
			LifecycleID: request.RemoteTarget.LifecycleID,
			SessionName: request.RemoteTarget.SessionName,
		}
	}
	hello := ports.Hello{
		Version:                ports.ProtocolVersion,
		Intent:                 intent,
		ClientID:               clientID,
		ResumeToken:            resumeToken,
		Name:                   name,
		Size:                   size,
		PixelWidth:             geometry.PixelWidth,
		PixelHeight:            geometry.PixelHeight,
		TermEnv:                termEnv,
		Cwd:                    cwd,
		TrueColor:              trueColor,
		KittyDirectGraphics:    kittyDirectGraphics,
		MaxOutputInFlight:      requestedOutputWindow(transport),
		Env:                    os.Environ(),
		RemoteTarget:           request.RemoteTarget,
		ExactTarget:            exactTarget,
		PreferredTabID:         request.PreferredTabID,
		EnvironmentPolicy:      request.EnvironmentPolicy,
		NavigationCapabilities: request.NavigationCapabilities,
		StartupOverlay:         request.StartupOverlay,
		Remote:                 remote,
	}
	if err := sendHandshake(func() error {
		return transport.Send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)})
	}); err != nil {
		return handshakeFailure("sending hello", err)
	}
	ms.helloSent = true

	// 2. Await Welcome or a typed rejection.
	var reply ports.Frame
	if err := boundedHandshakeOperationWithTransition(handshakeCtx, transport, func() error {
		var recvErr error
		reply, recvErr = transport.Recv()
		return recvErr
	}, transition); err != nil {
		return handshakeFailure("awaiting welcome", err)
	}
	var committedIdentity *ports.CommittedRouteIdentity
	var routePosition *ports.RoutePosition
	result := func(welcomed bool, err error) attachResult {
		return attachResult{
			resumeToken:       resumeToken,
			sessionName:       name,
			committedIdentity: cloneCommittedIdentity(committedIdentity),
			routePosition:     cloneRoutePosition(routePosition),
			welcomed:          welcomed,
			err:               err,
		}
	}
	switch reply.Type {
	case ports.MsgWelcome:
		welcome, derr := ports.UnmarshalWelcome(reply.Payload)
		if derr != nil {
			return result(false, fmt.Errorf("vev: decoding welcome: %w", derr))
		}
		resumeToken = welcome.ResumeToken
		name = welcome.SessionName
		committedIdentity = cloneCommittedIdentity(welcome.CommittedIdentity)
		ms.welcomed = true
		log.Debug("welcomed by daemon", "resume_token_present", resumeToken != 0)
		if remote {
			if remember := a.rememberRemoteHost; remember != nil {
				remember()
			}
		}
	case ports.MsgError:
		em, derr := ports.UnmarshalErrorMsg(reply.Payload)
		if derr != nil {
			return result(false, fmt.Errorf("vev: decoding error reply: %w", derr))
		}
		return result(false, &ProtocolError{Code: em.Code, Text: em.Text})
	default:
		return result(false, fmt.Errorf("vev: unexpected reply type %d before welcome", reply.Type))
	}
	if err := handshakeCtx.Err(); err != nil {
		return handshakeFailure("validating welcome", err)
	}
	welcomedResult := func(err error) attachResult { return result(true, err) }
	if committedIdentity != nil && a.runner.ledger != nil {
		if request.ExactTarget != nil && *request.ExactTarget != committedIdentity.Target {
			return welcomedResult(errRouteTargetChanged)
		}
		if request.RemoteTarget != nil && (request.RemoteTarget.LifecycleID != committedIdentity.Target.LifecycleID || request.RemoteTarget.SessionName != committedIdentity.Target.SessionName) {
			return welcomedResult(errRouteTargetChanged)
		}
		if !a.transientPicker {
			candidate := routeCandidateForAttach(request, *committedIdentity, a.dialer, resumeToken)
			var commitErr error
			if a.routeNavigationSelection != nil {
				selection := *a.routeNavigationSelection
				commitErr = a.runner.ledger.commitTransition(selection.selected.identity, selection.snapshotGeneration, selection.active, candidate)
			} else {
				_, commitErr = a.runner.ledger.commitAttach(candidate)
			}
			if commitErr != nil {
				return welcomedResult(fmt.Errorf("vev: committing route identity: %w", commitErr))
			}
		}
		snapshotPayload, err := ports.MarshalRecentRouteSnapshot(a.runner.ledger.snapshot())
		if err != nil {
			return welcomedResult(fmt.Errorf("vev: encoding route snapshot: %w", err))
		}
		attentionPayload, err := ports.MarshalRouteAttentionSubscription(a.runner.ledger.attentionSubscription())
		if err != nil {
			return welcomedResult(fmt.Errorf("vev: encoding route attention subscription: %w", err))
		}
		if err := sendHandshake(func() error {
			if err := transport.Send(ports.Frame{Type: ports.MsgRecentRouteSnapshot, Payload: snapshotPayload}); err != nil {
				return err
			}
			return transport.Send(ports.Frame{Type: ports.MsgRouteAttentionSubscription, Payload: attentionPayload})
		}); err != nil {
			return welcomedResult(fmt.Errorf("vev: publishing route snapshot: %w", err))
		}
	}
	if a.runner.routeFailure != nil {
		payload, err := ports.MarshalRouteNavigationFailure(*a.runner.routeFailure)
		if err != nil {
			return welcomedResult(fmt.Errorf("vev: encoding route navigation failure: %w", err))
		}
		if err := sendHandshake(func() error {
			return transport.Send(ports.Frame{Type: ports.MsgRouteNavigationFailure, Payload: payload})
		}); err != nil {
			return welcomedResult(fmt.Errorf("vev: publishing route navigation failure: %w", err))
		}
		a.runner.routeFailure = nil
	}

	// 3. Enter raw mode after Welcome; Run owns restoration.
	if err := enterRaw(); err != nil {
		return welcomedResult(err)
	}

	// 4. Derive a cancellable context so the pumps always unwind when the
	// loop returns. The attach loop is the sole terminal writer and palette
	// coordinator owner.
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	input := (*terminalInputPump)(nil)
	if a.terminalInput != nil {
		input = a.terminalInput()
	}
	ownedInput := input == nil
	if ownedInput {
		input = newTerminalInputPump(term.In())
		input.start()
		defer input.stop()
	}
	sendCh := make(chan ports.Frame, sendQueueDepth)
	controlCh := make(chan ports.Frame, 1)
	barrierCh := make(chan chan struct{})
	inputGate := newSamePeerInputGate()
	ackQueue := newCumulativeAckQueue()
	sendErrCh := make(chan error, 1)
	senderStarted := false
	paletteEvents := make(chan paletteGenerationEvent, 32)
	var activeGeneration atomic.Uint64
	coordinator := newPaletteGenerationCoordinator()
	type paletteTimer struct {
		timer  ports.Timer
		cancel chan struct{}
	}
	drainTimers := map[paletteGenerationID]paletteTimer{}
	completionTimers := map[paletteGenerationID]paletteTimer{}
	cancelTimer := func(timers map[paletteGenerationID]paletteTimer, id paletteGenerationID) {
		if timer, ok := timers[id]; ok {
			timer.timer.Stop()
			close(timer.cancel)
			delete(timers, id)
		}
	}
	publish := func(snapshot ports.Theme) error {
		frame := ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(snapshot)}
		// The initial cleared snapshot is sent before the sender exists. This
		// keeps the generation publication ordered before the first query and
		// avoids leaving a queued Theme behind an immediate detach.
		if !senderStarted {
			if err := sendHandshake(func() error { return transport.Send(frame) }); err != nil {
				return fmt.Errorf("sending initial theme: %w", err)
			}
			return nil
		}
		select {
		case sendCh <- frame:
			return nil
		case <-loopCtx.Done():
			return context.Canceled
		}
	}
	processPaletteActions := func(actions []paletteGenerationAction) error {
		for _, action := range actions {
			switch action.kind {
			case paletteActionPublishCleared:
				if err := publish(action.theme); err != nil {
					return fmt.Errorf("publishing theme: %w", err)
				}
			case paletteActionPublishFinal:
				themeState.update(func(current *ports.Theme) { *current = action.theme })
				if err := publish(action.theme); err != nil {
					return fmt.Errorf("publishing theme: %w", err)
				}
			case paletteActionWriteDrain, paletteActionWriteBatch:
				if _, err := term.Out().Write([]byte(action.bytes)); err != nil {
					return fmt.Errorf("writing palette query: %w", err)
				}
				if err := term.Flush(); err != nil {
					return fmt.Errorf("flushing palette query: %w", err)
				}
			case paletteActionArmDrainDeadline, paletteActionArmCompletionDeadline:
				timer := clk.NewTimer(action.deadline)
				entry := paletteTimer{timer: timer, cancel: make(chan struct{})}
				timers := drainTimers
				kind := paletteEventDrainDeadline
				if action.kind == paletteActionArmCompletionDeadline {
					timers = completionTimers
					kind = paletteEventCompletionDeadline
				}
				timers[action.id] = entry
				go func(id paletteGenerationID, eventKind paletteGenerationEventKind, timer paletteTimer) {
					select {
					case <-timer.timer.C():
						select {
						case paletteEvents <- paletteGenerationEvent{id: id, kind: eventKind}:
						case <-loopCtx.Done():
						}
					case <-timer.cancel:
					case <-loopCtx.Done():
					}
				}(action.id, kind, entry)
			case paletteActionCancelDrainDeadline:
				cancelTimer(drainTimers, action.id)
			case paletteActionCancelCompletionDeadline:
				cancelTimer(completionTimers, action.id)
			}
		}
		return nil
	}
	defer func() {
		for id := range drainTimers {
			cancelTimer(drainTimers, id)
		}
		for id := range completionTimers {
			cancelTimer(completionTimers, id)
		}
	}()

	// Initial acquisition is a generation too. A reconnect publishes its one
	// cleared snapshot from retained definitive colors, never a restore frame.
	retained := themeState.snapshot()
	initialActions := coordinator.start(retained, false)
	activeGeneration.Store(uint64(coordinator.current.id))
	if err := processPaletteActions(initialActions); err != nil {
		return welcomedResult(err)
	}

	var clip ports.ClipboardReader
	if remote {
		clip = clipboard
	}
	awaitingReconnectReset := false
	outputState := outputApplyState{}
	outputResetRequested := false
	transitionWaitingFull := false
	var samePeerSwitch *samePeerSwitchPending
	var nextSamePeerRequestID uint64
	type parkedRoutePending struct {
		requestID      uint64
		action         ports.ParkedRouteAction
		leaseID        ports.ParkedRouteLeaseID
		fallbackTarget *ports.AttachTarget
		timer          ports.Timer
	}
	var parkedPending *parkedRoutePending
	var nextParkedRequestID uint64
	parkedWaitingFull := false
	var parkedFullTimer ports.Timer
	transportFailed := make(chan struct{})
	if reconnect.showing {
		if reconnect.remote {
			// Before the asynchronous sender starts, preserve transport ordering by
			// requesting the reset synchronously after the initial Theme publication.
			resetErr := sendHandshake(func() error {
				payload, err := ports.MarshalResize(ports.Resize{Size: size})
				if err != nil {
					return err
				}
				return transport.Send(ports.Frame{Type: ports.MsgResize, Payload: payload})
			})
			if resetErr != nil {
				resetErr = handshakeContextError(ctx, handshakeTimedOut, resetErr)
				resetResult := welcomedResult(fmt.Errorf("vev: requesting reconnect reconciliation: %w", resetErr))
				resetResult.transportClosed = handshakeCtx.Err() != nil
				return resetResult
			}
			awaitingReconnectReset = true
		}
		if err := reconnect.clear(); err != nil {
			return welcomedResult(err)
		}
	}
	// Welcome and every synchronous initial publication have committed. The
	// handshake deadline must not own the long-lived runtime transport: a quiet
	// or unchanged screen may legitimately produce no initial Output frame.
	endHandshake()
	senderStarted = true
	go runSender(loopCtx, cancel, transport, controlCh, barrierCh, sendCh, inputGate, ackQueue, sendErrCh, log)

	type foregroundRuntime struct {
		cancel                context.CancelFunc
		consumer              uint64
		sendLease             *foregroundSendLease
		stdinDone, resizeDone chan struct{}
	}
	var foreground *foregroundRuntime
	startForeground := func() {
		if foreground != nil {
			return
		}
		foregroundCtx, foregroundCancel := context.WithCancel(loopCtx)
		consumer := input.claim()
		stdinDone := make(chan struct{})
		resizeDone := make(chan struct{})
		sendLease := newForegroundSendLease()
		foreground = &foregroundRuntime{cancel: foregroundCancel, consumer: consumer, sendLease: sendLease, stdinDone: stdinDone, resizeDone: resizeDone}
		go func() {
			defer close(stdinDone)
			cancelAll := func() {
				foregroundCancel()
				cancel()
			}
			(&stdinPump{ctx: foregroundCtx, cancel: cancelAll, input: input, consumer: consumer, out: sendCh, clock: clk, clipboard: clip, logger: log, paletteEvents: paletteEvents, activeGeneration: &activeGeneration, sendLease: sendLease}).run()
		}()
		go func() {
			defer close(resizeDone)
			runResize(foregroundCtx, term.ResizeEvents(), sendCh, sendLease, log)
		}()
	}
	stopForeground := func() {
		if foreground == nil {
			return
		}
		current := foreground
		foreground = nil
		current.cancel()
		current.sendLease.stop()
		<-current.stdinDone
		<-current.resizeDone
		input.revoke(current.consumer)
		for id := range drainTimers {
			cancelTimer(drainTimers, id)
		}
		for id := range completionTimers {
			cancelTimer(completionTimers, id)
		}
	}
	flushSender := func() error {
		done := make(chan struct{})
		select {
		case barrierCh <- done:
		case <-loopCtx.Done():
			return context.Canceled
		}
		select {
		case <-done:
			return nil
		case <-loopCtx.Done():
			return context.Canceled
		}
	}
	clearParkedFull := func() {
		parkedWaitingFull = false
		if parkedFullTimer != nil {
			parkedFullTimer.Stop()
			parkedFullTimer = nil
		}
	}
	awaitParkedFull := func() {
		clearParkedFull()
		parkedWaitingFull = true
		parkedFullTimer = clk.NewTimer(ports.HandshakeTimeout)
	}
	parkedFullC := func() <-chan time.Time {
		if parkedFullTimer == nil {
			return nil
		}
		return parkedFullTimer.C()
	}
	defer clearParkedFull()
	clearParkedPending := func() {
		if parkedPending != nil && parkedPending.timer != nil {
			parkedPending.timer.Stop()
		}
		parkedPending = nil
	}
	parkedResponseC := func() <-chan time.Time {
		if parkedPending == nil || parkedPending.timer == nil {
			return nil
		}
		return parkedPending.timer.C()
	}
	defer clearParkedPending()

	sendParkedRouteSize := func() error {
		current, err := term.Size()
		if err != nil {
			return fmt.Errorf("reading terminal size for parked route: %w", err)
		}
		payload, err := ports.MarshalResize(ports.Resize{Size: current})
		if err != nil {
			return fmt.Errorf("encoding terminal size for parked route: %w", err)
		}
		select {
		case controlCh <- ports.Frame{Type: ports.MsgResize, Payload: payload}:
			return nil
		case <-loopCtx.Done():
			return context.Canceled
		}
	}
	sendParkedRouteRequest := func(action ports.ParkedRouteAction, leaseID ports.ParkedRouteLeaseID, target *domain.RemoteSessionTarget) error {
		if parkedPending != nil {
			return errors.New("vev: parked-route request already pending")
		}
		nextParkedRequestID++
		request := ports.ParkedRouteRequest{RequestID: nextParkedRequestID, LeaseID: leaseID, Action: action, Target: target}
		payload := ports.MarshalParkedRouteRequest(request)
		if payload == nil {
			return errors.New("vev: invalid parked-route request")
		}
		select {
		case controlCh <- ports.Frame{Type: ports.MsgParkedRouteRequest, Payload: payload}:
			parkedPending = &parkedRoutePending{
				requestID: request.RequestID, action: action, leaseID: leaseID,
				timer: clk.NewTimer(ports.HandshakeTimeout),
			}
			return nil
		case <-loopCtx.Done():
			return context.Canceled
		}
	}
	handleParkedPicker := func(leaseID ports.ParkedRouteLeaseID) (attachResult, bool) {
		pickerCtx, cancelPicker := context.WithCancel(ctx)
		pickerDone := make(chan struct{})
		go func() {
			select {
			case <-transportFailed:
				cancelPicker()
			case <-pickerDone:
			}
		}()
		selection := a.openHomePicker(pickerCtx)
		close(pickerDone)
		cancelPicker()
		select {
		case <-transportFailed:
			return welcomedResult(errLinkOffline), true
		default:
		}
		if selection.target != nil && transition != nil {
			transitionWaitingFull = false
			transition.start(*selection.target)
		}
		if selection.target != nil && request.Remote && request.OriginKey != "" && selection.target.Endpoint == request.OriginKey && selection.target.RemoteTarget != nil {
			target := *selection.target.RemoteTarget
			if err := sendParkedRouteSize(); err != nil {
				return welcomedResult(err), true
			}
			if err := sendParkedRouteRequest(ports.ParkedRouteSwitch, leaseID, &target); err != nil {
				return welcomedResult(fmt.Errorf("vev: switching parked route: %w", err)), true
			}
			fallback := *selection.target
			parkedPending.fallbackTarget = &fallback
			return attachResult{}, false
		}
		if selection.target != nil {
			selection.homePickerTarget = true
			return selection, true
		}
		if selection.err != nil {
			log.Warn("transient home picker failed; resuming parked route", "err", selection.err)
		}
		if err := sendParkedRouteSize(); err != nil {
			return welcomedResult(err), true
		}
		if err := sendParkedRouteRequest(ports.ParkedRouteResume, leaseID, nil); err != nil {
			return welcomedResult(fmt.Errorf("vev: resuming parked route: %w", err)), true
		}
		return attachResult{}, false
	}
	startForeground()
	defer stopForeground()

	// 5. Output/main loop: the only goroutine that touches the terminal.
	recvCh := make(chan recvResult, 1)
	go runRecv(loopCtx, transport, recvCh, transportFailed, log)

	requestReconnectReset := func() error {
		if awaitingReconnectReset {
			return nil
		}
		size, serr := term.Size()
		if serr != nil {
			return fmt.Errorf("reading terminal size for reconnect reconciliation: %w", serr)
		}
		payload, err := ports.MarshalResize(ports.Resize{Size: size})
		if err != nil {
			return fmt.Errorf("encoding terminal size for reconnect reconciliation: %w", err)
		}
		select {
		case sendCh <- ports.Frame{Type: ports.MsgResize, Payload: payload}:
			awaitingReconnectReset = true
			return nil
		case <-loopCtx.Done():
			return context.Canceled
		}
	}
	publishRouteSnapshot := func() error {
		if a.runner.ledger == nil {
			return errors.New("vev: route ledger unavailable")
		}
		snapshotPayload, err := ports.MarshalRecentRouteSnapshot(a.runner.ledger.snapshot())
		if err != nil {
			return err
		}
		attentionPayload, err := ports.MarshalRouteAttentionSubscription(a.runner.ledger.attentionSubscription())
		if err != nil {
			return err
		}
		for _, frame := range []ports.Frame{
			{Type: ports.MsgRecentRouteSnapshot, Payload: snapshotPayload},
			{Type: ports.MsgRouteAttentionSubscription, Payload: attentionPayload},
		} {
			select {
			case sendCh <- frame:
			case <-loopCtx.Done():
				return context.Canceled
			}
		}
		return nil
	}
	sendRouteFailure := func(action ports.RouteNavigationAction, code ports.RouteFailureCode) error {
		payload, err := ports.MarshalRouteNavigationFailure(ports.RouteNavigationFailure{
			Key:        action.Key,
			Generation: action.Generation,
			Code:       code,
		})
		if err != nil {
			return err
		}
		select {
		case sendCh <- ports.Frame{Type: ports.MsgRouteNavigationFailure, Payload: payload}:
			return nil
		case <-loopCtx.Done():
			return context.Canceled
		}
	}
	dismissReconnect := func() error {
		if !reconnect.showing {
			return nil
		}
		if err := reconnect.clear(); err != nil {
			return err
		}
		if !reconnect.remote {
			return nil
		}
		return requestReconnectReset()
	}
	loopCanceledResult := func() attachResult {
		// A pump initiated shutdown (stdin EOF/detach) or the parent context
		// was cancelled. A queued sender error takes priority.
		select {
		case serr := <-sendErrCh:
			return welcomedResult(fmt.Errorf("vev: sending to daemon: %w", serr))
		default:
			return welcomedResult(nil)
		}
	}
	for {
		select {
		case <-loopCtx.Done():
			return loopCanceledResult()
		case <-transition.tickC():
			if err := transition.advance(); err != nil {
				return welcomedResult(err)
			}
		case <-parkedResponseC():
			clearParkedPending()
			return welcomedResult(errors.New("vev: timed out waiting for parked-route response"))
		case <-parkedFullC():
			clearParkedFull()
			return welcomedResult(errors.New("vev: timed out waiting for parked-route full output"))
		case ev, ok := <-linkEvents:
			if !ok {
				linkEvents = nil
				continue
			}
			if ev.State == ports.LinkStateConnected {
				if err := dismissReconnect(); err != nil {
					if errors.Is(err, context.Canceled) {
						return loopCanceledResult()
					}
					return welcomedResult(fmt.Errorf("dismissing reconnect toast: %w", err))
				}
				select {
				case sendCh <- ports.Frame{Type: ports.MsgClientNotice, Payload: ports.MarshalClientNotice(ports.ClientNotice{Action: ports.ClientNoticeLinkConnected})}:
				case <-loopCtx.Done():
					return welcomedResult(nil)
				}
				continue
			}
			if ev.State == ports.LinkStateDegraded {
				log.Warn("UDP link degraded")
				select {
				case sendCh <- ports.Frame{Type: ports.MsgClientNotice, Payload: ports.MarshalClientNotice(ports.ClientNotice{Action: ports.ClientNoticeLinkDegraded})}:
				case <-loopCtx.Done():
					return welcomedResult(nil)
				}
				continue
			}
			stage := stageForLinkState(ev.State)
			if reconnect.remote && reconnect.showing && reconnect.stage != stage {
				reconnect.stage = stage
				if err := requestReconnectReset(); err != nil {
					if errors.Is(err, context.Canceled) {
						return loopCanceledResult()
					}
					return welcomedResult(fmt.Errorf("requesting reconnect reconciliation: %w", err))
				}
			} else if err := reconnect.drawStage(stage); err != nil {
				return welcomedResult(err)
			}
			if ev.State == ports.LinkStateOffline {
				// Stop waiting for the transport to reach Dead at 60s: exit with
				// a retryable error so Run re-dials over ssh with the resume
				// token. Run closes this transport on the way out.
				return welcomedResult(errLinkOffline)
			}
		case event := <-paletteEvents:
			if event.kind == paletteEventScheme {
				retained := themeState.update(func(current *ports.Theme) {
					current.SchemeKnown = true
					current.Light = event.light
				})
				actions := coordinator.start(retained, true)
				activeGeneration.Store(uint64(coordinator.current.id))
				if err := processPaletteActions(actions); err != nil {
					return welcomedResult(err)
				}
				continue
			}
			if err := processPaletteActions(coordinator.handle(event)); err != nil {
				return welcomedResult(err)
			}
		case r := <-recvCh:
			if r.err != nil {
				if errors.Is(r.err, io.EOF) {
					return welcomedResult(fmt.Errorf("vev: daemon vanished (missing: %v): %w", ms.missing(), r.err))
				}
				return welcomedResult(fmt.Errorf("vev: receiving from daemon: %w", r.err))
			}
			switch r.frame.Type {
			case ports.MsgOutput:
				o, derr := ports.UnmarshalOutput(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding output: %w", derr))
				}
				nextState, accepted := outputState.next(o)
				if !accepted {
					// A discarded frame was never written and must never be ACKed.
					// Ask the daemon for one authoritative full reset, coalescing
					// repeated gaps while that reset is in flight.
					if !outputResetRequested {
						select {
						case sendCh <- ports.Frame{Type: ports.MsgOutputResetRequest, Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{})}:
							outputResetRequested = true
						case <-loopCtx.Done():
							return loopCanceledResult()
						}
					}
					continue
				}
				if parkedWaitingFull && !o.Full {
					return welcomedResult(errors.New("vev: parked route resumed without an authoritative full output"))
				}
				if _, werr := term.Out().Write(o.Data); werr != nil {
					return welcomedResult(fmt.Errorf("vev: writing terminal output: %w", werr))
				}
				if awaitingReconnectReset && o.Full && o.Base == 0 && o.New != 0 {
					awaitingReconnectReset = false
				}
				if reconnect.showing && reconnect.remote {
					size, serr := term.Size()
					if serr != nil {
						return welcomedResult(fmt.Errorf("vev: reading terminal size for reconnect redraw: %w", serr))
					}
					if rerr := reconnect.redraw(size); rerr != nil {
						return welcomedResult(fmt.Errorf("vev: redrawing reconnect toast: %w", rerr))
					}
				} else if ferr := term.Flush(); ferr != nil {
					return welcomedResult(fmt.Errorf("vev: flushing terminal: %w", ferr))
				}
				outputState = nextState
				if transition != nil && transition.active && (!transitionWaitingFull || o.Full) {
					transitionWaitingFull = false
					transition.stop()
				}
				if parkedWaitingFull {
					clearParkedFull()
					startForeground()
				}
				if o.New != 0 {
					ackQueue.offer(o.Epoch, o.New)
					outputResetRequested = false
				}
				// The terminal boundary is after a successful flush and before ACK.
				if observer != nil {
					observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeTerminalFlushed, uint64(len(o.Data)), true))
				}
				if !ms.firstOutput {
					ms.firstOutput = true
					log.Debug("received first output")
					endHandshake()
				}
			case ports.MsgAttachTarget:
				target, derr := ports.UnmarshalAttachTarget(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding attach target: %w", derr))
				}
				if a.returnAttachTargets {
					handoff := welcomedResult(nil)
					handoff.target = &target
					return handoff
				}
				if transition != nil {
					transitionWaitingFull = false
					transition.start(target)
				}
				if target.Endpoint == "" && target.RemoteTarget == nil && target.ExactTarget != nil && a.runner.ledger != nil {
					if samePeerSwitch != nil {
						return welcomedResult(errors.New("vev: concurrent same-peer switch offer"))
					}
					nextSamePeerRequestID++
					request := a.runner.ledger.samePeerHandoff(request, target)
					payload, err := ports.MarshalSamePeerSwitchRequest(ports.SamePeerSwitchRequest{
						RequestID:      nextSamePeerRequestID,
						Target:         *target.ExactTarget,
						PreferredTabID: request.PreferredTabID,
					})
					if err != nil {
						return welcomedResult(fmt.Errorf("vev: encoding same-peer switch: %w", err))
					}
					samePeerSwitch = &samePeerSwitchPending{requestID: nextSamePeerRequestID, target: *target.ExactTarget}
					inputGate.setPaused(true)
					select {
					case controlCh <- ports.Frame{Type: ports.MsgSamePeerSwitchRequest, Payload: payload}:
						continue
					case <-loopCtx.Done():
						return loopCanceledResult()
					}
				}
				handoff := welcomedResult(nil)
				handoff.target = &target
				return handoff
			case ports.MsgParkedRouteResponse:
				response, derr := ports.UnmarshalParkedRouteResponse(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding parked-route response: %w", derr))
				}
				if parkedPending == nil || response.RequestID != parkedPending.requestID {
					return welcomedResult(errors.New("vev: unexpected parked-route response"))
				}
				pending := *parkedPending
				clearParkedPending()
				switch pending.action {
				case ports.ParkedRoutePrepare:
					if response.Status != ports.ParkedRouteReady {
						fallback := welcomedResult(nil)
						fallback.action = ports.NavigationOpenHomePicker
						return fallback
					}
					if selection, done := handleParkedPicker(pending.leaseID); done {
						return selection
					}
				case ports.ParkedRouteResume:
					if response.Status != ports.ParkedRouteResumed {
						return welcomedResult(errors.New("vev: parked route could not resume"))
					}
					awaitParkedFull()
				case ports.ParkedRouteSwitch:
					switch response.Status {
					case ports.ParkedRouteSwitched:
						awaitParkedFull()
						continue
					case ports.ParkedRouteStaleTarget:
						if transition != nil {
							transition.stop()
						}
						if selection, done := handleParkedPicker(pending.leaseID); done {
							return selection
						}
					default:
						if pending.fallbackTarget == nil {
							return welcomedResult(errors.New("vev: parked route switch failed"))
						}
						fallback := welcomedResult(nil)
						fallback.target = pending.fallbackTarget
						fallback.homePickerTarget = true
						return fallback
					}
				}
			case ports.MsgNavigationAction:
				directive, derr := ports.UnmarshalNavigationDirective(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding navigation action: %w", derr))
				}
				_, datagramRoute := transport.(ports.DatagramTransport)
				if directive.Action == ports.NavigationOpenHomePicker && a.openHomePicker != nil && datagramRoute {
					stopForeground()
					if err := flushSender(); err != nil {
						return welcomedResult(fmt.Errorf("vev: parking remote foreground: %w", err))
					}
					if err := sendParkedRouteRequest(ports.ParkedRoutePrepare, directive.LeaseID, nil); err != nil {
						return welcomedResult(fmt.Errorf("vev: preparing parked route: %w", err))
					}
					continue
				}
				navigation := welcomedResult(nil)
				navigation.action = directive.Action
				return navigation
			case ports.MsgNavigateRecentRoute:
				action, derr := ports.UnmarshalRouteNavigationAction(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding route navigation action: %w", derr))
				}
				if a.runner.ledger == nil {
					if ferr := sendRouteFailure(action, ports.RouteFailureUnavailable); ferr != nil {
						return welcomedResult(ferr)
					}
					continue
				}
				_, valid, noOp := a.runner.ledger.navigationRecord(action)
				if !valid {
					if ferr := sendRouteFailure(action, ports.RouteFailureStaleSelection); ferr != nil {
						return welcomedResult(ferr)
					}
					continue
				}
				if noOp {
					continue
				}
				navigation := welcomedResult(nil)
				navigation.routeAction = &action
				return navigation
			case ports.MsgCommittedRouteIdentity:
				identity, derr := ports.UnmarshalCommittedRouteIdentity(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding committed route identity: %w", derr))
				}
				if samePeerSwitch != nil && identity.Target != samePeerSwitch.target {
					return welcomedResult(errors.New("vev: same-peer switch committed an unexpected target"))
				}
				name = identity.Target.SessionName
				committedIdentity = cloneCommittedIdentity(&identity)
				if a.runner.ledger == nil {
					return welcomedResult(errors.New("vev: route ledger unavailable"))
				}
				if _, derr := a.runner.ledger.commitCommittedIdentity(identity); derr != nil {
					return welcomedResult(fmt.Errorf("vev: committing daemon-local route identity: %w", derr))
				}
				if derr := publishRouteSnapshot(); derr != nil {
					return welcomedResult(fmt.Errorf("vev: publishing daemon-local route snapshot: %w", derr))
				}
				if samePeerSwitch != nil {
					samePeerSwitch = nil
					inputGate.setPaused(false)
				}
			case ports.MsgSamePeerSwitchFailure:
				failure, derr := ports.UnmarshalSamePeerSwitchFailure(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding same-peer switch failure: %w", derr))
				}
				if samePeerSwitch != nil && failure.RequestID == samePeerSwitch.requestID {
					samePeerSwitch = nil
					inputGate.setPaused(false)
					if transition != nil && transition.showing {
						transitionWaitingFull = true
						if !outputResetRequested {
							select {
							case sendCh <- ports.Frame{Type: ports.MsgOutputResetRequest, Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{})}:
								outputResetRequested = true
							case <-loopCtx.Done():
								return loopCanceledResult()
							}
						}
					} else if transition != nil {
						transition.stop()
					}
				}
			case ports.MsgRoutePosition:
				position, derr := ports.UnmarshalRoutePosition(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding route position: %w", derr))
				}
				if a.runner.ledger == nil {
					return welcomedResult(errors.New("vev: route ledger unavailable"))
				}
				if derr := a.runner.ledger.updateRoutePosition(position); derr != nil {
					return welcomedResult(fmt.Errorf("vev: remembering route position: %w", derr))
				}
				routePosition = cloneRoutePosition(&position)
			case ports.MsgRouteNavigationFailure:
				failure, derr := ports.UnmarshalRouteNavigationFailure(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding route navigation failure: %w", derr))
				}
				log.Warn("route navigation rejected", "key", failure.Key, "generation", failure.Generation, "code", failure.Code)
			case ports.MsgDetached:
				d, derr := ports.UnmarshalDetached(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding detached: %w", derr))
				}
				ms.detached = true
				if d.Reason == ports.ReasonDetach {
					log.Info("detached cleanly", "reason", d.Reason)
				} else {
					log.Warn("detached by daemon", "reason", d.Reason)
				}
				return welcomedResult(detachedResult(d.Reason))
			case ports.MsgPong:
				// Liveness reply; nothing to do.
			default:
				// Unknown/out-of-band message types are ignored so a newer
				// daemon can add server->client messages without breaking us.
			}
		}
	}
}

// recvResult carries one framed message (or a read error) from the receive
// pump to the main loop.
type recvResult struct {
	frame ports.Frame
	err   error
}

// runRecv reads frames until an error, forwarding each to the main loop.
// It exits on the first error or when the loop context is cancelled.
func runRecv(ctx context.Context, transport ports.Transport, out chan<- recvResult, failed chan<- struct{}, log *slog.Logger) {
	defer log.Debug("receive pump exited")
	for {
		f, err := transport.Recv()
		if err != nil && failed != nil {
			close(failed)
		}
		select {
		case out <- recvResult{frame: f, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// runSender is the sole caller of transport.Send: serialising the input and
// resize pumps through one goroutine keeps the transport's single-writer
// contract intact. A send failure cancels the loop and is surfaced once.
type cumulativeAckQueue struct {
	mu          sync.Mutex
	latestEpoch uint64
	latestState uint64
	wake        chan struct{}
}

func newCumulativeAckQueue() *cumulativeAckQueue {
	return &cumulativeAckQueue{wake: make(chan struct{}, 1)}
}

func (q *cumulativeAckQueue) offer(epoch, state uint64) {
	if epoch == 0 || state == 0 {
		return
	}
	q.mu.Lock()
	if epoch > q.latestEpoch || (epoch == q.latestEpoch && state > q.latestState) {
		q.latestEpoch, q.latestState = epoch, state
	}
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *cumulativeAckQueue) take() (uint64, uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	epoch, state := q.latestEpoch, q.latestState
	q.latestEpoch, q.latestState = 0, 0
	return epoch, state
}

// runSender preserves normal-frame order while allowing switch control and
// output ACKs to make progress when raw input is held for a same-peer switch.
func runSender(ctx context.Context, cancel context.CancelFunc, transport ports.Transport, control <-chan ports.Frame, barriers <-chan chan struct{}, in <-chan ports.Frame, inputGate *samePeerInputGate, acks *cumulativeAckQueue, errCh chan<- error, log *slog.Logger) {
	defer log.Debug("sender pump exited")
	send := func(f ports.Frame) bool {
		if err := transport.Send(f); err != nil {
			select {
			case errCh <- err:
			default:
			}
			cancel()
			return false
		}
		return true
	}
	sendAck := func() bool {
		epoch, state := acks.take()
		if state == 0 {
			return true
		}
		payload, err := ports.MarshalAck(ports.Ack{Epoch: epoch, State: state})
		if err != nil {
			select {
			case errCh <- fmt.Errorf("encoding output ACK: %w", err):
			default:
			}
			cancel()
			return false
		}
		return send(ports.Frame{Type: ports.MsgAck, Payload: payload})
	}
	flushPending := func() bool {
		for {
			select {
			case <-acks.wake:
				if !sendAck() {
					return false
				}
				continue
			default:
			}
			if paused, _ := inputGate.snapshot(); paused {
				return sendAck()
			}
			select {
			case frame := <-in:
				if !send(frame) {
					return false
				}
			default:
				return sendAck()
			}
		}
	}
	completeBarrier := func(done chan struct{}) bool {
		if !flushPending() {
			return false
		}
		close(done)
		return true
	}
	var heldInput *ports.Frame
	for {
		if heldInput != nil {
			paused, changed := inputGate.snapshot()
			if !paused {
				if !send(*heldInput) {
					return
				}
				heldInput = nil
				continue
			}
			select {
			case <-acks.wake:
				if !sendAck() {
					return
				}
			case frame := <-control:
				if !send(frame) {
					return
				}
			case done := <-barriers:
				if !completeBarrier(done) {
					return
				}
			case <-changed:
			case <-ctx.Done():
				return
			}
			continue
		}
		select {
		case <-acks.wake:
			if !sendAck() {
				return
			}
			continue
		case frame := <-control:
			if !send(frame) {
				return
			}
			continue
		case done := <-barriers:
			if !completeBarrier(done) {
				return
			}
			continue
		default:
		}
		select {
		case <-acks.wake:
			if !sendAck() {
				return
			}
		case frame := <-control:
			if !send(frame) {
				return
			}
		case done := <-barriers:
			if !completeBarrier(done) {
				return
			}
		case frame := <-in:
			select {
			case <-ctx.Done():
				return
			default:
			}
			if frame.Type == ports.MsgInput || frame.Type == ports.MsgImagePush {
				if paused, _ := inputGate.snapshot(); paused {
					held := frame
					heldInput = &held
					if inputGate != nil && inputGate.afterInputHeld != nil {
						inputGate.afterInputHeld()
					}
					continue
				}
			}
			if !send(frame) {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// terminalReadResult is copied by terminalInputPump before the next Read so
// terminal input remains owned by one lifecycle reader across reconnects.
type terminalReadResult struct {
	data []byte
	err  error
}

// terminalInputPump is the sole consumer of the caller-owned terminal reader.
// It is started once after raw mode is entered and is reused by each attach
// scanner. A bare io.Reader cannot be interrupted, so a Read may stay blocked
// after Run exits; suspend drops any result received while inactive and never
// closes caller-owned stdin. Reconnects and later runs never add another reader.
type terminalInputPump struct {
	in   io.Reader
	done chan struct{}

	mu sync.Mutex
	// residual holds scanner bytes that were read but not delivered when an
	// attempt is cancelled. It is consumed before the next terminal Read so a
	// replacement scanner neither loses a partial marker nor replays bytes
	// whose callbacks already ran.
	residual           []byte
	pending            *terminalReadResult
	consumer           uint64
	delivering         uint64
	deliveringResidual bool
	nextID             uint64
	activation         uint64
	closed             bool
	active             bool
	ready              chan struct{}
	space              chan struct{}
	state              chan struct{}
	exited             chan struct{}
	afterRevoke        func() // test synchronization hook
	startMu            sync.Once
	stopMu             sync.Once
}

func newTerminalInputPump(in io.Reader) *terminalInputPump {
	space := make(chan struct{}, 1)
	space <- struct{}{}
	return &terminalInputPump{
		in:         in,
		done:       make(chan struct{}),
		activation: 1,
		active:     true,
		ready:      make(chan struct{}, 1),
		space:      space,
		state:      make(chan struct{}, 1),
		exited:     make(chan struct{}),
	}
}

// claim grants one attach scanner exclusive permission to dequeue terminal
// input. The reader itself remains lifecycle-owned across reconnects.
func (p *terminalInputPump) claim() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.consumer != 0 {
		panic("terminal input consumer already claimed")
	}
	p.nextID++
	p.consumer = p.nextID
	return p.consumer
}

// revoke invalidates an attempt before its replacement is allowed to claim
// input. Pending bytes are deliberately retained for that replacement.
func (p *terminalInputPump) revoke(consumer uint64) {
	p.mu.Lock()
	if p.consumer == consumer {
		p.consumer = 0
	}
	// An unacknowledged read was never delivered by this scanner. Leave it
	// pending and make it available to its replacement.
	if p.delivering == consumer {
		p.delivering = 0
		p.deliveringResidual = false
	}
	p.mu.Unlock()
	p.signalReady()
	if p.afterRevoke != nil {
		p.afterRevoke()
	}
}

func (p *terminalInputPump) signalReady() {
	select {
	case p.ready <- struct{}{}:
	default:
	}
}

// enqueue retains at most one completed terminal read. The reader blocks
// before publishing another result, keeping handoff buffering bounded.
func (p *terminalInputPump) enqueue(result terminalReadResult, activation uint64) bool {
	select {
	case <-p.done:
		return false
	case <-p.space:
	}
	p.mu.Lock()
	if p.closed || !p.active || p.activation != activation {
		p.mu.Unlock()
		p.space <- struct{}{}
		return false
	}
	p.pending = &result
	p.mu.Unlock()
	p.signalReady()
	return true
}

// take leases a ready result only to the current, non-cancelled consumer.
// The raw read remains pending until ack, so revocation or cancellation before
// scanner delivery leaves the exact bytes available to the next attempt.
func (p *terminalInputPump) take(ctx context.Context, consumer uint64) (terminalReadResult, bool) {
	p.mu.Lock()
	if ctx.Err() != nil || p.consumer != consumer || p.delivering != 0 {
		ready := (len(p.residual) != 0 || p.pending != nil) && p.delivering == 0
		p.mu.Unlock()
		if ready {
			p.signalReady()
		}
		return terminalReadResult{}, false
	}
	if len(p.residual) != 0 {
		result := terminalReadResult{data: append([]byte(nil), p.residual...)}
		p.delivering = consumer
		p.deliveringResidual = true
		p.mu.Unlock()
		return result, true
	}
	if p.pending == nil {
		p.mu.Unlock()
		return terminalReadResult{}, false
	}
	result := *p.pending
	p.delivering = consumer
	p.deliveringResidual = false
	p.mu.Unlock()
	return result, true
}

// preserveResidual records the undecided marker prefix after callbacks for the
// rest of its read have completed. It is intentionally bounded by the marker
// scanner's fixed response prefixes and is replayed before later terminal
// reads on a replacement attempt.
func (p *terminalInputPump) preserveResidual(consumer uint64, data []byte) {
	if len(data) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.consumer != consumer {
		return
	}
	p.residual = append(p.residual[:0], data...)
}

// ack commits scanner delivery of a leased read and lets the lifecycle reader
// accept the next result. Only the consumer that holds the lease can ack it.
func (p *terminalInputPump) ack(consumer uint64) {
	p.mu.Lock()
	if p.consumer != consumer || p.delivering != consumer {
		p.mu.Unlock()
		return
	}
	wasResidual := p.deliveringResidual
	if wasResidual {
		p.residual = nil
	} else {
		p.pending = nil
	}
	p.delivering = 0
	p.deliveringResidual = false
	more := len(p.residual) != 0 || p.pending != nil
	p.mu.Unlock()
	if wasResidual {
		if more {
			p.signalReady()
		}
		return
	}
	p.space <- struct{}{}
}

func (p *terminalInputPump) finish() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.signalReady()
	p.signalState()
}

func (p *terminalInputPump) signalState() {
	select {
	case p.state <- struct{}{}:
	default:
	}
}

func (p *terminalInputPump) waitForActive() (uint64, bool) {
	for {
		p.mu.Lock()
		active, closed, activation := p.active, p.closed, p.activation
		p.mu.Unlock()
		if closed {
			return 0, false
		}
		if active {
			return activation, true
		}
		select {
		case <-p.done:
			return 0, false
		case <-p.state:
		}
	}
}

func (p *terminalInputPump) start() {
	p.startMu.Do(func() {
		go func() {
			defer close(p.exited)
			buf := make([]byte, stdinBufSize)
			for {
				activation, ok := p.waitForActive()
				if !ok {
					return
				}
				n, err := p.in.Read(buf)
				result := terminalReadResult{err: err}
				if n > 0 {
					result.data = append([]byte(nil), buf[:n]...)
				}
				if !p.enqueue(result, activation) {
					if p.isClosed() {
						return
					}
					continue
				}
				if err != nil {
					p.finish()
					return
				}
			}
		}()
	})
}

func (p *terminalInputPump) hasExited() bool {
	select {
	case <-p.exited:
		return true
	default:
		return false
	}
}

func (p *terminalInputPump) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// suspend drops input completed after an attach ends and parks the lifecycle
// reader before its next Read. A later Run resumes this same reader, avoiding
// both input replay between runs and a competing read on caller-owned stdin.
func (p *terminalInputPump) suspend() {
	p.mu.Lock()
	p.active = false
	discarded := p.pending != nil
	p.residual = nil
	p.pending = nil
	p.delivering = 0
	p.deliveringResidual = false
	p.mu.Unlock()
	if discarded {
		select {
		case p.space <- struct{}{}:
		default:
		}
	}
	p.signalState()
	p.signalReady()
}

func (p *terminalInputPump) resume() {
	p.mu.Lock()
	if !p.closed && !p.active {
		p.activation++
		p.active = true
	}
	p.mu.Unlock()
	p.signalState()
}

func (p *terminalInputPump) stop() {
	p.stopMu.Do(func() {
		// Mark closure while holding the same mutex that publishes a completed
		// Read. A Read which returns after stop therefore cannot publish even
		// when both done and space are selectable in enqueue.
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		close(p.done)
		p.signalReady()
		p.signalState()
	})
}

// stdinPump parses terminal color reports into generation-tagged events while
// preserving all ordinary terminal input byte-for-byte. It never reads the
// terminal directly, publishes a Theme, or writes terminal output;
// attachAttempt owns those operations.
type foregroundSendLease struct {
	mu     sync.Mutex
	active bool
}

func newForegroundSendLease() *foregroundSendLease {
	return &foregroundSendLease{active: true}
}

func (l *foregroundSendLease) send(send func() bool) bool {
	if l == nil {
		return send()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active {
		return false
	}
	return send()
}

func (l *foregroundSendLease) stop() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.active = false
	l.mu.Unlock()
}

type stdinPump struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	in                  io.Reader // compatibility for isolated scanner tests
	input               *terminalInputPump
	consumer            uint64
	out                 chan<- ports.Frame
	clock               ports.Clock
	clipboard           ports.ClipboardReader
	logger              *slog.Logger
	paletteEvents       chan<- paletteGenerationEvent
	activeGeneration    *atomic.Uint64
	afterPaletteEvent   func(paletteGenerationEvent) // test synchronization hook
	afterInputTake      func()                       // test synchronization hook
	afterInputDelivered func()                       // test synchronization hook
	sendLease           *foregroundSendLease
}

func (p *stdinPump) run() {
	defer p.logger.Debug("stdin pump exited")
	var scanner theme.Scanner
	var markers paletteMarkerScanner
	var inputSeq uint64
	var sendOK atomic.Bool
	var undeliveredMu sync.Mutex
	var undelivered []byte
	sendOK.Store(true)
	send := func(frame ports.Frame) bool {
		if !sendOK.Load() {
			return false
		}
		return p.sendLease.send(func() bool {
			select {
			case p.out <- frame:
				if p.afterInputDelivered != nil {
					p.afterInputDelivered()
				}
				return true
			case <-p.ctx.Done():
				sendOK.Store(false)
				return false
			}
		})
	}
	sendEvent := func(event paletteGenerationEvent) bool {
		if !sendOK.Load() {
			return false
		}
		return p.sendLease.send(func() bool {
			select {
			case p.paletteEvents <- event:
				if p.afterPaletteEvent != nil {
					p.afterPaletteEvent(event)
				}
				return true
			case <-p.ctx.Done():
				sendOK.Store(false)
				return false
			}
		})
	}
	coalescer := newPasteCoalescer(p.clock, func(data []byte) {
		if len(data) == 0 {
			return
		}
		inputSeq++
		if !send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{InputSeq: inputSeq, Data: append([]byte(nil), data...)})}) {
			undeliveredMu.Lock()
			undelivered = append(undelivered, data...)
			undeliveredMu.Unlock()
		}
	})
	defer coalescer.Close()
	sink := coalescer.Scan
	if p.clipboard != nil {
		ci := &clipboardIntercept{
			coalescer: coalescer,
			reader:    p.clipboard,
			log:       p.logger,
			sendNotice: func(action uint8) {
				send(ports.Frame{Type: ports.MsgClientNotice, Payload: ports.MarshalClientNotice(ports.ClientNotice{Action: action})})
			},
			sendImage: func(mime string, data []byte) {
				inputSeq++
				send(ports.Frame{Type: ports.MsgImagePush, Payload: ports.MarshalImagePush(ports.ImagePush{InputSeq: inputSeq, Mime: mime, Data: data})})
			},
			next: coalescer.Scan,
		}
		sink = ci.Scan
	}
	input := p.input
	ownedInput := input == nil
	if ownedInput {
		input = newTerminalInputPump(p.in)
		input.start()
		defer input.stop()
	}
	consumer := p.consumer
	if consumer == 0 {
		consumer = input.claim()
		defer input.revoke(consumer)
	}
	// A marker prefix is ordinary terminal input until it becomes a complete
	// DECRQM reply. If cancellation wins while it is withheld, preserve just
	// that undecided suffix; the rest of its source read has already been
	// delivered and must not be replayed by the next attempt.
	defer func() {
		if p.ctx.Err() != nil {
			// Closing flushes any bracketed-paste prefix or buffer through the
			// coalescer's emit callback. Failed ordinary-input deliveries are
			// retained ahead of a trailing undecided marker prefix, matching their
			// original order within the raw read.
			held := coalescer.CloseAndTakeHeld()
			undeliveredMu.Lock()
			residual := append([]byte(nil), undelivered...)
			undeliveredMu.Unlock()
			residual = append(residual, held...)
			residual = append(residual, markers.takePending()...)
			input.preserveResidual(consumer, residual)
		}
	}()

	var markerTimer ports.Timer
	var markerTimerC <-chan time.Time
	flushMarkerPrefix := func() {
		markers.flush(sink)
		markerTimer = nil
		markerTimerC = nil
	}
	// Disarm before processing a subsequent read. A deadline already ready at
	// that boundary wins, so bytes cannot be retroactively consumed as a
	// marker after they were due to be forwarded as ordinary input.
	disarmMarkerDeadline := func() {
		if markerTimer == nil {
			return
		}
		select {
		case <-markerTimerC:
			flushMarkerPrefix()
			return
		default:
		}
		if !markerTimer.Stop() {
			// Stop reports false only after expiry or a prior stop. This timer is
			// owned only here, so expiry wins even if delivery to C races this
			// select; do not let a late value consume a newly arrived suffix.
			flushMarkerPrefix()
			return
		}
		markerTimer = nil
		markerTimerC = nil
	}
	armMarkerDeadline := func() {
		disarmMarkerDeadline()
		markerTimer = p.clock.NewTimer(paletteMarkerAmbiguityDeadline)
		markerTimerC = markerTimer.C()
	}
	defer disarmMarkerDeadline()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-markerTimerC:
			flushMarkerPrefix()
		case <-input.ready:
			result, ok := input.take(p.ctx, consumer)
			if !ok {
				if p.ctx.Err() != nil {
					return
				}
				continue
			}
			if p.afterInputTake != nil {
				p.afterInputTake()
			}
			// The lease is not acknowledged until scanner delivery completes.
			// This second check closes the cancellation window after take.
			if p.ctx.Err() != nil {
				return
			}
			disarmMarkerDeadline()
			// All scanner callbacks from this Read must retain this one generation.
			// A scheme notification can start its replacement before a later byte in
			// the same read is scanned; reloading there would misclassify an old
			// completion marker as the replacement's drain response.
			readGeneration := paletteGenerationID(p.activeGeneration.Load())
			if len(result.data) > 0 {
				scanner.Scan(result.data, func(kind int, rgb renderer.RGB) {
					kindEvent := paletteEventForeground
					if kind == 11 {
						kindEvent = paletteEventBackground
					}
					sendEvent(paletteGenerationEvent{id: readGeneration, kind: kindEvent, rgb: rgb})
				}, func(slot int, rgb renderer.RGB) {
					// Scanner implementations are independently testable; retain the
					// protocol boundary here too so a malformed implementation cannot
					// wrap a negative or oversized int into an ANSI palette slot.
					slot8, ok := paletteSlot(slot)
					if !ok {
						return
					}
					sendEvent(paletteGenerationEvent{id: readGeneration, kind: paletteEventPalette, slot: slot8, rgb: rgb})
				}, func(light bool) {
					sendEvent(paletteGenerationEvent{id: readGeneration, kind: paletteEventScheme, light: light})
				}, func(data []byte) {
					markers.scan(data, sink, func() {
						sendEvent(paletteGenerationEvent{id: readGeneration, kind: paletteEventMarker})
					})
				})
				if !sendOK.Load() {
					// Commit the source read so callbacks accepted before cancellation
					// are never replayed. The deferred handoff retains only ordinary
					// bytes whose callback could not be delivered, plus any undecided
					// trailing marker prefix.
					input.ack(consumer)
					return
				}
			}
			if markers.hasPendingPrefix() {
				armMarkerDeadline()
			}
			if result.err != nil {
				disarmMarkerDeadline()
				markers.flush(sink)
				select {
				case p.out <- ports.Frame{Type: ports.MsgDetach, Payload: ports.MarshalDetach(ports.Detach{})}:
				case <-p.ctx.Done():
				}
				p.cancel()
				return
			}
			// Commit every callback from this raw read before observing a
			// concurrent cancellation. Any remaining marker prefix is retained
			// by the deferred handoff above, so reconnects never duplicate the
			// delivered bytes or lose a standalone Escape.
			input.ack(consumer)
			if p.ctx.Err() != nil {
				return
			}
		}
	}
}

// runResize forwards coalesced terminal resize events to the daemon. It
// tolerates an already-closed resize channel (which the terminal adapter
// hands back when restore ran before ResizeEvents was first called).
func runResize(ctx context.Context, events <-chan domain.Geometry, out chan<- ports.Frame, sendLease *foregroundSendLease, log *slog.Logger) {
	defer log.Debug("resize pump exited")
	for {
		select {
		case geometry, ok := <-events:
			if !ok {
				return
			}
			geometry = geometry.NormalizePixels()
			payload, err := ports.MarshalResize(ports.Resize{Size: geometry.Size, PixelWidth: geometry.PixelWidth, PixelHeight: geometry.PixelHeight})
			if err != nil {
				log.Error("encoding terminal resize", "error", err)
				continue
			}
			frame := ports.Frame{Type: ports.MsgResize, Payload: payload}
			if !sendLease.send(func() bool {
				select {
				case out <- frame:
					return true
				case <-ctx.Done():
					return false
				}
			}) {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// detachedResult maps a Detached reason to a return value: an ordinary
// user-initiated detach is a clean (nil) exit; anything else is surfaced so
// the user learns why the session went away.
func detachedResult(reason uint8) error {
	switch reason {
	case ports.ReasonDetach:
		return nil
	case ports.ReasonSessionKilled:
		return &DetachedError{Reason: reason, Text: "session was killed"}
	case ports.ReasonServerShutdown:
		return &DetachedError{Reason: reason, Text: "daemon shut down"}
	case ports.ReasonReplaced:
		return &DetachedError{Reason: reason, Text: "session taken over by another client"}
	default:
		return &DetachedError{Reason: reason, Text: "detached by daemon"}
	}
}
