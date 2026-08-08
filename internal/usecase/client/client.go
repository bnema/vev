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
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
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
}

// AttachRequest identifies the session for one client run. Remote is
// client-only presentation metadata. RemoteTarget is an optional exact picker
// handoff and is serialized into Hello only when present.
type AttachRequest struct {
	Intent                 uint8
	SessionName            string
	Remote                 bool
	RemoteTarget           *domain.RemoteSessionTarget
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

	inputMu sync.Mutex
	input   *terminalInputPump
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

func validateAttachRequest(request AttachRequest) error {
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

	for {
		if err := validateAttachRequest(attemptRequest); err != nil {
			return err
		}
		handshakeCtx, timedOut, finishHandshake := newHandshakeContext(ctx, r.clock)
		transport, err := boundedDial(handshakeCtx, dialer)
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
			runner:                 r,
			connection:             connection,
			remote:                 remote,
			handshakeCtx:           handshakeCtx,
			handshakeTimedOut:      timedOut,
			finishHandshake:        finishHandshake,
			stopHandshakeTransport: stopHandshakeTransport,
			request:                attemptRequest,
			resumeToken:            resumeToken,
			clientID:               processClientID,
			milestones:             &ms,
			themeState:             themeState,
			enterRaw:               enterRaw,
			reconnect:              reconnect,
			linkEvents:             linkEvents,
			rememberRemoteHost:     rememberRemoteHost,
			terminalInput: func() *terminalInputPump {
				return input
			},
		}).run(ctx)
		stopHandshakeTransport()
		finishHandshake()
		if result.welcomed {
			backoff = defaultReconnectBackoff.initial
		}
		if !result.transportClosed {
			_ = connection.Close()
		}
		if result.action != 0 {
			switch result.action {
			case ports.NavigationOpenHomePicker:
				if attemptRequest.NavigationCapabilities&ports.NavigationCapabilityHomePicker == 0 || homeRoute == nil {
					return errors.New("vev: stale home navigation action")
				}
				returnRequest := attemptRequest
				if result.sessionName != "" {
					returnRequest.SessionName = result.sessionName
				}
				returnRoute = &attachRoute{dialer: dialer, request: returnRequest, resumeToken: result.resumeToken}
				homeNavigationPending = true
				dialer = homeRoute.dialer
				attemptRequest = homeRoute.request
				attemptRequest.Remote = homeRoute.request.Remote || r.remote
				attemptRequest.Intent = ports.IntentAttach
				attemptRequest.NavigationCapabilities = ports.NavigationCapabilityBack
				attemptRequest.StartupOverlay = ports.StartupOverlaySessionPicker
				attemptRequest.RemoteTarget = nil
				attemptRequest.EnvironmentPolicy = ports.EnvironmentPolicyClientOwned
				remote = syncReconnectRemote(reconnect, homeRoute.request.Remote || r.remote)
				resumeToken = 0
				backoff = defaultReconnectBackoff.initial
				continue
			case ports.NavigationBack:
				if attemptRequest.StartupOverlay != ports.StartupOverlaySessionPicker || returnRoute == nil || attemptRequest.NavigationCapabilities&ports.NavigationCapabilityBack == 0 {
					return errors.New("vev: stale return navigation action")
				}
				route := *returnRoute
				returnRoute = nil
				homeNavigationPending = false
				returnNavigationPending = true
				dialer = route.dialer
				attemptRequest = route.request
				attemptRequest.Intent = ports.IntentResume
				attemptRequest.StartupOverlay = ports.StartupOverlayNone
				resumeToken = route.resumeToken
				returnResumeFallback = true
				remote = syncReconnectRemote(reconnect, attemptRequest.Remote || r.remote)
				backoff = defaultReconnectBackoff.initial
				continue
			default:
				return errors.New("vev: unsupported navigation action")
			}
		}
		if result.target != nil {
			if r.attachHandoff == nil {
				return &AttachTargetError{Target: *result.target}
			}
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
			nextDialer, nextRequest, handoffErr := r.attachHandoff(*result.target)
			if handoffErr != nil {
				return handoffErr
			}
			if homeRoute != nil {
				nextRequest.NavigationCapabilities |= ports.NavigationCapabilityHomePicker
			}
			if attemptRequest.StartupOverlay == ports.StartupOverlaySessionPicker {
				returnRoute = nil
				returnResumeFallback = false
			}
			nextRequest = cloneAttachRequest(nextRequest)
			if err := validateAttachRequest(nextRequest); err != nil {
				return fmt.Errorf("vev: invalid remote attach handoff request: %w", err)
			}
			if nextDialer == nil {
				return errors.New("vev: remote attach handoff returned nil dialer")
			}
			dialer = nextDialer
			attemptRequest = nextRequest
			remote = syncReconnectRemote(reconnect, attemptRequest.Remote || r.remote)
			resumeToken = 0
			backoff = defaultReconnectBackoff.initial
			continue
		}
		if result.resumeToken != 0 {
			resumeToken = result.resumeToken
		}
		if result.sessionName != "" {
			attemptRequest.SessionName = result.sessionName
		}
		if result.welcomed {
			homeNavigationPending = false
		}
		if result.err == nil {
			if result.welcomed && returnNavigationPending {
				returnRoute = nil
				returnNavigationPending = false
				returnResumeFallback = false
			}
			if clearErr := reconnect.clear(); clearErr != nil {
				return clearErr
			}
			return nil
		}
		if homeNavigationPending && returnRoute != nil {
			route := *returnRoute
			returnRoute = nil
			homeNavigationPending = false
			returnNavigationPending = true
			returnResumeFallback = true
			dialer = route.dialer
			attemptRequest = route.request
			attemptRequest.Intent = ports.IntentResume
			attemptRequest.StartupOverlay = ports.StartupOverlayNone
			resumeToken = route.resumeToken
			remote = syncReconnectRemote(reconnect, attemptRequest.Remote || r.remote)
			backoff = defaultReconnectBackoff.initial
			continue
		}
		if returnResumeFallback {
			var protocolErr *ProtocolError
			if errors.As(result.err, &protocolErr) && (protocolErr.Code == ports.ErrNoSuchSession || protocolErr.Code == ports.ErrNoSuchTarget) {
				attemptRequest.Intent = ports.IntentAttach
				resumeToken = 0
				returnResumeFallback = false
				continue
			}
		}
		if returnNavigationPending && !returnResumeFallback && homeRoute != nil {
			returnRoute = nil
			returnNavigationPending = false
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
	runner                 *Runner
	connection             *SessionConnection
	remote                 bool
	transport              ports.Transport
	handshakeCtx           context.Context
	handshakeTimedOut      <-chan struct{}
	finishHandshake        func()
	stopHandshakeTransport func()
	request                AttachRequest
	resumeToken            uint64
	clientID               [16]byte
	milestones             *milestones
	themeState             *terminalThemeState
	enterRaw               func() error
	reconnect              *reconnectUI
	linkEvents             <-chan ports.LinkEvent
	rememberRemoteHost     func()
	terminalInput          func() *terminalInputPump
}

type attachResult struct {
	resumeToken     uint64
	sessionName     string
	welcomed        bool
	transportClosed bool
	target          *ports.AttachTarget
	action          ports.NavigationAction
	err             error
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
		err := boundedHandshakeOperation(handshakeCtx, transport, operation)
		if err != nil {
			return handshakeContextError(ctx, handshakeTimedOut, err)
		}
		return nil
	}
	// 1. Handshake: send Hello with our size and TERM.
	size, err := term.Size()
	if err != nil {
		return attachResult{err: fmt.Errorf("vev: reading terminal size: %w", err)}
	}
	cwd, err := os.Getwd()
	if err != nil {
		cwd = ""
	}
	termEnv := os.Getenv("TERM")
	colorTerm := os.Getenv("COLORTERM")
	trueColor := DetectTrueColor(termEnv, colorTerm)
	themeState.setTrueColor(trueColor)
	hello := ports.Hello{
		Version:                ports.ProtocolVersion,
		Intent:                 intent,
		ClientID:               clientID,
		ResumeToken:            resumeToken,
		Name:                   name,
		Size:                   size,
		TermEnv:                termEnv,
		Cwd:                    cwd,
		TrueColor:              trueColor,
		MaxOutputInFlight:      requestedOutputWindow(transport),
		Env:                    os.Environ(),
		RemoteTarget:           request.RemoteTarget,
		EnvironmentPolicy:      request.EnvironmentPolicy,
		NavigationCapabilities: request.NavigationCapabilities,
		StartupOverlay:         request.StartupOverlay,
	}
	if err := sendHandshake(func() error {
		return transport.Send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)})
	}); err != nil {
		return handshakeFailure("sending hello", err)
	}
	ms.helloSent = true

	// 2. Await Welcome or a typed rejection.
	var reply ports.Frame
	if err := boundedHandshakeOperation(handshakeCtx, transport, func() error {
		var recvErr error
		reply, recvErr = transport.Recv()
		return recvErr
	}); err != nil {
		return handshakeFailure("awaiting welcome", err)
	}
	result := func(welcomed bool, err error) attachResult {
		return attachResult{resumeToken: resumeToken, sessionName: name, welcomed: welcomed, err: err}
	}
	switch reply.Type {
	case ports.MsgWelcome:
		welcome, derr := ports.UnmarshalWelcome(reply.Payload)
		if derr != nil {
			return result(false, fmt.Errorf("vev: decoding welcome: %w", derr))
		}
		resumeToken = welcome.ResumeToken
		name = welcome.SessionName
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
	inputConsumer := input.claim()
	claimActive := true
	revokeInputClaim := func() {
		if claimActive {
			input.revoke(inputConsumer)
			claimActive = false
		}
	}
	// Register revocation before any palette publication or terminal I/O: both
	// can fail before the stdin scanner is started, and a reconnect must still
	// be able to claim this lifecycle-owned reader.
	defer revokeInputClaim()

	sendCh := make(chan ports.Frame, sendQueueDepth)
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
	senderStarted = true
	go runSender(loopCtx, cancel, transport, sendCh, ackQueue, sendErrCh, log)
	stdinDone := make(chan struct{})
	go func() {
		defer close(stdinDone)
		(&stdinPump{ctx: loopCtx, cancel: cancel, input: input, consumer: inputConsumer, out: sendCh, clock: clk, clipboard: clip, logger: log, paletteEvents: paletteEvents, activeGeneration: &activeGeneration}).run()
	}()
	// Scanner cancellation can leave an undecided marker suffix (including a
	// standalone Escape) that it must hand back to the lifecycle-owned reader.
	// Wait for that handoff before revoking the claim: otherwise a replacement
	// scanner could claim the reader between revocation and preservation and
	// lose the suffix.
	defer func() {
		cancel()
		<-stdinDone
		revokeInputClaim()
	}()
	go runResize(loopCtx, term.ResizeEvents(), sendCh, log)

	// 5. Output/main loop: the only goroutine that touches the terminal.
	recvCh := make(chan recvResult, 1)
	go runRecv(loopCtx, transport, recvCh, log)

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
				handoff := welcomedResult(nil)
				handoff.target = &target
				return handoff
			case ports.MsgNavigationAction:
				action, derr := ports.UnmarshalNavigationAction(r.frame.Payload)
				if derr != nil {
					return welcomedResult(fmt.Errorf("vev: decoding navigation action: %w", derr))
				}
				navigation := welcomedResult(nil)
				navigation.action = action
				return navigation
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
func runRecv(ctx context.Context, transport ports.Transport, out chan<- recvResult, log *slog.Logger) {
	defer log.Debug("receive pump exited")
	for {
		f, err := transport.Recv()
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

func runSender(ctx context.Context, cancel context.CancelFunc, transport ports.Transport, in <-chan ports.Frame, acks *cumulativeAckQueue, errCh chan<- error, log *slog.Logger) {
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
	for {
		select {
		case <-acks.wake:
			if !sendAck() {
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
		case f := <-in:
			select {
			case <-ctx.Done():
				return
			default:
			}
			if !send(f) {
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
	}
	sendEvent := func(event paletteGenerationEvent) bool {
		if !sendOK.Load() {
			return false
		}
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
func runResize(ctx context.Context, events <-chan domain.Size, out chan<- ports.Frame, log *slog.Logger) {
	defer log.Debug("resize pump exited")
	for {
		select {
		case sz, ok := <-events:
			if !ok {
				return
			}
			payload, err := ports.MarshalResize(ports.Resize{Size: sz})
			if err != nil {
				log.Error("encoding terminal resize", "error", err)
				continue
			}
			frame := ports.Frame{Type: ports.MsgResize, Payload: payload}
			select {
			case out <- frame:
			case <-ctx.Done():
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
