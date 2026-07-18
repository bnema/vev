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

// preWelcomeTimeout bounds each blocking operation before the daemon has
// accepted the attach. Mosh uses the same 15-second startup budget.
const preWelcomeTimeout = 15 * time.Second

var reconnectSleep = sleepReconnect
var reconnectSleepWithResize = sleepReconnectWithResizeEvents

const (
	statusReconnect = "\r\x1b[2Kreconnecting…"
	statusClear     = "\r\x1b[2K"
)

// Dependencies supplies the collaborators required by a Runner.
type Dependencies struct {
	Dialer          ports.Dialer
	Terminal        ports.Terminal
	Clock           ports.Clock
	Clipboard       ports.ClipboardReader
	Logger          *slog.Logger
	RuntimeObserver ports.SerializedRuntimeObserver
}

// AttachRequest identifies the session and transport mode for one client run.
type AttachRequest struct {
	Intent      uint8
	SessionName string
	Remote      bool
}

// Runner owns the client lifecycle across one or more attachment attempts.
type Runner struct {
	dialer          ports.Dialer
	term            ports.Terminal
	clock           ports.Clock
	clipboard       ports.ClipboardReader
	logger          *slog.Logger
	runtimeObserver ports.SerializedRuntimeObserver

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
		dialer:          deps.Dialer,
		term:            deps.Terminal,
		clock:           deps.Clock,
		clipboard:       deps.Clipboard,
		logger:          log,
		runtimeObserver: deps.RuntimeObserver,
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

// Run connects and runs the attach client. It owns the terminal lifecycle
// above attach attempts so raw mode remains active while a live client process
// redials a lost link.
func (r *Runner) Run(ctx context.Context, request AttachRequest) (retErr error) {
	ms := milestones{}

	defer func() {
		if retErr != nil {
			r.logger.Error("attach ended with error", "err", retErr, "missing_milestones", ms.missing())
		}
	}()

	var restore func() error
	rawEntered := false
	defer func() {
		if rawEntered {
			if rerr := restore(); rerr != nil && retErr == nil {
				retErr = fmt.Errorf("vev: restoring terminal: %w", rerr)
			}
		}
	}()
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
	backoff := defaultReconnectBackoff.initial
	themeState := &terminalThemeState{}
	reconnect := &reconnectUI{
		term:       r.term,
		remote:     request.Remote,
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
		transport, err := r.dialer.Dial(ctx)
		if err != nil {
			if resumeToken == 0 || ctx.Err() != nil {
				reconnect.clear()
				return err
			}
			r.logger.Warn("reconnect dial failed", "err", err, "backoff", backoff)
			reconnect.draw()
			if !reconnect.sleep(ctx, r.clock, backoff) {
				reconnect.clear()
				return ctx.Err()
			}
			backoff = nextReconnectBackoff(backoff, defaultReconnectBackoff.max)
			attemptRequest.Intent = ports.IntentResume
			continue
		}
		ms.dialed = true

		var linkEvents <-chan ports.LinkEvent
		if reporter, ok := transport.(ports.LinkStateReporter); ok {
			linkEvents = reporter.LinkEvents()
		}
		result := (&attachAttempt{
			runner:      r,
			transport:   transport,
			request:     attemptRequest,
			resumeToken: resumeToken,
			clientID:    processClientID,
			milestones:  &ms,
			themeState:  themeState,
			enterRaw:    enterRaw,
			reconnect:   reconnect,
			linkEvents:  linkEvents,
			terminalInput: func() *terminalInputPump {
				return input
			},
		}).run(ctx)
		if result.welcomed {
			backoff = defaultReconnectBackoff.initial
		}
		if !result.transportClosed {
			_ = transport.Close()
		}
		if result.resumeToken != 0 {
			resumeToken = result.resumeToken
		}
		if result.sessionName != "" {
			attemptRequest.SessionName = result.sessionName
		}
		if result.err == nil {
			reconnect.clear()
			return nil
		}
		if !shouldReconnect(result.err) || resumeToken == 0 || ctx.Err() != nil {
			reconnect.clear()
			return result.err
		}
		r.logger.Warn("reconnecting after attach error", "err", result.err, "backoff", backoff)
		reconnect.drawStage(reconnectStageSSH)
		if !reconnect.sleep(ctx, r.clock, backoff) {
			reconnect.clear()
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
	rect       domain.Rect
	stage      reconnectStage
}

func (u *reconnectUI) redraw(size domain.Size) {
	if !*u.rawEntered || !u.remote {
		return
	}
	if u.showing {
		_ = clearReconnectToast(u.term.Out(), u.rect)
	}
	drawnRect, _ := drawReconnectToastStage(u.term.Out(), size, u.stage)
	u.rect = drawnRect
	_ = u.term.Flush()
	u.showing = true
}

func (u *reconnectUI) drawStage(stage reconnectStage) {
	u.stage = stage
	if u.showing && u.remote {
		size, err := u.term.Size()
		if err == nil {
			u.redraw(size)
		}
		return
	}
	u.draw()
}

func (u *reconnectUI) draw() {
	if !*u.rawEntered || u.showing {
		return
	}
	if u.remote {
		size, err := u.term.Size()
		if err != nil {
			return
		}
		u.redraw(size)
		return
	}
	_, _ = u.term.Out().Write([]byte(statusReconnect))
	_ = u.term.Flush()
	u.showing = true
}

func (u *reconnectUI) clear() {
	if !u.showing {
		return
	}
	if u.remote {
		_ = clearReconnectToast(u.term.Out(), u.rect)
	} else {
		_, _ = u.term.Out().Write([]byte(statusClear))
	}
	_ = u.term.Flush()
	u.showing = false
}

func (u *reconnectUI) sleep(ctx context.Context, clk ports.Clock, d time.Duration) bool {
	if u.remote && u.showing {
		return reconnectSleepWithResize(ctx, clk, d, u.term.ResizeEvents(), u.redraw)
	}
	return reconnectSleep(ctx, clk, d)
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

func sleepReconnectWithResizeEvents(ctx context.Context, clk ports.Clock, d time.Duration, resizeEvents <-chan domain.Size, onResize func(domain.Size)) bool {
	t := clk.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-t.C():
			return true
		case <-ctx.Done():
			return false
		case size, ok := <-resizeEvents:
			if !ok {
				resizeEvents = nil
				continue
			}
			if onResize != nil {
				onResize(size)
			}
		}
	}
}

type attachAttempt struct {
	runner        *Runner
	transport     ports.Transport
	request       AttachRequest
	resumeToken   uint64
	clientID      [16]byte
	milestones    *milestones
	themeState    *terminalThemeState
	enterRaw      func() error
	reconnect     *reconnectUI
	linkEvents    <-chan ports.LinkEvent
	terminalInput func() *terminalInputPump
}

type attachResult struct {
	resumeToken     uint64
	sessionName     string
	welcomed        bool
	transportClosed bool
	err             error
}

// boundedPreWelcome runs one blocking handshake operation. Transport methods
// do not accept a context, so cancellation and expiry close this attempt's
// transport to interrupt the call, then wait for its goroutine to exit.
func boundedPreWelcome(ctx context.Context, clk ports.Clock, transport ports.Transport, operation func() error) (bool, error) {
	if err := ctx.Err(); err != nil {
		_ = transport.Close()
		return true, err
	}

	timer := clk.NewTimer(preWelcomeTimeout)
	defer timer.Stop()
	completed := make(chan error, 1)
	go func() { completed <- operation() }()

	select {
	case err := <-completed:
		// Prefer a cancellation/expiry observed concurrently with completion.
		// That event owns the transport lifetime even though the operation has
		// already returned by the time we observe it here.
		if ctx.Err() != nil {
			_ = transport.Close()
			return true, ctx.Err()
		}
		select {
		case <-timer.C():
			_ = transport.Close()
			return true, context.DeadlineExceeded
		default:
			return false, err
		}
	case <-ctx.Done():
		_ = transport.Close()
		<-completed
		return true, ctx.Err()
	case <-timer.C():
		_ = transport.Close()
		<-completed
		return true, context.DeadlineExceeded
	}
}

// DetectTrueColor reports whether TERM/COLORTERM advertise direct color support.
func DetectTrueColor(termEnv, colorTerm string) bool {
	switch strings.ToLower(strings.TrimSpace(colorTerm)) {
	case "truecolor", "24bit":
		return true
	}

	termEnv = strings.ToLower(strings.TrimSpace(termEnv))
	return termEnv == "xterm-direct" || strings.HasSuffix(termEnv, "-direct")
}

func requestedOutputWindow(transport ports.Transport) uint8 {
	if _, ok := transport.(ports.DatagramTransport); ok {
		return 1
	}
	return 8
}

func (a *attachAttempt) run(ctx context.Context) attachResult {
	transport := a.transport
	term := a.runner.term
	clk := a.runner.clock
	intent := a.request.Intent
	name := a.request.SessionName
	resumeToken := a.resumeToken
	clientID := a.clientID
	ms := a.milestones
	themeState := a.themeState
	enterRaw := a.enterRaw
	reconnect := a.reconnect
	linkEvents := a.linkEvents
	remote := a.request.Remote
	clipboard := a.runner.clipboard
	log := a.runner.logger
	observer := a.runner.runtimeObserver
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
		Version:           ports.ProtocolVersion,
		Intent:            intent,
		ClientID:          clientID,
		ResumeToken:       resumeToken,
		Name:              name,
		Size:              size,
		TermEnv:           termEnv,
		Cwd:               cwd,
		TrueColor:         trueColor,
		MaxOutputInFlight: requestedOutputWindow(transport),
		Env:               os.Environ(),
	}
	if closed, err := boundedPreWelcome(ctx, clk, transport, func() error {
		return transport.Send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)})
	}); err != nil {
		return attachResult{transportClosed: closed, err: fmt.Errorf("vev: sending hello: %w", err)}
	}
	ms.helloSent = true

	// 2. Await Welcome or a typed rejection.
	var reply ports.Frame
	closed, err := boundedPreWelcome(ctx, clk, transport, func() error {
		var recvErr error
		reply, recvErr = transport.Recv()
		return recvErr
	})
	if err != nil {
		return attachResult{transportClosed: closed, err: fmt.Errorf("vev: awaiting welcome: %w", err)}
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
	case ports.MsgError:
		em, derr := ports.UnmarshalErrorMsg(reply.Payload)
		if derr != nil {
			return result(false, fmt.Errorf("vev: decoding error reply: %w", derr))
		}
		return result(false, &ProtocolError{Code: em.Code, Text: em.Text})
	default:
		return result(false, fmt.Errorf("vev: unexpected reply type %d before welcome", reply.Type))
	}
	welcomedResult := func(err error) attachResult { return result(true, err) }

	// 3. Enter raw mode after Welcome; Run owns restoration.
	if err := enterRaw(); err != nil {
		return welcomedResult(err)
	}
	reconnect.clear()

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
			if err := transport.Send(frame); err != nil {
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
	senderStarted = true
	go runSender(loopCtx, cancel, transport, sendCh, ackQueue, sendErrCh, log)
	stdinDone := make(chan struct{})
	go func() {
		defer close(stdinDone)
		(&stdinPump{ctx: loopCtx, cancel: cancel, input: input, consumer: inputConsumer, out: sendCh, clock: clk, clipboard: clip, logger: log, paletteEvents: paletteEvents, activeGeneration: &activeGeneration}).run()
	}()
	// Revoke this scanner's dequeue permission before waiting for it to exit.
	// The lifecycle-owned reader retains queued bytes for the next attempt.
	defer func() {
		// Revoke before scanner teardown so replacement attempts can claim the
		// lifecycle-owned reader as soon as this attempt releases it.
		revokeInputClaim()
		cancel()
		<-stdinDone
	}()
	go runResize(loopCtx, term.ResizeEvents(), sendCh, log)

	// 5. Output/main loop: the only goroutine that touches the terminal.
	recvCh := make(chan recvResult, 1)
	go runRecv(loopCtx, transport, recvCh, log)

	for {
		select {
		case <-loopCtx.Done():
			// A pump initiated shutdown (stdin EOF/detach) or the parent
			// context was cancelled. A queued sender error takes priority.
			select {
			case serr := <-sendErrCh:
				return welcomedResult(fmt.Errorf("vev: sending to daemon: %w", serr))
			default:
				return welcomedResult(nil)
			}
		case ev, ok := <-linkEvents:
			if !ok {
				linkEvents = nil
				continue
			}
			if ev.State == ports.LinkStateConnected {
				reconnect.clear()
				continue
			}
			if ev.State == ports.LinkStateDegraded {
				log.Warn("UDP link degraded")
				reconnect.clear()
				continue
			}
			reconnect.drawStage(stageForLinkState(ev.State))
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
				if _, werr := term.Out().Write(o.Data); werr != nil {
					return welcomedResult(fmt.Errorf("vev: writing terminal output: %w", werr))
				}
				if ferr := term.Flush(); ferr != nil {
					return welcomedResult(fmt.Errorf("vev: flushing terminal: %w", ferr))
				}
				// The terminal boundary is after a successful flush and before ACK.
				if observer != nil {
					observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeTerminalFlushed, uint64(len(o.Data)), true))
				}
				if o.NewStateNum != 0 {
					ackQueue.offer(o.NewStateNum)
				}
				if !ms.firstOutput {
					ms.firstOutput = true
					log.Debug("received first output")
				}
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
	latest atomic.Uint64
	wake   chan struct{}
}

func newCumulativeAckQueue() *cumulativeAckQueue {
	return &cumulativeAckQueue{wake: make(chan struct{}, 1)}
}

func (q *cumulativeAckQueue) offer(state uint64) {
	for {
		current := q.latest.Load()
		if state <= current || q.latest.CompareAndSwap(current, state) {
			break
		}
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
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
		state := acks.latest.Swap(0)
		return state == 0 || send(ports.Frame{Type: ports.MsgAck, Payload: ports.MarshalAck(ports.Ack{AckedStateNum: state})})
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

	mu         sync.Mutex
	pending    *terminalReadResult
	consumer   uint64
	delivering uint64
	nextID     uint64
	closed     bool
	active     bool
	ready      chan struct{}
	space      chan struct{}
	state      chan struct{}
	exited     chan struct{}
	startMu    sync.Once
	stopMu     sync.Once
}

func newTerminalInputPump(in io.Reader) *terminalInputPump {
	space := make(chan struct{}, 1)
	space <- struct{}{}
	return &terminalInputPump{
		in:     in,
		done:   make(chan struct{}),
		active: true,
		ready:  make(chan struct{}, 1),
		space:  space,
		state:  make(chan struct{}, 1),
		exited: make(chan struct{}),
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
	}
	p.mu.Unlock()
	p.signalReady()
}

func (p *terminalInputPump) signalReady() {
	select {
	case p.ready <- struct{}{}:
	default:
	}
}

// enqueue retains at most one completed terminal read. The reader blocks
// before publishing another result, keeping handoff buffering bounded.
func (p *terminalInputPump) enqueue(result terminalReadResult) bool {
	select {
	case <-p.done:
		return false
	case <-p.space:
	}
	p.mu.Lock()
	if p.closed || !p.active {
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
	if ctx.Err() != nil || p.consumer != consumer || p.pending == nil || p.delivering != 0 {
		pending := p.pending != nil && p.delivering == 0
		p.mu.Unlock()
		if pending {
			p.signalReady()
		}
		return terminalReadResult{}, false
	}
	result := *p.pending
	p.delivering = consumer
	p.mu.Unlock()
	return result, true
}

// ack commits scanner delivery of a leased read and lets the lifecycle reader
// accept the next result. Only the consumer that holds the lease can ack it.
func (p *terminalInputPump) ack(consumer uint64) {
	p.mu.Lock()
	if p.consumer != consumer || p.delivering != consumer {
		p.mu.Unlock()
		return
	}
	p.pending = nil
	p.delivering = 0
	p.mu.Unlock()
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

func (p *terminalInputPump) waitForActive() bool {
	for {
		p.mu.Lock()
		active, closed := p.active, p.closed
		p.mu.Unlock()
		if closed {
			return false
		}
		if active {
			return true
		}
		select {
		case <-p.done:
			return false
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
				if !p.waitForActive() {
					return
				}
				n, err := p.in.Read(buf)
				result := terminalReadResult{err: err}
				if n > 0 {
					result.data = append([]byte(nil), buf[:n]...)
				}
				if !p.enqueue(result) {
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
	p.pending = nil
	p.delivering = 0
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
	if !p.closed {
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
	ctx               context.Context
	cancel            context.CancelFunc
	in                io.Reader // compatibility for isolated scanner tests
	input             *terminalInputPump
	consumer          uint64
	out               chan<- ports.Frame
	clock             ports.Clock
	clipboard         ports.ClipboardReader
	logger            *slog.Logger
	paletteEvents     chan<- paletteGenerationEvent
	activeGeneration  *atomic.Uint64
	afterPaletteEvent func(paletteGenerationEvent) // test synchronization hook
	afterInputTake    func()                       // test synchronization hook
}

func (p *stdinPump) run() {
	defer p.logger.Debug("stdin pump exited")
	var scanner theme.Scanner
	var markers paletteMarkerScanner
	var inputSeq uint64
	var sendOK atomic.Bool
	sendOK.Store(true)
	send := func(frame ports.Frame) {
		select {
		case p.out <- frame:
		case <-p.ctx.Done():
			sendOK.Store(false)
		}
	}
	sendEvent := func(event paletteGenerationEvent) {
		select {
		case p.paletteEvents <- event:
			if p.afterPaletteEvent != nil {
				p.afterPaletteEvent(event)
			}
		case <-p.ctx.Done():
			sendOK.Store(false)
		}
	}
	coalescer := newPasteCoalescer(p.clock, func(data []byte) {
		if len(data) == 0 {
			return
		}
		inputSeq++
		send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{InputSeq: inputSeq, Data: append([]byte(nil), data...)})})
	})
	defer coalescer.Close()
	sink := coalescer.Scan
	if p.clipboard != nil {
		ci := &clipboardIntercept{
			coalescer: coalescer,
			reader:    p.clipboard,
			log:       p.logger,
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
					sendEvent(paletteGenerationEvent{id: readGeneration, kind: paletteEventPalette, slot: uint8(slot), rgb: rgb})
				}, func(light bool) {
					sendEvent(paletteGenerationEvent{id: readGeneration, kind: paletteEventScheme, light: light})
				}, func(data []byte) {
					markers.scan(data, sink, func() {
						sendEvent(paletteGenerationEvent{id: readGeneration, kind: paletteEventMarker})
					})
				})
				if !sendOK.Load() {
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
			if !sendOK.Load() || p.ctx.Err() != nil {
				return
			}
			input.ack(consumer)
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
			frame := ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: sz})}
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
	default:
		return &DetachedError{Reason: reason, Text: "detached by daemon"}
	}
}
