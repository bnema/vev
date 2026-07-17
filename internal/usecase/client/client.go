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

// clearPalette invalidates entries from a previous terminal color query while
// retaining independently reported foreground, background, and scheme data.
func (s *terminalThemeState) clearPalette() ports.Theme {
	return s.update(func(theme *ports.Theme) {
		theme.PaletteKnown = 0
		theme.Palette = [16]renderer.RGB{}
	})
}

func (s *terminalThemeState) reportedTheme() (ports.Theme, bool) {
	theme := s.snapshot()
	return theme, theme.HasForeground || theme.HasBackground || theme.SchemeKnown || theme.PaletteKnown != 0
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
	runner      *Runner
	transport   ports.Transport
	request     AttachRequest
	resumeToken uint64
	clientID    [16]byte
	milestones  *milestones
	themeState  *terminalThemeState
	enterRaw    func() error
	reconnect   *reconnectUI
	linkEvents  <-chan ports.LinkEvent
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
	reportedTheme, restoreTheme := themeState.reportedTheme()

	// 4. Derive a cancellable context so the pumps always unwind when the
	// loop returns. cancel runs first (LIFO): it signals the pumps; the
	// deferred Close above then unblocks any pump parked in a syscall.
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sendCh := make(chan ports.Frame, sendQueueDepth)
	if restoreTheme {
		sendCh <- ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(reportedTheme)}
	}
	ackQueue := newCumulativeAckQueue()
	sendErrCh := make(chan error, 1)

	// colorQueryCh hands an OSC 10/11 re-query request from the stdin pump to
	// the main loop, which is the only goroutine allowed to touch term.Out()/
	// term.Flush(); QueryColors writes through the same batched writer, so
	// calling it directly from the stdin pump would race the main loop's
	// output writes. The acknowledgement creates a generation boundary: after
	// a scheme response, stdin must not read another chunk (which may contain
	// old OSC 4 replies) until this loop has cleared the palette and issued the
	// replacement query.
	colorQueryCh := make(chan colorQueryRequest)
	requestColors := func() bool {
		request := colorQueryRequest{acknowledged: make(chan struct{})}
		select {
		case colorQueryCh <- request:
		case <-loopCtx.Done():
			return false
		}
		select {
		case <-request.acknowledged:
			return true
		case <-loopCtx.Done():
			return false
		}
	}

	var clip ports.ClipboardReader
	if remote {
		clip = clipboard
	}
	go runSender(loopCtx, cancel, transport, sendCh, ackQueue, sendErrCh, log)
	go (&stdinPump{ctx: loopCtx, cancel: cancel, in: term.In(), out: sendCh, clock: clk, themeState: themeState, clipboard: clip, logger: log, requestColors: requestColors}).run()
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
		case request := <-colorQueryCh:
			// A fresh query supersedes every prior OSC 4 response. Queue the
			// cleared snapshot before writing the query so the daemon never
			// applies stale palette entries while it awaits the new responses.
			clearedTheme := themeState.clearPalette()
			select {
			case sendCh <- ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(clearedTheme)}:
			case <-loopCtx.Done():
				return welcomedResult(nil)
			}
			if err := term.QueryColors(); err != nil {
				log.Warn("querying terminal colors", "err", err)
			}
			close(request.acknowledged)
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

// stdinPump pumps terminal input to the daemon as Input frames. A read error
// or EOF is treated as a detach: it best-effort sends a Detach frame and
// cancels the loop.
//
// Known MVP limitation: a bare io.Reader cannot be unblocked from outside,
// so on shutdown initiated elsewhere (daemon detach, transport loss) this
// goroutine stays parked in Read until the next byte arrives or the process
// exits. That is harmless here — Run has already returned and restored the
// terminal — and matches the standard pattern for stdin pumps; a
// closable stdin duplicate could lift it later if ever needed.
type colorQueryRequest struct {
	acknowledged chan struct{}
}

type stdinPump struct {
	ctx           context.Context
	cancel        context.CancelFunc
	in            io.Reader
	out           chan<- ports.Frame
	clock         ports.Clock
	themeState    *terminalThemeState
	clipboard     ports.ClipboardReader
	logger        *slog.Logger
	requestColors func() bool
}

func (p *stdinPump) run() {
	ctx := p.ctx
	cancel := p.cancel
	in := p.in
	out := p.out
	clk := p.clock
	themeState := p.themeState
	requestColors := p.requestColors
	clipboard := p.clipboard
	log := p.logger
	defer log.Debug("stdin pump exited")
	buf := make([]byte, stdinBufSize)
	var scanner theme.Scanner
	var inputSeq uint64
	var sendOK atomic.Bool
	sendOK.Store(true)
	send := func(frame ports.Frame) {
		select {
		case out <- frame:
		case <-ctx.Done():
			sendOK.Store(false)
		}
	}
	// The coalescer reframes a bracketed paste split across reads into one
	// MsgInput frame, so a marker boundary can never leave a lone ESC on the
	// wire. Its emit runs one MsgInput per call, keeping the inputSeq contract.
	coalescer := newPasteCoalescer(clk, func(data []byte) {
		if len(data) == 0 {
			return
		}
		copyData := append([]byte(nil), data...)
		inputSeq++
		send(ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{InputSeq: inputSeq, Data: copyData})})
	})
	defer coalescer.Close()

	// On a remote attach with a ClipboardReader configured, splice in the
	// clipboard interceptor ahead of the coalescer so a bare Ctrl+V (0x16)
	// becomes a clipboard image push instead of reaching the coalescer (and
	// from there the remote pane) as an ordinary keystroke.
	sink := coalescer.Scan
	if clipboard != nil {
		ci := &clipboardIntercept{
			coalescer: coalescer,
			reader:    clipboard,
			log:       log,
			sendImage: func(mime string, data []byte) {
				inputSeq++
				send(ports.Frame{Type: ports.MsgImagePush, Payload: ports.MarshalImagePush(ports.ImagePush{InputSeq: inputSeq, Mime: mime, Data: data})})
			},
			next: coalescer.Scan,
		}
		sink = ci.Scan
	}

	for {
		n, rerr := in.Read(buf)
		if n > 0 {
			themeChanged := false
			queryColors := false
			scanner.Scan(buf[:n], func(kind int, rgb renderer.RGB) {
				themeState.update(func(current *ports.Theme) {
					switch kind {
					case 10:
						current.HasForeground = true
						current.Foreground = rgb
					case 11:
						current.HasBackground = true
						current.Background = rgb
					}
				})
				themeChanged = true
			}, func(slot int, rgb renderer.RGB) {
				themeState.update(func(current *ports.Theme) {
					current.PaletteKnown |= uint16(1) << slot
					current.Palette[slot] = rgb
				})
				themeChanged = true
			}, func(light bool) {
				themeState.update(func(current *ports.Theme) {
					current.SchemeKnown = true
					current.Light = light
				})
				themeChanged = true
				queryColors = true
			}, sink)
			if themeChanged {
				send(ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(themeState.snapshot())})
			}
			// Defer the re-query until this read's complete snapshot is queued;
			// otherwise the main loop could invalidate palette state midway
			// through a chunk containing multiple terminal responses.
			if queryColors && requestColors != nil && !requestColors() {
				return
			}
			if !sendOK.Load() {
				return
			}
		}
		if rerr != nil {
			// Best-effort detach notification, then unwind.
			select {
			case out <- ports.Frame{Type: ports.MsgDetach, Payload: ports.MarshalDetach(ports.Detach{})}:
			case <-ctx.Done():
			}
			cancel()
			return
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
