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

// runtimeObserverQueueDepth bounds marks awaiting the one reporting worker.
// A full queue records an ordered diagnostic gap rather than blocking ACKs.
const runtimeObserverQueueDepth = 64

var reconnectSleep = sleepReconnect
var reconnectSleepWithResize = sleepReconnectWithResizeEvents

const (
	statusReconnect = "\r\x1b[2Kreconnecting…"
	statusClear     = "\r\x1b[2K"
)

// Run connects via dialer and runs the attach client. It owns the terminal
// lifecycle above attach attempts so raw mode remains active while a live
// client process redials a lost link.
//
// remote and clipboard together gate the clipboard-image-transfer feature
// (docs/superpowers/specs/2026-07-04-clipboard-image-transfer-design.md):
// Ctrl+V is only intercepted on a remote attach (remote true) with a
// ClipboardReader configured (clipboard non-nil) — local attaches forward
// 0x16 untouched so the locally running agent's own clipboard handling
// applies.
// Option configures opt-in client runtime observation.
type Option func(*runtimeOptions)

type runtimeOptions struct{ observer ports.RuntimeObserver }

// WithRuntimeObserver enables process-local marks. It intentionally accepts no
// clock; timestamps are assigned by the concrete observer.
func WithRuntimeObserver(observer ports.RuntimeObserver) Option {
	return func(opts *runtimeOptions) { opts.observer = observer }
}

func Run(ctx context.Context, dialer ports.Dialer, term ports.Terminal, clk ports.Clock, intent uint8, name string, remote bool, clipboard ports.ClipboardReader, log *slog.Logger) error {
	return run(ctx, dialer, term, clk, intent, name, remote, clipboard, log, nil)
}

// RunWithRuntimeObserver is the application wiring entry point.
func RunWithRuntimeObserver(ctx context.Context, dialer ports.Dialer, term ports.Terminal, clk ports.Clock, intent uint8, name string, remote bool, clipboard ports.ClipboardReader, log *slog.Logger, observer ports.RuntimeObserver) error {
	return run(ctx, dialer, term, clk, intent, name, remote, clipboard, log, observer)
}

func run(ctx context.Context, dialer ports.Dialer, term ports.Terminal, clk ports.Clock, intent uint8, name string, remote bool, clipboard ports.ClipboardReader, log *slog.Logger, observer ports.RuntimeObserver) (retErr error) {
	if observer != nil {
		reporter := ports.NewSerializedRuntimeObserver(observer, runtimeObserverQueueDepth)
		defer reporter.Close()
		observer = reporter
	}
	if log == nil {
		log = slog.Default()
	}
	ms := milestones{}

	defer func() {
		if retErr != nil {
			log.Error("attach ended with error", "err", retErr, "missing_milestones", ms.missing())
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
		restore, err = term.EnterRaw()
		if err != nil {
			return fmt.Errorf("vev: entering raw mode: %w", err)
		}
		rawEntered = true
		ms.rawEntered = true
		return nil
	}

	resumeToken := uint64(0)
	attemptIntent := intent
	attemptName := name
	backoff := defaultReconnectBackoff.initial
	showingStatus := false
	var reconnectToastRect domain.Rect
	statusStage := reconnectStageOfflineRetrying
	redrawRemoteStatus := func(size domain.Size) {
		if !rawEntered || !remote {
			return
		}
		if showingStatus {
			_ = clearReconnectToast(term.Out(), reconnectToastRect)
		}
		drawnRect, _ := drawReconnectToastStage(term.Out(), size, statusStage)
		reconnectToastRect = drawnRect
		_ = term.Flush()
		showingStatus = true
	}
	var drawStatus func()
	drawStatusStage := func(stage reconnectStage) {
		statusStage = stage
		if showingStatus && remote {
			size, err := term.Size()
			if err == nil {
				redrawRemoteStatus(size)
			}
			return
		}
		drawStatus()
	}
	drawStatus = func() {
		if !rawEntered || showingStatus {
			return
		}
		if remote {
			size, err := term.Size()
			if err != nil {
				return
			}
			redrawRemoteStatus(size)
		} else {
			_, _ = term.Out().Write([]byte(statusReconnect))
			_ = term.Flush()
			showingStatus = true
		}
	}
	clearStatus := func() {
		if !showingStatus {
			return
		}
		if remote {
			_ = clearReconnectToast(term.Out(), reconnectToastRect)
		} else {
			_, _ = term.Out().Write([]byte(statusClear))
		}
		_ = term.Flush()
		showingStatus = false
	}
	sleepWhileReconnecting := func(d time.Duration) bool {
		if remote && showingStatus {
			return reconnectSleepWithResize(ctx, clk, d, term.ResizeEvents(), redrawRemoteStatus)
		}
		return reconnectSleep(ctx, clk, d)
	}

	for {
		transport, err := dialer.Dial(ctx)
		if err != nil {
			if resumeToken == 0 || ctx.Err() != nil {
				clearStatus()
				return err
			}
			log.Warn("reconnect dial failed", "err", err, "backoff", backoff)
			drawStatus()
			if !sleepWhileReconnecting(backoff) {
				clearStatus()
				return ctx.Err()
			}
			backoff = nextReconnectBackoff(backoff, defaultReconnectBackoff.max)
			attemptIntent = ports.IntentResume
			continue
		}
		ms.dialed = true

		var linkEvents <-chan ports.LinkEvent
		if reporter, ok := transport.(ports.LinkStateReporter); ok {
			linkEvents = reporter.LinkEvents()
		}
		result := attachOnce(ctx, transport, term, clk, attemptIntent, attemptName, resumeToken, processClientID, &ms, enterRaw, clearStatus, drawStatusStage, linkEvents, remote, clipboard, log, observer)
		if result.welcomed {
			backoff = defaultReconnectBackoff.initial
		}
		_ = transport.Close()
		if result.resumeToken != 0 {
			resumeToken = result.resumeToken
		}
		if result.sessionName != "" {
			attemptName = result.sessionName
		}
		if result.err == nil {
			clearStatus()
			return nil
		}
		if !shouldReconnect(result.err) || resumeToken == 0 || ctx.Err() != nil {
			clearStatus()
			return result.err
		}
		log.Warn("reconnecting after attach error", "err", result.err, "backoff", backoff)
		drawStatusStage(reconnectStageSSH)
		if !sleepWhileReconnecting(backoff) {
			clearStatus()
			return ctx.Err()
		}
		backoff = nextReconnectBackoff(backoff, defaultReconnectBackoff.max)
		attemptIntent = ports.IntentResume
	}
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

// Attach connects an already-dialed transport to the controlling terminal
// and runs the attach loop until the session detaches, the daemon
// disappears, or the context is cancelled.
//
// It is kept as a compatibility wrapper for callers that still own dialing.
func Attach(ctx context.Context, transport ports.Transport, term ports.Terminal, clk ports.Clock, intent uint8, name string, options ...Option) error {
	var opts runtimeOptions
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}
	return run(ctx, singleTransportDialer{transport: transport}, term, clk, intent, name, false, nil, slog.Default(), opts.observer)
}

type singleTransportDialer struct{ transport ports.Transport }

func (d singleTransportDialer) Dial(context.Context) (ports.Transport, error) {
	return d.transport, nil
}

type attachResult struct {
	resumeToken uint64
	sessionName string
	welcomed    bool
	err         error
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

func attachOnce(ctx context.Context, transport ports.Transport, term ports.Terminal, clk ports.Clock, intent uint8, name string, resumeToken uint64, clientID [16]byte, ms *milestones, enterRaw func() error, clearStatus func(), drawStatusStage func(reconnectStage), linkEvents <-chan ports.LinkEvent, remote bool, clipboard ports.ClipboardReader, log *slog.Logger, observers ...ports.RuntimeObserver) attachResult {
	var observer ports.RuntimeObserver
	if len(observers) != 0 {
		observer = observers[0]
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
	}
	if err := transport.Send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)}); err != nil {
		return attachResult{err: fmt.Errorf("vev: sending hello: %w", err)}
	}
	ms.helloSent = true

	// 2. Await Welcome or a typed rejection.
	reply, err := transport.Recv()
	if err != nil {
		return attachResult{err: fmt.Errorf("vev: awaiting welcome: %w", err)}
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
	clearStatus()

	// 4. Derive a cancellable context so the pumps always unwind when the
	// loop returns. cancel runs first (LIFO): it signals the pumps; the
	// deferred Close above then unblocks any pump parked in a syscall.
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sendCh := make(chan ports.Frame, sendQueueDepth)
	ackQueue := newCumulativeAckQueue()
	sendErrCh := make(chan error, 1)

	// colorQueryCh hands an OSC 10/11 re-query request from the stdin pump to
	// the main loop, which is the only goroutine allowed to touch term.Out()/
	// term.Flush(); QueryColors writes through the same batched writer, so
	// calling it directly from the stdin pump would race the main loop's
	// output writes.
	colorQueryCh := make(chan struct{}, 1)
	requestColors := func() {
		select {
		case colorQueryCh <- struct{}{}:
		default:
		}
	}

	var clip ports.ClipboardReader
	if remote {
		clip = clipboard
	}
	go runSender(loopCtx, cancel, transport, sendCh, ackQueue, sendErrCh, log)
	go runStdin(loopCtx, cancel, term.In(), sendCh, clk, trueColor, requestColors, clip, log)
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
				clearStatus()
				continue
			}
			if ev.State == ports.LinkStateDegraded {
				log.Warn("UDP link degraded")
				clearStatus()
				continue
			}
			drawStatusStage(stageForLinkState(ev.State))
			if ev.State == ports.LinkStateOffline {
				// Stop waiting for the transport to reach Dead at 60s: exit with
				// a retryable error so Run re-dials over ssh with the resume
				// token. Run closes this transport on the way out.
				return welcomedResult(errLinkOffline)
			}
		case <-colorQueryCh:
			if err := term.QueryColors(); err != nil {
				log.Warn("querying terminal colors", "err", err)
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

// runStdin pumps terminal input to the daemon as Input frames. A read error
// or EOF is treated as a detach: it best-effort sends a Detach frame and
// cancels the loop.
//
// Known MVP limitation: a bare io.Reader cannot be unblocked from outside,
// so on shutdown initiated elsewhere (daemon detach, transport loss) this
// goroutine stays parked in Read until the next byte arrives or the process
// exits. That is harmless here — Attach has already returned and restored
// the terminal — and matches the standard pattern for stdin pumps; a
// closable stdin duplicate could lift it later if ever needed.
func runStdin(ctx context.Context, cancel context.CancelFunc, in io.Reader, out chan<- ports.Frame, clk ports.Clock, trueColor bool, requestColors func(), clipboard ports.ClipboardReader, log *slog.Logger) {
	defer log.Debug("stdin pump exited")
	buf := make([]byte, stdinBufSize)
	var scanner theme.Scanner
	var inputSeq uint64
	current := ports.Theme{TrueColor: trueColor}
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
			sendTheme := func() {
				send(ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(current)})
			}
			scanner.Scan(buf[:n], func(kind int, rgb renderer.RGB) {
				switch kind {
				case 10:
					current.HasForeground = true
					current.Foreground = rgb
				case 11:
					current.HasBackground = true
					current.Background = rgb
				default:
					return
				}
				sendTheme()
			}, func(light bool) {
				current.SchemeKnown = true
				current.Light = light
				if requestColors != nil {
					requestColors()
				}
				sendTheme()
			}, sink)
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
