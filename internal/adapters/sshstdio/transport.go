// Package sshstdio adapts an ssh subprocess' stdin/stdout into vev's framed
// Transport interface. It intentionally builds argv slices for os/exec instead
// of shell command strings so remote targets and session names are never shell-
// interpolated locally.
package sshstdio

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev/internal/adapters/streamframe"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

const sshCloseTimeout = 3 * time.Second

var (
	ErrZeroLengthFrame = streamframe.ErrZeroLength
	ErrFrameTooLarge   = streamframe.ErrTooLarge
)

type closeFunc func() error
type eofErrFunc func() error
type operationKind uint8

const (
	operationSend operationKind = iota
	operationReceive
	operationKindCount
)

type operationWait struct {
	send    <-chan struct{}
	receive <-chan struct{}
}

type Option func(*transport)

// WithRuntimeObserver enables process-local transport marks; it deliberately
// takes only an observer so a carriage adapter never owns trace time.
func WithRuntimeObserver(observer ports.SerializedRuntimeObserver) Option {
	return func(t *transport) { t.observer = observer }
}

// NewTransport wraps separate reader/writer streams as a framed Transport.
func NewTransport(r io.Reader, w io.Writer, closeFn closeFunc, opts ...Option) wire.Transport {
	t := newTransport(r, w, closeFn, nil)
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
}

func newTransport(r io.Reader, w io.Writer, closeFn closeFunc, eofErr eofErrFunc) *transport {
	if closeFn == nil {
		closeFn = func() error { return nil }
	}
	t := &transport{
		r:      r,
		w:      w,
		close:  closeFn,
		eofErr: eofErr,
		done:   make(chan struct{}),
		framer: nil,
	}
	if file, ok := r.(*os.File); ok && file == os.Stdin {
		t.r = newProcessStdinReader(file, t.done)
	} else if closer, ok := r.(io.Closer); ok {
		t.readerCloser = closer
	} else {
		t.r = newUnownedReader(r, t.done)
	}
	if file, ok := w.(*os.File); !ok || file != os.Stdout {
		if closer, ok := w.(io.Closer); ok {
			t.writerCloser = closer
		}
	}
	// Framing, queueing, and the writer live in streamframe; this adapter
	// keeps reader cancellation, process waiting, and EOF mapping. The
	// framer's closer releases owned streams so Close unblocks Recv.
	t.framer = streamframe.NewFramer(t.r, &serializedWriter{w: t.w}, t.closeOwnedStreams)
	return t
}

var _ wire.BoundedTransport = (*transport)(nil)

// serializedWriter serializes concurrent streamframe writes over one child
// stdin pipe; streamframe already serializes queue admission, this guards
// the raw Write against interleaving.
type serializedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *serializedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

type transport struct {
	r              io.Reader
	w              io.Writer
	framer         *streamframe.Framer
	close          closeFunc
	eofErr         eofErrFunc
	readerCloser   io.Closer
	writerCloser   io.Closer
	done           chan struct{}
	observer       ports.SerializedRuntimeObserver
	operationMu    sync.Mutex
	operationCount [operationKindCount]int
	closing        bool
	operationsDone [operationKindCount]chan struct{}
	closeOnce      sync.Once
	closeErr       error
}

type readResult struct {
	data []byte
	err  error
}

// unownedReader is the generic cancellation boundary for readers that cannot
// be closed by the transport. The worker may remain blocked until the source
// itself is released; process stdin uses the singleton pump below instead.
// ponytail: arbitrary io.Reader has no cancellation contract, so one fallback
// worker may remain until that source returns; add a cancellable reader port if
// another production unowned source appears.
type unownedReader struct {
	r    io.Reader
	done <-chan struct{}
}

func newUnownedReader(r io.Reader, done <-chan struct{}) *unownedReader {
	return &unownedReader{r: r, done: done}
}

func (r *unownedReader) Read(dst []byte) (int, error) {
	select {
	case <-r.done:
		return 0, io.ErrClosedPipe
	default:
	}
	result := make(chan readResult, 1)
	go func() {
		buf := make([]byte, len(dst))
		n, err := r.r.Read(buf)
		result <- readResult{data: buf[:n], err: err}
	}()
	select {
	case <-r.done:
		return 0, io.ErrClosedPipe
	case result := <-result:
		select {
		case <-r.done:
			return 0, io.ErrClosedPipe
		default:
		}
		copy(dst, result.data)
		return len(result.data), result.err
	}
}

type processStdinPump struct {
	chunks chan readResult
	mu     sync.Mutex
	buf    []byte
	err    error
}

var processStdinPumps struct {
	sync.Mutex
	file *os.File
	pump *processStdinPump
}

// processStdinPumpFor keeps one read worker for the process stdin lifetime.
// Reconnects therefore do not strand one blocked worker per transport.
func processStdinPumpFor(file *os.File) *processStdinPump {
	processStdinPumps.Lock()
	defer processStdinPumps.Unlock()
	if processStdinPumps.pump != nil && processStdinPumps.file == file {
		return processStdinPumps.pump
	}
	pump := &processStdinPump{chunks: make(chan readResult, 1)}
	processStdinPumps.file = file
	processStdinPumps.pump = pump
	go pump.run(file)
	return pump
}

func (p *processStdinPump) run(file *os.File) {
	for {
		buf := make([]byte, 32*1024)
		n, err := file.Read(buf)
		if n > 0 {
			p.chunks <- readResult{data: append([]byte(nil), buf[:n]...)}
		}
		if err != nil {
			p.chunks <- readResult{err: err}
			close(p.chunks)
			return
		}
	}
}

func (p *processStdinPump) read(done <-chan struct{}, dst []byte) (int, error) {
	select {
	case <-done:
		return 0, io.ErrClosedPipe
	default:
	}
	p.mu.Lock()
	if len(p.buf) != 0 {
		n := copy(dst, p.buf)
		p.buf = p.buf[n:]
		p.mu.Unlock()
		return n, nil
	}
	if p.err != nil {
		err := p.err
		p.mu.Unlock()
		return 0, err
	}
	p.mu.Unlock()

	select {
	case <-done:
		return 0, io.ErrClosedPipe
	case result, ok := <-p.chunks:
		if !ok {
			return 0, io.EOF
		}
		if len(result.data) != 0 {
			select {
			case <-done:
				return 0, io.ErrClosedPipe
			default:
			}
			n := copy(dst, result.data)
			p.mu.Lock()
			p.buf = append(p.buf[:0], result.data[n:]...)
			p.mu.Unlock()
			return n, nil
		}
		p.mu.Lock()
		p.err = result.err
		p.mu.Unlock()
		return 0, result.err
	}
}

type processStdinReader struct {
	pump *processStdinPump
	done <-chan struct{}
}

func newProcessStdinReader(file *os.File, done <-chan struct{}) io.Reader {
	return processStdinReader{pump: processStdinPumpFor(file), done: done}
}

func (r processStdinReader) Read(dst []byte) (int, error) {
	return r.pump.read(r.done, dst)
}

func (t *transport) Send(envelope wire.Envelope) error {
	end := t.beginOperation(ports.RuntimeAdapterSendStart, uint64(len(envelope.Payload)))
	err := t.framer.Send(envelope.Payload)
	end(err == nil)
	return err
}

func (t *transport) Recv() (wire.Envelope, error) {
	end := t.beginOperation(ports.RuntimeAdapterReceiveStart, 0)
	payload, err := t.framer.Recv()
	if err != nil {
		err = t.mapEOFError(err)
		end(false)
		return wire.Envelope{}, err
	}
	end(true)
	return wire.Envelope{Payload: payload}, nil
}

// RecvBounded reads one complete envelope bounded to limit before allocation,
// sharing the framer's validation with Recv: an over-limit length prefix is
// refused without reading its body. It is the bounded raw-carriage capability
// a preamble-negotiated multiplexer consumes.
func (t *transport) RecvBounded(limit uint64) (wire.Envelope, error) {
	end := t.beginOperation(ports.RuntimeAdapterReceiveStart, 0)
	payload, err := t.framer.RecvBounded(limit)
	if err != nil {
		err = t.mapEOFError(err)
		end(false)
		return wire.Envelope{}, err
	}
	end(true)
	return wire.Envelope{Payload: payload}, nil
}

func (t *transport) beginOperation(start ports.RuntimeMarkKind, bytes uint64) func(bool) {
	if t.observer == nil {
		return func(bool) {}
	}
	kind := operationSend
	if start == ports.RuntimeAdapterReceiveStart {
		kind = operationReceive
	}
	if !t.beginObservedOperation(kind) {
		return func(bool) {}
	}
	correlation := ports.NewRuntimeCorrelation()
	t.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("sshstdio", correlation, start, bytes, true))
	end := ports.RuntimeAdapterSendEnd
	if start == ports.RuntimeAdapterReceiveStart {
		end = ports.RuntimeAdapterReceiveEnd
	}
	return func(valid bool) {
		defer t.finishObservedOperation(kind)
		t.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("sshstdio", correlation, end, bytes, valid))
	}
}

func (t *transport) mapEOFError(err error) error {
	if t.eofErr == nil || (!errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF)) {
		return err
	}
	if sshErr := t.eofErr(); sshErr != nil {
		return sshErr
	}
	return err
}

func (t *transport) Close() error {
	t.closeOnce.Do(func() {
		wait := t.beginShutdown()
		// The framer's closer (closeOwnedStreams below) releases owned
		// streams before waiting for in-flight operations. Unowned
		// streams cannot be interrupted safely, so the cancellation
		// boundary releases Recv without closing the source.
		t.closeErr = t.framer.Close()
		if t.writerCloser != nil && wait.send != nil {
			<-wait.send
		}
		if t.readerCloser != nil && wait.receive != nil {
			<-wait.receive
		}
	})
	return t.closeErr
}

// closeOwnedStreams closes owned streams before the framer waits for its
// writer, then waits for the child process. It runs exactly once via the
// framer's Close.
func (t *transport) closeOwnedStreams() error {
	var writeErr, readErr error
	if t.writerCloser != nil {
		writeErr = t.writerCloser.Close()
	}
	if t.readerCloser != nil {
		readErr = t.readerCloser.Close()
	}
	closeErr := t.close()
	switch {
	case closeErr != nil:
		return closeErr
	case readErr != nil:
		return readErr
	default:
		return writeErr
	}
}

func (t *transport) beginObservedOperation(kind operationKind) bool {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	if t.closing {
		return false
	}
	if t.operationCount[kind] == 0 {
		t.operationsDone[kind] = make(chan struct{})
	}
	t.operationCount[kind]++
	return true
}

func (t *transport) finishObservedOperation(kind operationKind) {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	t.operationCount[kind]--
	if t.operationCount[kind] == 0 {
		close(t.operationsDone[kind])
	}
}

func (t *transport) beginShutdown() operationWait {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	t.closing = true
	close(t.done)
	var wait operationWait
	if t.operationCount[operationSend] != 0 {
		wait.send = t.operationsDone[operationSend]
	}
	if t.operationCount[operationReceive] != 0 {
		wait.receive = t.operationsDone[operationReceive]
	}
	return wait
}

// stderrSink is the bounded diagnostic capture a process waiter reads for a
// non-clean exit. A plain *bytes.Buffer keeps the session dial's existing raw
// behavior; the mux dial supplies a bounded, sanitizing capture and sets
// sanitize so the public error never carries remote stderr.
type stderrSink interface {
	io.Writer
	String() string
}

type processWaiter struct {
	cmd      *exec.Cmd
	stdin    io.Closer
	stderr   stderrSink
	timeout  time.Duration
	log      *slog.Logger
	target   string
	session  string
	sanitize bool

	waitOnce sync.Once
	waitErr  error
}

func newProcessWaiter(cmd *exec.Cmd, stdin io.Closer, stderr stderrSink, timeout time.Duration, log *slog.Logger, target, session string) *processWaiter {
	w := &processWaiter{
		cmd:     cmd,
		stdin:   stdin,
		stderr:  stderr,
		timeout: timeout,
		log:     log,
		target:  target,
		session: session,
	}
	return w
}

// newMuxProcessWaiter builds the waiter for one mux child. It reuses the
// session wait/kill contract unchanged but sanitizes the public error: the
// bounded, sanitized stderr is logged, never returned.
func newMuxProcessWaiter(cmd *exec.Cmd, stdin io.Closer, stderr stderrSink, timeout time.Duration, log *slog.Logger) *processWaiter {
	w := newProcessWaiter(cmd, stdin, stderr, timeout, log, "", "")
	w.sanitize = true
	return w
}

func (w *processWaiter) close() error {
	_ = w.stdin.Close()
	return w.wait(w.timeout)
}

func (w *processWaiter) eofErr() error {
	return w.wait(sshCloseTimeout)
}

func (w *processWaiter) wait(timeout time.Duration) error {
	w.waitOnce.Do(func() {
		// cmd.Wait must start only after stdout is no longer being read. os/exec
		// requires callers to finish reading StdoutPipe before Wait, so DialContext
		// calls this from transport Close or after Recv has already observed EOF.
		waitCh := make(chan error, 1)
		go func() { waitCh <- w.cmd.Wait() }()
		select {
		case w.waitErr = <-waitCh:
		case <-time.After(timeout):
			_ = w.cmd.Process.Kill()
			w.waitErr = <-waitCh
		}
		w.waitErr = formatProcessWaitError(w.waitErr, w.stderr, w.log, w.target, w.session, w.sanitize)
	})
	return w.waitErr
}

func formatProcessWaitError(err error, stderr stderrSink, log *slog.Logger, target, session string, sanitize bool) error {
	if err == nil {
		return nil
	}
	var stderrText string
	if stderr != nil {
		stderrText = strings.TrimSpace(stderr.String())
	}
	if log != nil {
		attrs := []any{"target", target, "session", session, "err", err}
		if stderrText != "" {
			attrs = append(attrs, "stderr", stderrText)
		}
		log.Warn("ssh exited non-cleanly", attrs...)
	}
	if sanitize {
		// The mux carriage's public error carries only the typed outcome; the
		// bounded, sanitized diagnostic above is local-only.
		return fmt.Errorf("%w: %w", ErrMuxSSHExit, err)
	}
	if stderrText != "" {
		return fmt.Errorf("sshstdio: ssh exited: %w: %s", err, stderrText)
	}
	return fmt.Errorf("sshstdio: ssh exited: %w", err)
}

func newProcessCloser(cmd *exec.Cmd, stdin io.Closer, stderr stderrSink, timeout time.Duration, log *slog.Logger, target, session string) closeFunc {
	return newProcessWaiter(cmd, stdin, stderr, timeout, log, target, session).close
}

// CommandSpec is the exact ssh argv vev will execute locally.
type CommandSpec struct {
	Path string
	Args []string
}

// BuildCommandForRemoteCommand constructs ssh argv for an arbitrary remote
// command. Every remote word is POSIX single-quoted; the target remains one
// local argv word after the option terminator.
func BuildCommandForRemoteCommand(target string, command ...string) CommandSpec {
	remote := make([]string, 0, len(command))
	for _, word := range command {
		remote = append(remote, shellQuote(word))
	}
	args := []string{"--", target, strings.Join(remote, " ")}
	return CommandSpec{Path: "ssh", Args: args}
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
