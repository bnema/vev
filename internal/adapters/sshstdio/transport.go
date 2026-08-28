// Package sshstdio adapts an ssh subprocess' stdin/stdout into vev's framed
// Transport interface. It intentionally builds argv slices for os/exec instead
// of shell command strings so remote targets and session names are never shell-
// interpolated locally.
package sshstdio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

const (
	maxFrameLen     = wire.MaxFrameLen
	frameHeaderLen  = 4
	sshCloseTimeout = 3 * time.Second
)

var (
	ErrZeroLengthFrame = errors.New("sshstdio: zero-length frame")
	ErrFrameTooLarge   = errors.New("sshstdio: frame exceeds maximum length")
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
func NewTransport(r io.Reader, w io.Writer, closeFn closeFunc, opts ...Option) ports.Transport {
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
	return t
}

type transport struct {
	r              io.Reader
	w              io.Writer
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

	mu      sync.Mutex
	readBuf []byte
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

func (t *transport) Send(f wire.Frame) error {
	end := t.beginOperation(ports.RuntimeAdapterSendStart, uint64(len(f.Payload)))
	n := 1 + len(f.Payload)
	if n > maxFrameLen {
		end(false)
		return ErrFrameTooLarge
	}

	buf := make([]byte, frameHeaderLen+n)
	binary.BigEndian.PutUint32(buf[:frameHeaderLen], uint32(n))
	buf[frameHeaderLen] = byte(f.Type)
	copy(buf[frameHeaderLen+1:], f.Payload)

	t.mu.Lock()
	_, err := t.w.Write(buf)
	t.mu.Unlock()
	end(err == nil)
	return err
}

func (t *transport) Recv() (wire.Frame, error) {
	end := t.beginOperation(ports.RuntimeAdapterReceiveStart, 0)
	var hdr [frameHeaderLen]byte
	if _, err := io.ReadFull(t.r, hdr[:]); err != nil {
		err = t.mapEOFError(err)
		end(false)
		return wire.Frame{}, err
	}

	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		end(false)
		return wire.Frame{}, ErrZeroLengthFrame
	}
	if n > maxFrameLen {
		end(false)
		return wire.Frame{}, ErrFrameTooLarge
	}

	if cap(t.readBuf) < int(n) {
		t.readBuf = make([]byte, n)
	} else {
		t.readBuf = t.readBuf[:n]
	}
	if _, err := io.ReadFull(t.r, t.readBuf); err != nil {
		err = t.mapEOFError(err)
		end(false)
		return wire.Frame{}, err
	}

	payload := append([]byte(nil), t.readBuf[1:]...)
	end(true)
	return wire.Frame{Type: wire.MsgType(t.readBuf[0]), Payload: payload}, nil
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
		// Close owned streams before waiting for in-flight operations. The
		// writer is what releases a Send blocked on a child stdin pipe; the
		// reader releases a Recv blocked in io.ReadFull. Unowned streams cannot
		// be interrupted safely, so the cancellation boundary releases Recv
		// without closing the source.
		var writeErr, readErr error
		if t.writerCloser != nil {
			writeErr = t.writerCloser.Close()
		}
		if t.readerCloser != nil {
			readErr = t.readerCloser.Close()
		}
		closeErr := t.close()
		if t.writerCloser != nil && wait.send != nil {
			<-wait.send
		}
		if t.readerCloser != nil && wait.receive != nil {
			<-wait.receive
		}
		switch {
		case closeErr != nil:
			t.closeErr = closeErr
		case readErr != nil:
			t.closeErr = readErr
		default:
			t.closeErr = writeErr
		}
	})
	return t.closeErr
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

type processWaiter struct {
	cmd     *exec.Cmd
	stdin   io.Closer
	stderr  *bytes.Buffer
	timeout time.Duration
	log     *slog.Logger
	target  string
	session string

	waitOnce sync.Once
	waitErr  error
}

func newProcessWaiter(cmd *exec.Cmd, stdin io.Closer, stderr *bytes.Buffer, timeout time.Duration, log *slog.Logger, target, session string) *processWaiter {
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
		w.waitErr = formatProcessWaitError(w.waitErr, w.stderr, w.log, w.target, w.session)
	})
	return w.waitErr
}

func formatProcessWaitError(err error, stderr *bytes.Buffer, log *slog.Logger, target, session string) error {
	if err == nil {
		return nil
	}
	stderrText := strings.TrimSpace(stderr.String())
	if log != nil {
		attrs := []any{"target", target, "session", session, "err", err}
		if stderrText != "" {
			attrs = append(attrs, "stderr", stderrText)
		}
		log.Warn("ssh exited non-cleanly", attrs...)
	}
	if stderrText != "" {
		return fmt.Errorf("sshstdio: ssh exited: %w: %s", err, stderrText)
	}
	return fmt.Errorf("sshstdio: ssh exited: %w", err)
}

func newProcessCloser(cmd *exec.Cmd, stdin io.Closer, stderr *bytes.Buffer, timeout time.Duration, log *slog.Logger, target, session string) closeFunc {
	return newProcessWaiter(cmd, stdin, stderr, timeout, log, target, session).close
}

// CommandSpec is the exact ssh argv vev will execute locally.
type CommandSpec struct {
	Path string
	Args []string
}

// BuildCommand constructs the local ssh subprocess argv without invoking a
// local shell. OpenSSH sends the remote command as one string for the remote
// user's shell to interpret, so every remote argv word is POSIX single-quoted.
func BuildCommand(target, _ string) CommandSpec {
	return BuildCommandForMode(target, "_stdio", "")
}

// BuildCommandForMode constructs the local ssh subprocess argv for a hidden vev
// remote mode such as _stdio or _udp-bootstrap.
func BuildCommandForMode(target, mode, _ string) CommandSpec {
	return BuildCommandForRemoteCommand(target, "vev", mode)
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

// Dial starts ssh target vev _stdio and returns a Transport over the child
// process' stdio. Session selection is carried in Hello. The subprocess is
// started with exec.Command argv, never through a shell.
func Dial(target, session string) (ports.Transport, error) {
	return DialContext(context.Background(), target, session)
}

// DialContextWithRuntimeObserver is the option-bearing production constructor.
func DialContextWithRuntimeObserver(ctx context.Context, target, session string, logger *slog.Logger, opts ...Option) (ports.Transport, error) {
	return dialContext(ctx, target, session, logger, opts...)
}

// DialContext is like Dial, but the context gates ssh startup. Once the
// transport is returned, its Close method owns the subprocess lifetime; the
// handshake context must not kill an already-established carriage. Callers may
// pass a logger to record ssh start failures and non-clean exits without logging
// the generated command line.
func DialContext(ctx context.Context, target, session string, logger ...*slog.Logger) (ports.Transport, error) {
	var log *slog.Logger
	if len(logger) > 0 {
		log = logger[0]
	}
	return dialContext(ctx, target, session, log)
}

func dialContext(ctx context.Context, target, session string, log *slog.Logger, opts ...Option) (ports.Transport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	spec := BuildCommand(target, session)
	// The caller's context is also the bounded protocol-handshake context and is
	// canceled after the first committed publication. Binding it to the command
	// would kill a healthy long-lived carriage at that boundary. Transport.Close
	// owns cancellation after Start; the preflight check above keeps canceled
	// attempts from starting a process.
	cmd := exec.Command(spec.Path, spec.Args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("sshstdio: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("sshstdio: stdout pipe: %w", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		if log != nil {
			log.Error("ssh start failed", "target", target, "session", session, "err", err)
		}
		return nil, fmt.Errorf("sshstdio: start ssh: %w", err)
	}

	waiter := newProcessWaiter(cmd, stdin, &stderr, sshCloseTimeout, log, target, session)
	transport := newTransport(stdout, stdin, waiter.close, waiter.eofErr)
	if err := ctx.Err(); err != nil {
		_ = transport.Close()
		return nil, err
	}
	for _, opt := range opts {
		if opt != nil {
			opt(transport)
		}
	}
	return transport, nil
}
