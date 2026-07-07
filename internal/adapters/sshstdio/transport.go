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
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const (
	maxFrameLen     = 16 << 20
	frameHeaderLen  = 4
	sshCloseTimeout = 3 * time.Second
)

var (
	ErrZeroLengthFrame = errors.New("sshstdio: zero-length frame")
	ErrFrameTooLarge   = errors.New("sshstdio: frame exceeds maximum length")
)

type closeFunc func() error
type eofErrFunc func() error

// NewTransport wraps separate reader/writer streams as a framed Transport.
func NewTransport(r io.Reader, w io.Writer, closeFn closeFunc) ports.Transport {
	return newTransport(r, w, closeFn, nil)
}

func newTransport(r io.Reader, w io.Writer, closeFn closeFunc, eofErr eofErrFunc) ports.Transport {
	if closeFn == nil {
		closeFn = func() error { return nil }
	}
	return &transport{r: r, w: w, close: closeFn, eofErr: eofErr}
}

type transport struct {
	r      io.Reader
	w      io.Writer
	close  closeFunc
	eofErr eofErrFunc

	mu      sync.Mutex
	readBuf []byte
}

func (t *transport) Send(f ports.Frame) error {
	n := 1 + len(f.Payload)
	if n > maxFrameLen {
		return ErrFrameTooLarge
	}

	buf := make([]byte, frameHeaderLen+n)
	binary.BigEndian.PutUint32(buf[:frameHeaderLen], uint32(n))
	buf[frameHeaderLen] = byte(f.Type)
	copy(buf[frameHeaderLen+1:], f.Payload)

	t.mu.Lock()
	defer t.mu.Unlock()
	_, err := t.w.Write(buf)
	return err
}

func (t *transport) Recv() (ports.Frame, error) {
	var hdr [frameHeaderLen]byte
	if _, err := io.ReadFull(t.r, hdr[:]); err != nil {
		return ports.Frame{}, t.mapEOFError(err)
	}

	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return ports.Frame{}, ErrZeroLengthFrame
	}
	if n > maxFrameLen {
		return ports.Frame{}, ErrFrameTooLarge
	}

	if cap(t.readBuf) < int(n) {
		t.readBuf = make([]byte, n)
	} else {
		t.readBuf = t.readBuf[:n]
	}
	if _, err := io.ReadFull(t.r, t.readBuf); err != nil {
		return ports.Frame{}, t.mapEOFError(err)
	}

	payload := append([]byte(nil), t.readBuf[1:]...)
	return ports.Frame{Type: ports.MsgType(t.readBuf[0]), Payload: payload}, nil
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

func (t *transport) Close() error { return t.close() }

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
func BuildCommand(target, session string) CommandSpec {
	return BuildCommandForMode(target, "_stdio", session)
}

// BuildCommandForMode constructs the local ssh subprocess argv for a hidden vev
// remote mode such as _stdio or _udp-bootstrap.
func BuildCommandForMode(target, mode, session string) CommandSpec {
	remote := []string{shellQuote("vev"), shellQuote(mode)}
	if session != "" {
		remote = append(remote, shellQuote(session))
	}
	args := []string{"--", target, strings.Join(remote, " ")}
	return CommandSpec{Path: "ssh", Args: args}
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// Dial starts ssh target vev _stdio [session] and returns a Transport over the
// child process' stdio. The subprocess is started with exec.Command argv, never
// through a shell.
func Dial(target, session string) (ports.Transport, error) {
	return DialContext(context.Background(), target, session)
}

// DialContext is like Dial, but the context is propagated to ssh startup so a
// canceled attach attempt interrupts the local ssh process. Callers may pass a
// logger to record ssh start failures and non-clean exits without logging the
// generated command line.
func DialContext(ctx context.Context, target, session string, logger ...*slog.Logger) (ports.Transport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var log *slog.Logger
	if len(logger) > 0 {
		log = logger[0]
	}
	spec := BuildCommand(target, session)
	cmd := exec.CommandContext(ctx, spec.Path, spec.Args...)
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
	return newTransport(stdout, stdin, waiter.close, waiter.eofErr), nil
}
