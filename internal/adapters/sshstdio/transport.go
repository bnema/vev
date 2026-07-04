// Package sshstdio adapts an ssh subprocess' stdin/stdout into vev's framed
// Transport interface. It intentionally builds argv slices for os/exec instead
// of shell command strings so remote targets and session names are never shell-
// interpolated locally.
package sshstdio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

// NewTransport wraps separate reader/writer streams as a framed Transport.
func NewTransport(r io.Reader, w io.Writer, close closeFunc) ports.Transport {
	if close == nil {
		close = func() error { return nil }
	}
	return &transport{r: r, w: w, close: close}
}

type transport struct {
	r     io.Reader
	w     io.Writer
	close closeFunc

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
		return ports.Frame{}, err
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
		return ports.Frame{}, err
	}

	payload := append([]byte(nil), t.readBuf[1:]...)
	return ports.Frame{Type: ports.MsgType(t.readBuf[0]), Payload: payload}, nil
}

func (t *transport) Close() error { return t.close() }

func newProcessCloser(cmd *exec.Cmd, stdin io.Closer, stderr *bytes.Buffer, timeout time.Duration) closeFunc {
	return func() error {
		_ = stdin.Close()

		waitErr := make(chan error, 1)
		go func() { waitErr <- cmd.Wait() }()

		var err error
		select {
		case err = <-waitErr:
		case <-time.After(timeout):
			_ = cmd.Process.Kill()
			err = <-waitErr
		}
		if err == nil {
			return nil
		}

		stderrText := strings.TrimSpace(stderr.String())
		if stderrText != "" {
			return fmt.Errorf("sshstdio: ssh exited: %w: %s", err, stderrText)
		}
		return fmt.Errorf("sshstdio: ssh exited: %w", err)
	}
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
	spec := BuildCommand(target, session)
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
		return nil, fmt.Errorf("sshstdio: start ssh: %w", err)
	}

	return NewTransport(stdout, stdin, newProcessCloser(cmd, stdin, &stderr, sshCloseTimeout)), nil
}
