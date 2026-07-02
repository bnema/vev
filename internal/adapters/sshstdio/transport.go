// Package sshstdio adapts an ssh subprocess' stdin/stdout into vev's framed
// Transport interface. It intentionally builds argv slices for os/exec instead
// of shell command strings so remote targets and session names are never shell-
// interpolated locally.
package sshstdio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

const (
	maxFrameLen    = 16 << 20
	frameHeaderLen = 4
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

// CommandSpec is the exact ssh argv vev will execute locally.
type CommandSpec struct {
	Path string
	Args []string
}

// BuildCommand constructs the ssh subprocess argv without invoking a shell.
func BuildCommand(target, session string) CommandSpec {
	args := []string{target, "vev", "_stdio"}
	if session != "" {
		args = append(args, session)
	}
	return CommandSpec{Path: "ssh", Args: args}
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
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("sshstdio: start ssh: %w", err)
	}

	return NewTransport(stdout, stdin, func() error {
		_ = stdin.Close()
		err := cmd.Wait()
		if err != nil {
			return fmt.Errorf("sshstdio: ssh exited: %w", err)
		}
		return nil
	}), nil
}
