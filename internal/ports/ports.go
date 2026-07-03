package ports

import (
	"io"
	"time"

	"github.com/bnema/vev/internal/domain"
)

// PTY is a running pseudo-terminal-backed child process.
type PTY interface {
	io.ReadWriteCloser // Read = child output, Write = child input
	Resize(sz domain.Size) error
	Pid() int
}

// PTYFactory creates PTYs by spawning a command attached to a new pseudo-terminal.
type PTYFactory interface {
	Open(cmd string, args []string, env []string, dir string, sz domain.Size) (PTY, error)
}

// Terminal is the CLIENT-side controlling terminal.
type Terminal interface {
	EnterRaw() (restore func() error, err error)
	Size() (domain.Size, error)
	ResizeEvents() <-chan domain.Size
	In() io.Reader
	Out() io.Writer
	Flush() error
}

// Transport is a framed message channel over a single connection.
type Transport interface {
	Send(Frame) error
	Recv() (Frame, error) // blocking; io.EOF on close
	Close() error
}

// Listener accepts incoming Transport connections.
type Listener interface {
	Accept() (Transport, error)
	Close() error
	Addr() string
}

// Clock abstracts time so usecases can be tested without real delays.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer abstracts a cancellable, resettable time.Timer.
type Timer interface {
	C() <-chan time.Time
	Reset(d time.Duration) bool
	Stop() bool
}
