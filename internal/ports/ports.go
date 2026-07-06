package ports

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/bnema/vev/internal/domain"
)

// ErrNoClipboardImage is returned by ClipboardReader.ReadImage when the
// clipboard holds no image (or an unsupported type). Callers fall back to
// forwarding whatever keystroke triggered the read.
var ErrNoClipboardImage = errors.New("ports: no image on clipboard")

// PTY is a running pseudo-terminal-backed child process.
type PTY interface {
	io.ReadWriteCloser // Read = child output, Write = child input
	Resize(sz domain.Size) error
	Pid() int
	ForegroundPgid() (int, error)
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
	QueryColors() error
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

// Dialer establishes outbound Transport connections.
type Dialer interface {
	Dial(ctx context.Context) (Transport, error)
}

// Listener accepts incoming Transport connections.
type Listener interface {
	Accept() (Transport, error)
	Close() error
	Addr() string
}

// Store is a small byte-key/value persistence port.
//
// Implementations may buffer writes; Sync is the durability barrier.
type Store interface {
	Get(key []byte) ([]byte, bool)
	Set(key, val []byte) error
	Delete(key []byte) error
	// Range iterates key/value pairs; fn returning false stops iteration early.
	Range(fn func(k, v []byte) bool)
	Sync() error
	Close() error
}

// SnapshotBlob is a durable named session snapshot payload.
type SnapshotBlob struct {
	Name string
	Data []byte
}

// SnapshotStore persists encoded session snapshots by name.
type SnapshotStore interface {
	Write(name string, data []byte) error
	Load() ([]SnapshotBlob, error)
	Delete(name string) error
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

// ProcessInspector reads process metadata for daemon snapshot/persistence use.
// Implementations may live in platform packages; usecases depend on this port.
type ProcessInspector interface {
	Cwd(pid int) (string, error)
	Comm(pid int) (string, error)
	Argv(pid int) ([]string, error)
	GroupArgv(pgid int, shellPid int) ([]string, error)
}

// ClipboardReader reads an image off the CLIENT-side system clipboard, for
// forwarding to a remote session's focused pane. Implementations live in
// internal/adapters (e.g. a wl-paste-backed one for Wayland); usecases only
// see this interface. See
// docs/superpowers/specs/2026-07-04-clipboard-image-transfer-design.md.
type ClipboardReader interface {
	// ReadImage returns the clipboard's image bytes and MIME type. It returns
	// ErrNoClipboardImage when the clipboard holds no image (or an
	// unsupported type).
	ReadImage(ctx context.Context) (mime string, data []byte, err error)
}
