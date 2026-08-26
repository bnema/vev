package ports

import (
	"context"
	"io"

	"github.com/bnema/vev/internal/domain"
)

// PTY is a running pseudo-terminal-backed child process.
type PTY interface {
	io.ReadWriteCloser // Read = child output, Write = child input
	Resize(geometry domain.Geometry) error
	Pid() int
	ForegroundPgid() (int, error)
}

// PTYFactory creates PTYs by spawning a command attached to a new pseudo-terminal.
type PTYFactory interface {
	// Open creates a PTY while honoring ctx cancellation. Implementations must
	// return promptly once ctx is cancelled and must not leave a child running.
	// geometry is applied before the child starts, including optional pixel
	// dimensions when they are known.
	Open(ctx context.Context, cmd string, args []string, env []string, dir string, geometry domain.Geometry) (PTY, error)
}
