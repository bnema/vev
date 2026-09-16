package brokeripc

import (
	"path/filepath"

	"github.com/bnema/vev/internal/adapters/ipc"
)

// SocketFileName is the fixed name of the per-user broker IPC socket inside its
// runtime directory. It is deliberately distinct from the daemon's
// daemon.sock, so the broker endpoint and a session daemon endpoint can never
// be confused for one another.
const SocketFileName = "broker.sock"

// SocketPath returns the broker IPC socket path inside runtimeDir.
func SocketPath(runtimeDir string) string {
	return filepath.Join(runtimeDir, SocketFileName)
}

// SocketDir returns the per-user directory the broker endpoint belongs in:
// $XDG_RUNTIME_DIR/vev when set, else /run/user/<uid>/vev when that directory
// exists, else /tmp/vev-<uid>. It is the daemon's per-user runtime directory as
// well, so both endpoints share one owner-only directory while keeping
// distinct socket names.
func SocketDir() string { return ipc.SocketDir() }

// DefaultSocketPath returns the broker endpoint path in the per-user runtime
// directory. Callers that need a private temporary endpoint supply their own
// parent directory to SocketPath instead.
func DefaultSocketPath() string { return SocketPath(SocketDir()) }
