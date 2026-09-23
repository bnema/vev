package brokeripc

import (
	"path/filepath"
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
