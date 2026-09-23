package ipc

import "path/filepath"

// SocketPath returns the daemon socket path inside runtimeDir. It has no
// current caller: brokeripc.SocketPath (a distinct, unrelated function) is
// the one every production and test call site actually uses for the daemon's
// own socket path. This is kept as a test-only convenience the package name
// implies is available.
func SocketPath(runtimeDir string) string {
	return filepath.Join(runtimeDir, socketFileName)
}
