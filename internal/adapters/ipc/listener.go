package ipc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bnema/vev/internal/ports"
)

// socketFileName is the fixed name of the daemon's listening socket within
// its socket directory.
const socketFileName = "daemon.sock"

// ErrDaemonRunning is returned by Listen when the socket path is already
// bound by a live daemon (a dial-probe against it succeeded).
var ErrDaemonRunning = errors.New("ipc: a daemon is already listening on this socket")

// unixListener implements ports.Listener over an AF_UNIX SOCK_STREAM
// listener.
type unixListener struct {
	ln   *net.UnixListener
	addr string
}

// Listen creates the socket directory (0700) if needed and starts
// listening on dir/daemon.sock (chmod 0600 after bind).
//
// If the socket path is already bound (EADDRINUSE), Listen dial-probes it:
// a failed dial (connection refused, or the file having vanished) means
// the previous owner died without cleaning up, so the stale socket file is
// unlinked and bind is retried once. A successful dial means a live daemon
// owns the socket, and Listen returns ErrDaemonRunning.
func Listen(dir string) (ports.Listener, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("ipc: creating socket directory: %w", err)
	}

	sockPath := filepath.Join(dir, socketFileName)

	ln, err := bindUnix(sockPath)
	if err != nil {
		if !errors.Is(err, syscall.EADDRINUSE) {
			return nil, fmt.Errorf("ipc: listen on %s: %w", sockPath, err)
		}

		if probeErr := probeLiveDaemon(sockPath); probeErr != nil {
			return nil, probeErr
		}

		// Dial failed: the socket file is stale (its listener died
		// without unlinking). Remove it and retry the bind once.
		if rmErr := os.Remove(sockPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return nil, fmt.Errorf("ipc: removing stale socket %s: %w", sockPath, rmErr)
		}

		ln, err = bindUnix(sockPath)
		if err != nil {
			return nil, fmt.Errorf("ipc: listen on %s after removing stale socket: %w", sockPath, err)
		}
	}

	if err := os.Chmod(sockPath, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("ipc: chmod %s: %w", sockPath, err)
	}

	return &unixListener{ln: ln, addr: sockPath}, nil
}

// bindUnix binds and listens on sockPath.
func bindUnix(sockPath string) (*net.UnixListener, error) {
	addr, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		return nil, err
	}
	return net.ListenUnix("unix", addr)
}

// probeLiveDaemon dial-probes sockPath to distinguish a stale socket file
// from one actively served by a running daemon. It returns nil if the
// socket looks stale (safe to unlink and retry), or ErrDaemonRunning if a
// peer accepted the dial.
func probeLiveDaemon(sockPath string) error {
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOENT) {
			return nil // stale: nobody home
		}
		return fmt.Errorf("ipc: probing socket %s: %w", sockPath, err)
	}
	_ = conn.Close()
	return ErrDaemonRunning
}

// Accept waits for and returns the next connection, wrapped as a
// ports.Transport.
func (l *unixListener) Accept() (ports.Transport, error) {
	conn, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	return NewTransport(conn), nil
}

// Close stops accepting connections. The underlying net.UnixListener
// unlinks the socket file on close (default behavior for listeners it
// created).
func (l *unixListener) Close() error {
	return l.ln.Close()
}

// Addr returns the filesystem path of the listening socket.
func (l *unixListener) Addr() string {
	return l.addr
}

// SocketDir returns the directory vev's daemon should place its socket in:
// $XDG_RUNTIME_DIR/vev if set, else /run/user/<uid>/vev if that directory
// exists, else /tmp/vev-<uid>.
func SocketDir() string {
	uid := os.Getuid()

	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		return filepath.Join(xdg, "vev")
	}

	runUser := fmt.Sprintf("/run/user/%d", uid)
	if fi, err := os.Stat(runUser); err == nil && fi.IsDir() {
		return filepath.Join(runUser, "vev")
	}

	return fmt.Sprintf("/tmp/vev-%d", uid)
}
