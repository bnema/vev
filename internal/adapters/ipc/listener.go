package ipc

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/pkg/safedir"
)

// socketFileName is the fixed name of the daemon's listening socket within
// its socket directory.
const socketFileName = "daemon.sock"

// SocketPath returns the daemon socket path inside runtimeDir.
func SocketPath(runtimeDir string) string {
	return filepath.Join(runtimeDir, socketFileName)
}

// ErrDaemonRunning is returned by Listen when the socket path is already
// bound by a live daemon (a dial-probe against it succeeded).
var ErrDaemonRunning = errors.New("ipc: a daemon is already listening on this socket")

// staleRecoveryAttempts bounds the bind/probe/remove/rebind loop that recovers
// from a socket file whose owner died without unlinking it. A racing owner can
// rebind between our liveness probe and our removal, in which case we leave its
// socket alone and retry; the bound keeps a pathological race from spinning.
const staleRecoveryAttempts = 3

// unixListener implements wire.Listener over an AF_UNIX SOCK_STREAM
// listener.
type unixListener struct {
	ln   *net.UnixListener
	addr string
	opts []Option
}

// Listen creates the socket directory (0700) if needed and starts
// listening on dir/daemon.sock (chmod 0600 after bind).
//
// If the socket path is already bound (EADDRINUSE), Listen dial-probes it:
// a failed dial (connection refused, or the file having vanished) means
// the previous owner died without cleaning up, so the stale socket file is
// unlinked and bind is retried (race-safely, never unlinking a socket a
// racing owner rebound in the probe window). A successful dial means a live
// daemon owns the socket, and Listen returns ErrDaemonRunning.
func Listen(dir string, opts ...Option) (wire.Listener, error) {
	if err := safedir.EnsurePrivate(dir); err != nil {
		return nil, fmt.Errorf("ipc: securing socket directory: %w", err)
	}

	sockPath := filepath.Join(dir, socketFileName)

	ln, err := listenUnixStaleSafe(sockPath)
	if err != nil {
		return nil, fmt.Errorf("ipc: listen on %s: %w", sockPath, err)
	}

	if err := os.Chmod(sockPath, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("ipc: chmod %s: %w", sockPath, err)
	}

	return &unixListener{ln: ln, addr: sockPath, opts: opts}, nil
}

// listenUnixStaleSafe binds sockPath, recovering race-safely from a stale
// socket file left behind by an owner that died without unlinking it. A socket
// still served by a live peer is reported as ErrDaemonRunning. A path that
// exists and is not a socket is refused without being removed, so a caller's
// foreign file is never unlinked.
//
// The recovery is race-safe: the socket file's inode is observed before the
// liveness probe and compared with the inode at removal time, so a socket a
// racing owner rebound in that window is never unlinked. When the inode
// changed, the bind is retried (and typically reports the racing owner live)
// instead of removing it.
func listenUnixStaleSafe(sockPath string) (*net.UnixListener, error) {
	for attempt := 0; attempt < staleRecoveryAttempts; attempt++ {
		ln, err := bindUnix(sockPath)
		if err == nil {
			return ln, nil
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			return nil, err
		}

		before, statErr := os.Lstat(sockPath)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue // a racing owner removed it; retry the bind
			}
			return nil, fmt.Errorf("ipc: inspecting socket %s: %w", sockPath, statErr)
		}
		if before.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("ipc: %s exists and is not a socket", sockPath)
		}

		if probeErr := probeLiveDaemon(sockPath); probeErr != nil {
			return nil, probeErr
		}

		current, statErr := os.Lstat(sockPath)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue // a racing owner removed it; retry the bind
			}
			return nil, fmt.Errorf("ipc: inspecting socket %s: %w", sockPath, statErr)
		}
		if !os.SameFile(before, current) {
			// A racing owner rebound the path after the probe said the old
			// socket was dead. Never unlink its live socket; retry the bind,
			// which will observe the new owner.
			continue
		}

		// The inode is still the stale one the probe found dead. Remove it and
		// retry the bind. A racing owner that binds between this removal and the
		// next bind simply wins; the next bind reports it live.
		if rmErr := os.Remove(sockPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return nil, fmt.Errorf("ipc: removing stale socket %s: %w", sockPath, rmErr)
		}
	}

	// Every attempt observed a live owner (or a racing owner kept winning).
	return nil, ErrDaemonRunning
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
// wire.Transport.
func (l *unixListener) Accept() (wire.Transport, error) {
	conn, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	return NewTransport(conn, l.opts...), nil
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
// exists, else /tmp/vev-<uid>. An XDG root too long for the longest
// production endpoint (broker/broker.sock) uses a deterministic, per-user
// private directory in /tmp instead. Hash the complete root so isolated
// environments never fall back to the ordinary user's daemon. Listeners and
// lifecycle locks still validate ownership and mode; no symlink is introduced.
func SocketDir() string {
	uid := os.Getuid()

	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		dir := filepath.Join(xdg, "vev")
		if len(filepath.Join(dir, "broker", "broker.sock")) > muxSocketPathMax {
			return fmt.Sprintf("/tmp/vev-%d-%x", uid, sha256.Sum256([]byte(dir)))
		}
		return dir
	}

	runUser := fmt.Sprintf("/run/user/%d", uid)
	if fi, err := os.Stat(runUser); err == nil && fi.IsDir() {
		return filepath.Join(runUser, "vev")
	}

	return fmt.Sprintf("/tmp/vev-%d", uid)
}
