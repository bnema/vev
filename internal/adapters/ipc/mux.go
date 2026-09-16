// Private daemonmux Unix carriage (P3.2d).
//
// This file exposes the explicit, mux-specific raw Unix carriage that the
// daemonmux multiplexer (internal/adapters/daemonmux) runs over. It is
// deliberately separate from the daemon's public Listen/DialContext pair: the
// mux carriage is bound at a caller-supplied, isolated path, never the live
// production daemon.sock, so the two endpoints cannot be confused and the
// daemon's public socket is never reused or inspected here.
//
// Both ends of the carriage are authenticated before they are handed out. A
// listener and a dialer verify that the connected AF_UNIX peer is the current
// effective user from the kernel's own peer credentials (SO_PEERCRED on
// Linux), not from filesystem permissions or an X11-style "the directory is
// private so the peer is trusted" assumption. On a platform or build without a
// same-user peer-credential check this file fails closed: it refuses the
// connection rather than admitting an unverified peer (peer_other.go).
//
// The carriage reuses the existing IPC framing and transport (NewTransport,
// streamframe, RecvBounded) and the shared race-safe stale-socket recovery
// (listenUnixStaleSafe). Listener setup creates or validates an owner-only
// (0700) parent directory with safedir.EnsurePrivate and tightens the bound
// socket to 0600; a foreign path that is not a socket is refused without being
// removed. Close stops Accept, unlinks only the socket inode this listener
// created, and leaves a caller replacement alone.

package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/pkg/safedir"
)

const (
	// muxSocketMode is the owner-only permission a bound mux socket is
	// tightened to after bind.
	muxSocketMode = 0o600
	// muxSocketPathMax is the worst-case AF_UNIX pathname length observed
	// across the supported platforms (BSD/macOS 103 < Linux 107). A mux path
	// longer than this is refused on every platform, so a path that binds on
	// Linux can never be silently truncated on macOS.
	muxSocketPathMax = 103
)

// Mux carriage sentinels. They are typed so a caller can classify a path,
// platform, or peer-credential outcome without matching on message text.
var (
	// ErrMuxPath reports an invalid caller-supplied carriage path: empty,
	// relative, unclean, too long, containing a NUL, or naming a connection
	// that is not a Unix socket.
	ErrMuxPath = errors.New("ipc: invalid mux socket path")
	// ErrMuxForeignPath reports a mux path that already exists and is not a
	// socket. ListenMux refuses it without removing the caller's file.
	ErrMuxForeignPath = errors.New("ipc: mux socket path exists and is not a socket")
	// ErrMuxUnsupported reports a same-user peer-credential check on a
	// platform or build that has none. The carriage fails closed: it never
	// admits an unverified peer.
	ErrMuxUnsupported = errors.New("ipc: same-user peer credential verification is unsupported on this platform")
	// ErrMuxPeerRejected reports a connected AF_UNIX peer whose kernel
	// credentials are not the current effective user, or could not be read.
	ErrMuxPeerRejected = errors.New("ipc: mux peer failed same-user credential verification")
)

// PeerVerifier checks one connected AF_UNIX peer's kernel credentials. It
// returns nil only for a peer proven to be the current effective user. It is a
// function type so a caller (or test) can inject an observable refusal without
// root; SameUserPeerVerifier returns the platform default.
type PeerVerifier func(conn *net.UnixConn) error

// muxPeerVerifier is the in-package spelling of PeerVerifier.
type muxPeerVerifier = PeerVerifier

// SameUserPeerVerifier returns this build's default same-user peer verifier:
// Linux SO_PEERCRED admission, or a fail-closed refusal on a platform or
// build without it.
func SameUserPeerVerifier() PeerVerifier { return verifySameUserUnixPeer }

// MuxListener is the listener half of a private daemonmux Unix carriage. Accept
// returns a raw bounded transport (Send/RecvBounded/Close) ready for the
// daemonmux FramedCarrierBridge, and verifies same-user peer credentials before
// handing it out. Close unblocks a blocked Accept and removes only the socket
// inode this listener created.
type MuxListener interface {
	Accept() (wire.BoundedTransport, error)
	Close() error
	Addr() string
}

// muxListener implements MuxListener over one bound AF_UNIX SOCK_STREAM socket.
type muxListener struct {
	ln     *net.UnixListener
	path   string
	file   os.FileInfo
	opts   []Option
	verify muxPeerVerifier

	closeOnce sync.Once
	closeErr  error
}

var _ MuxListener = (*muxListener)(nil)

// ListenMux binds the private daemonmux carriage at exactly path and returns a
// listener that verifies same-user peer credentials before handing out a raw
// bounded transport.
//
// path is caller-supplied and isolation is the caller's responsibility: it must
// be an absolute, cleaned path inside an owner-only directory and must not be
// the daemon's public daemon.sock. ListenMux creates or validates the parent
// directory as 0700 (safedir.EnsurePrivate), refuses a path that already exists
// and is not a socket without removing it, recovers race-safely from a stale
// socket left by a dead owner, and tightens the bound socket to 0600. A live
// owner already bound at path is reported as ErrDaemonRunning.
func ListenMux(path string, opts ...Option) (MuxListener, error) {
	return listenMux(path, verifySameUserUnixPeer, opts...)
}

// ListenMuxWithPeerVerifier binds the private carriage exactly like ListenMux
// but admits peers with an injected verifier instead of the platform default.
// It exists so a caller or test can supply an explicit admission policy (or
// observe a deterministic refusal without root); a nil verifier falls back to
// the platform default. Every other security property - owner-only parent,
// 0600 socket, race-safe stale recovery, foreign-path refusal, inode-scoped
// unlink on Close - is unchanged.
func ListenMuxWithPeerVerifier(path string, verify PeerVerifier, opts ...Option) (MuxListener, error) {
	return listenMux(path, verify, opts...)
}

// listenMux is ListenMux with an injected peer verifier, so a test can observe
// a credential refusal deterministically without root. A nil verifier falls
// back to the platform default.
func listenMux(path string, verify muxPeerVerifier, opts ...Option) (MuxListener, error) {
	if err := validateMuxSocketPath(path); err != nil {
		return nil, err
	}
	if verify == nil {
		verify = verifySameUserUnixPeer
	}
	if err := prepareMuxParent(path); err != nil {
		return nil, err
	}
	if err := inspectMuxSocketPath(path); err != nil {
		return nil, err
	}

	ln, err := listenUnixStaleSafe(path)
	if err != nil {
		return nil, fmt.Errorf("ipc: listen on mux socket %s: %w", path, err)
	}
	// The listener must never unlink a pathname it did not create. A caller may
	// replace the path after bind; Close preserves that replacement.
	ln.SetUnlinkOnClose(false)

	cleanup := func(cause error) (MuxListener, error) {
		_ = ln.Close()
		return nil, cause
	}
	if err := os.Chmod(path, muxSocketMode); err != nil {
		return cleanup(fmt.Errorf("ipc: securing mux socket %s: %w", path, err))
	}
	info, err := os.Lstat(path)
	if err != nil {
		return cleanup(fmt.Errorf("ipc: stat mux socket %s: %w", path, err))
	}
	if info.Mode()&os.ModeSocket == 0 {
		return cleanup(fmt.Errorf("%w: %s", ErrMuxForeignPath, path))
	}
	return &muxListener{ln: ln, path: path, file: info, opts: opts, verify: verify}, nil
}

// DialMuxContext connects to the private daemonmux carriage at exactly path and
// returns a raw bounded transport (Send/RecvBounded/Close) ready for the
// daemonmux FramedCarrierBridge. The peer's kernel credentials are verified as
// the current effective user before the transport is returned; a peer that
// cannot be proven same-user is refused and its connection closed.
//
// A dial failure (no carriage bound, or a stale path) is wrapped so callers can
// distinguish it; cancellation and deadline errors are preserved for
// classification.
func DialMuxContext(ctx context.Context, path string, opts ...Option) (wire.BoundedTransport, error) {
	return dialMux(ctx, path, verifySameUserUnixPeer, opts...)
}

// DialMuxContextWithPeerVerifier connects to the private carriage exactly like
// DialMuxContext but verifies the accepted peer with an injected verifier
// instead of the platform default. A nil verifier falls back to the platform
// default; a peer the verifier refuses is closed without being handed out.
func DialMuxContextWithPeerVerifier(ctx context.Context, path string, verify PeerVerifier, opts ...Option) (wire.BoundedTransport, error) {
	return dialMux(ctx, path, verify, opts...)
}

// dialMux is DialMuxContext with an injected peer verifier, so a test can
// observe a credential refusal deterministically without root. A nil verifier
// falls back to the platform default.
func dialMux(ctx context.Context, path string, verify muxPeerVerifier, opts ...Option) (wire.BoundedTransport, error) {
	if err := validateMuxSocketPath(path); err != nil {
		return nil, err
	}
	if verify == nil {
		verify = verifySameUserUnixPeer
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("ipc: dial mux socket %s: %w", path, err)
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: %s is not a Unix connection", ErrMuxPath, path)
	}
	if err := verify(unixConn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	transport, ok := NewTransport(conn, opts...).(wire.BoundedTransport)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: %s transport is not bounded", ErrMuxPath, path)
	}
	return transport, nil
}

// Accept waits for the next connection on the bound socket. Before handing a
// transport out it verifies the peer's same-user kernel credentials; a refused
// peer is closed and the refusal returned, so an unverified carriage is never
// admitted. Accept fails once the listener is closed (Close unblocks it).
func (l *muxListener) Accept() (wire.BoundedTransport, error) {
	conn, err := l.ln.AcceptUnix()
	if err != nil {
		return nil, err
	}
	if err := l.verify(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	transport, ok := NewTransport(conn, l.opts...).(wire.BoundedTransport)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: %s transport is not bounded", ErrMuxPath, l.path)
	}
	return transport, nil
}

// Addr returns the bound filesystem path.
func (l *muxListener) Addr() string { return l.path }

// Close stops accepting, unblocking a blocked Accept, and removes the socket
// only if the pathname still names the inode this listener created. A caller
// replacement is preserved. Close is idempotent and concurrent-safe.
func (l *muxListener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		l.closeErr = l.ln.Close()
		if err := removeMuxSocket(l.path, l.file); err != nil {
			l.closeErr = errors.Join(l.closeErr, err)
		}
	})
	return l.closeErr
}

// validateMuxSocketPath refuses a caller path that cannot be a portable private
// carriage endpoint: it must be absolute, already cleaned, within the
// cross-platform AF_UNIX pathname limit, free of NUL, and name a real base.
func validateMuxSocketPath(path string) error {
	if path == "" || !filepath.IsAbs(path) {
		return fmt.Errorf("%w: path must be absolute", ErrMuxPath)
	}
	if filepath.Clean(path) != path || path == string(filepath.Separator) {
		return fmt.Errorf("%w: path must be cleaned", ErrMuxPath)
	}
	base := filepath.Base(path)
	if base == "." || base == ".." || base == string(filepath.Separator) {
		return fmt.Errorf("%w: invalid base name", ErrMuxPath)
	}
	if len(path) > muxSocketPathMax {
		return fmt.Errorf("%w: path exceeds %d bytes", ErrMuxPath, muxSocketPathMax)
	}
	for i := 0; i < len(path); i++ {
		if path[i] == 0 {
			return fmt.Errorf("%w: path contains a NUL", ErrMuxPath)
		}
	}
	return nil
}

// prepareMuxParent creates or validates the carriage's parent directory as an
// owner-only (0700) directory owned by the current user.
func prepareMuxParent(path string) error {
	if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
		return fmt.Errorf("ipc: securing mux socket directory: %w", err)
	}
	return nil
}

// inspectMuxSocketPath refuses an existing path that is not a socket before any
// bind or liveness probe, so a caller's regular file, directory, or symlink is
// never mistaken for a stale endpoint and never removed.
func inspectMuxSocketPath(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("ipc: inspecting mux socket %s: %w", path, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("%w: %s", ErrMuxForeignPath, path)
	}
	return nil
}

// removeMuxSocket removes path only when it still names expected, so a socket
// a caller replaced during Close is preserved.
func removeMuxSocket(path string, expected os.FileInfo) error {
	if expected == nil {
		return nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !os.SameFile(info, expected) {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
