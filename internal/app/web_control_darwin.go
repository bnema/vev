//go:build darwin

package app

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/pkg/safedir"
)

// Darwin has no abstract Unix sockets, so the control interface binds a
// 0600 socket file in a private per-user directory. Peer credentials, not
// the discoverability of this path, protect the local control interface.
func webControlAddress() string {
	path, _ := filepath.Abs(platform.StateDir())
	sum := sha256.Sum256([]byte(path))
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("vev-web-%d", os.Getuid()))
	return filepath.Join(dir, fmt.Sprintf("%x.sock", sum[:8]))
}

func sameWebUID(conn *net.UnixConn) bool {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false
	}
	var credential *unix.Xucred
	var peerErr error
	err = raw.Control(func(fd uintptr) {
		credential, peerErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	})
	return err == nil && peerErr == nil && credential != nil && credential.Uid == uint32(os.Geteuid())
}

func bindWebControl() (*net.UnixListener, error) {
	path := webControlAddress()
	if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("vev: securing web control directory: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		if !errors.Is(err, syscall.EADDRINUSE) {
			return nil, err
		}
		// Distinguish a live gateway from a stale socket left by a crash.
		probeConn, probeErr := net.DialTimeout("unix", path, time.Second)
		if probeErr == nil {
			_ = probeConn.Close()
			return nil, errors.New("vev: web gateway control is already bound")
		}
		if !errors.Is(probeErr, syscall.ECONNREFUSED) && !errors.Is(probeErr, syscall.ENOENT) {
			return nil, fmt.Errorf("vev: probing web control socket: %w", probeErr)
		}
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
			return nil, rmErr
		}
		listener, err = net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			return nil, err
		}
	}
	return listener, secureWebControlSocket(listener, path)
}

func secureWebControlSocket(listener *net.UnixListener, path string) error {
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return fmt.Errorf("vev: securing web control socket: %w", err)
	}
	return nil
}
