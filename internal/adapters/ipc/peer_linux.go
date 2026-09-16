//go:build linux

package ipc

import (
	"fmt"
	"net"
	"os"
	"syscall"
)

// verifySameUserUnixPeer returns nil only when the peer at the other end of an
// AF_UNIX connection is the current effective user, as reported by the kernel
// through SO_PEERCRED.
//
// The check is deliberately explicit and relies on no filesystem permission and
// no X11-style convention that a private socket directory makes the peer
// trustworthy: the kernel's own credential record for this connection is the
// authority. A socket whose credentials cannot be read is refused, so a build
// that cannot prove the peer is same-user fails closed.
func verifySameUserUnixPeer(conn *net.UnixConn) error {
	if conn == nil {
		return ErrMuxPeerRejected
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMuxPeerRejected, err)
	}
	var credential *syscall.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return fmt.Errorf("%w: %v", ErrMuxPeerRejected, err)
	}
	if controlErr != nil {
		return fmt.Errorf("%w: %v", ErrMuxPeerRejected, controlErr)
	}
	if credential == nil {
		return ErrMuxPeerRejected
	}
	if uid := uint32(os.Geteuid()); uid != credential.Uid {
		return fmt.Errorf("%w: peer uid %d, want %d", ErrMuxPeerRejected, credential.Uid, uid)
	}
	return nil
}
