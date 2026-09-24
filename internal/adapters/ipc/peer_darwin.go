//go:build darwin

package ipc

import (
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// verifySameUserUnixPeer returns nil only when the peer at the other end of an
// AF_UNIX connection is the current effective user, as reported by the kernel
// through LOCAL_PEERCRED. A socket whose credentials cannot be read is refused.
func verifySameUserUnixPeer(conn *net.UnixConn) error {
	if conn == nil {
		return ErrMuxPeerRejected
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMuxPeerRejected, err)
	}
	var credential *unix.Xucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
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
