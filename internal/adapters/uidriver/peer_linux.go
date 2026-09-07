//go:build linux

package uidriver

import (
	"net"
	"os"
	"syscall"
)

func peerIsCurrentUser(conn *net.UnixConn) bool {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false
	}
	var credential *syscall.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil || controlErr != nil || credential == nil {
		return false
	}
	return uint32(os.Geteuid()) == credential.Uid
}
