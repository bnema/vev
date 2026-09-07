//go:build darwin

package uidriver

import (
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func peerIsCurrentUser(conn *net.UnixConn) bool {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false
	}
	var credential *unix.Xucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credential, controlErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil || controlErr != nil || credential == nil {
		return false
	}
	return uint32(os.Geteuid()) == credential.Uid
}
