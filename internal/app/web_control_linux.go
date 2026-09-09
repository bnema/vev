//go:build linux

package app

import (
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bnema/vev/internal/platform"
)

// Linux abstract sockets have no filesystem artifact. Peer credentials, not
// the discoverability of this name, protect the local control interface.
func webControlAddress() string {
	path, _ := filepath.Abs(platform.StateDir())
	return fmt.Sprintf("@vev-web-%d-%x", os.Geteuid(), sha256.Sum256([]byte(path)))
}

func sameWebUID(conn *net.UnixConn) bool {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false
	}
	var credential *syscall.Ucred
	var peerErr error
	err = raw.Control(func(fd uintptr) {
		credential, peerErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	return err == nil && peerErr == nil && credential != nil && credential.Uid == uint32(os.Geteuid())
}

func bindWebControl() (*net.UnixListener, error) {
	return net.ListenUnix("unix", &net.UnixAddr{Name: webControlAddress(), Net: "unix"})
}
