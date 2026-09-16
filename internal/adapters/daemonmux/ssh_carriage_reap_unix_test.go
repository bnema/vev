//go:build unix

package daemonmux

import (
	"errors"
	"syscall"
)

// sshMuxProcessGone reports whether pid no longer names any process, including a
// zombie. A zombie remains signalable, so kill(pid, 0) reports ESRCH only once
// the child was reaped.
func sshMuxProcessGone(pid int) bool {
	if pid <= 0 {
		return true
	}
	return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
}
