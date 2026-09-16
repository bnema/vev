//go:build !unix

package daemonmux

// sshMuxProcessGone is a no-op on platforms without unix process signaling.
func sshMuxProcessGone(int) bool { return true }
