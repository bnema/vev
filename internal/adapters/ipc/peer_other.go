//go:build !linux && !darwin

package ipc

import "net"

// verifySameUserUnixPeer fails closed on a platform or build without a
// same-user credential check wired into this adapter. The private
// carriage never admits an unverified peer: a caller sees ErrMuxUnsupported
// rather than a same-user result inferred from filesystem permissions or an
// X11-style trust assumption.
func verifySameUserUnixPeer(*net.UnixConn) error {
	return ErrMuxUnsupported
}
