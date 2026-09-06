//go:build !linux && !darwin && !windows

package uidriver

import "net"

// Unsupported systems rely on the private 0700 parent and 0600 socket
// permissions because no platform-specific peer credential implementation is
// available here.
func peerIsCurrentUser(*net.UnixConn) bool { return true }
