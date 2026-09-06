//go:build !linux

package uidriver

import "net"

// Unix peer credentials are a Linux security requirement. Other supported
// systems rely on the private 0700 parent and 0600 socket permissions.
func peerIsCurrentUser(*net.UnixConn) bool { return true }
