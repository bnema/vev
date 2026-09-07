//go:build windows

package uidriver

import "net"

// Windows does not expose a peer-credential check through this adapter yet.
// Fail closed rather than accepting an unverified UI-driver connection.
func peerIsCurrentUser(*net.UnixConn) bool { return false }
