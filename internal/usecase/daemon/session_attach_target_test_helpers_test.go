package daemon

import "bnema.dev/vev/internal/protocol"

// ptrSessionAttachTarget builds the pointer form Resume.SessionTarget and
// similar fields require from a value literal.
func ptrSessionAttachTarget(target protocol.SessionAttachTarget) *protocol.SessionAttachTarget {
	return &target
}
