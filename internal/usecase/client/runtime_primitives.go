package client

import (
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// ProtocolError is a session- or protocol-level failure reported before attach.
type ProtocolError struct {
	Code uint16
	Text string
}

func (e *ProtocolError) Error() string {
	if e.Text == "" {
		return fmt.Sprintf("daemon rejected attach (code %d)", e.Code)
	}
	return e.Text
}

func newClientID() [16]byte {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic("crypto/rand failed generating client ID: " + err.Error())
	}
	return id
}

// requestedOutputWindow is the number of unacknowledged Output states this
// client lets the daemon keep in flight: what the carriage prefers, or the
// protocol maximum when it states no preference. A window of one makes every
// frame wait a full round trip, which stalls scrolling on any remote link.
func requestedOutputWindow(connection ports.ClientConnection) uint8 {
	if window := connection.Capabilities().PreferredOutputWindow; window != 0 {
		return window
	}
	return protocol.MaxOutputWindow
}

type foregroundSendLease struct {
	mu     sync.Mutex
	active bool
}

func newForegroundSendLease() *foregroundSendLease { return &foregroundSendLease{active: true} }

func (l *foregroundSendLease) send(send func() bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active && send()
}

func (l *foregroundSendLease) stop() {
	l.mu.Lock()
	l.active = false
	l.mu.Unlock()
}

const stdinBufSize = 4096
