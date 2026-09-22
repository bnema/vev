package client

import (
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// ProtocolError is a session- or protocol-level failure reported before attach.
type ProtocolError struct {
	Code uint16
	Text string
}

func (e *ProtocolError) Error() string {
	if e.Text == "" {
		return fmt.Sprintf("vev: daemon rejected attach (code %d)", e.Code)
	}
	return fmt.Sprintf("vev: %s", e.Text)
}

func newClientID() [16]byte {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic("crypto/rand failed generating client ID: " + err.Error())
	}
	return id
}

func requestedOutputWindow(connection ports.ClientConnection) uint8 {
	if windowed, ok := connection.(interface{ OutputWindow() uint8 }); ok {
		return windowed.OutputWindow()
	}
	return 1
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

type reconnectStage uint8

const (
	reconnectStageDegraded reconnectStage = iota + 1
	reconnectStageProbingUDP
	reconnectStageSSH
	reconnectStageOfflineRetrying
)
