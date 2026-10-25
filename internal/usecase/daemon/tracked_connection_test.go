package daemon

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/protocol/wire"
)

// connLog is a plain recorder for one tracked server connection. It implements
// no interface; the generated wire transport mock behind the connection feeds
// it through RunAndReturn closures.
type connLog struct {
	mu     sync.Mutex
	sends  []wire.Envelope
	closes int
	closed bool
	done   chan struct{}
}

func newConnLog() *connLog { return &connLog{done: make(chan struct{})} }

// record appends one sent envelope.
func (l *connLog) record(envelope wire.Envelope) {
	l.mu.Lock()
	l.sends = append(l.sends, envelope)
	l.mu.Unlock()
}

// markClosed records one Close call. Done is closed on the first call, after
// the closed flag is visible to Closed.
func (l *connLog) markClosed() {
	l.mu.Lock()
	l.closes++
	first := !l.closed
	l.closed = true
	l.mu.Unlock()
	if first {
		close(l.done)
	}
}

// Sends returns a copy of every envelope recorded so far.
func (l *connLog) Sends() []wire.Envelope {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]wire.Envelope(nil), l.sends...)
}

// Closed reports whether Close was called at least once.
func (l *connLog) Closed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

// CloseCount reports how many times Close was called.
func (l *connLog) CloseCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closes
}

// Done is closed on the first Close, after the closed flag is recorded.
func (l *connLog) Done() <-chan struct{} { return l.done }

// newTrackedConnection returns a server connection over a generated transport
// mock plus the log of its sends and closes. Options run first so their
// RunAndReturn expectations take precedence over the recording defaults
// registered afterwards (testify matches the first registered expectation).
func newTrackedConnection(t testing.TB, opts ...func(*mockServerConnection, *connLog)) (*mockServerConnection, *connLog) {
	t.Helper()
	conn := newMockServerConnection(t)
	log := newConnLog()
	for _, opt := range opts {
		opt(conn, log)
	}
	conn.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error {
		log.record(f)
		return nil
	}).Maybe()
	conn.EXPECT().Recv().RunAndReturn(func() (wire.Envelope, error) {
		return wire.Envelope{}, errors.New("closed")
	}).Maybe()
	conn.EXPECT().Close().RunAndReturn(func() error {
		log.markClosed()
		return nil
	}).Maybe()
	return conn, log
}

// newQuietConnection is newTrackedConnection for call sites that never inspect
// the log.
func newQuietConnection(t testing.TB) *mockServerConnection {
	t.Helper()
	conn, _ := newTrackedConnection(t)
	return conn
}
