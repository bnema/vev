package daemonmux

import (
	"errors"
	"sync"

	"github.com/bnema/vev/internal/domain"
)

// ErrTerminalFailure is the stable package sentinel published by Fail when it
// is called with a nil cause. A failure always pairs a non-nil Err() with a
// nonzero RemoteFailureKind, so Fail substitutes this sentinel for a nil cause
// rather than publish an inconsistent terminal outcome.
var ErrTerminalFailure = errors.New("daemonmux: terminal failure without a cause")

// terminalState is the publish-once terminal authority shared by a physical
// daemon connection and every logical stream it owns. It mirrors the
// Done/Err/FailureKind contract of ports.BrokerPhysicalConnection: once the
// connection reaches a terminal outcome, every getter returns the same stable
// values and Done is closed exactly once, independent of reads.
//
// Exactly one publication wins. The first concurrent Fail or Close decides Err
// and FailureKind; later publishers observe the winner and never change it. An
// orderly local Close publishes a nil error and RemoteFailureNone. A failure
// publishes its cause and a nonzero valid RemoteFailureKind, preserves the
// error identity for errors.Is/As, and normalizes any kind outside the
// transport-failure taxonomy (including RemoteFailureNone) to
// RemoteFailureTransport. A failure never publishes a nil error: a nil cause is
// replaced with the ErrTerminalFailure sentinel.
type terminalState struct {
	once sync.Once
	done chan struct{}
	err  error
	kind domain.RemoteFailureKind
}

// newTerminalState returns an open terminal state. Done stays open until a
// publisher wins.
func newTerminalState() *terminalState {
	return &terminalState{done: make(chan struct{})}
}

// Done returns a channel closed exactly once when the terminal outcome is
// published. The channel is stable and this method is safe before or after
// publication.
func (s *terminalState) Done() <-chan struct{} { return s.done }

// Err returns the published failure cause: nil while open and for an orderly
// local Close. The result is stable once Done is closed.
func (s *terminalState) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

// FailureKind returns the published failure classification: RemoteFailureNone
// while open and for an orderly local Close. The result is stable once Done is
// closed.
func (s *terminalState) FailureKind() domain.RemoteFailureKind {
	select {
	case <-s.done:
		return s.kind
	default:
		return domain.RemoteFailureNone
	}
}

// Fail publishes a failure terminal outcome. It wins only while the state is
// open, so at most one publisher ever applies. The kind must be a nonzero
// valid RemoteFailureKind; any other value, including RemoteFailureNone, is
// normalized to RemoteFailureTransport. A non-nil error is stored verbatim so
// callers keep errors.Is/As identity; a nil error is replaced with the
// ErrTerminalFailure package sentinel so a failure never publishes a nil Err()
// alongside a nonzero kind.
func (s *terminalState) Fail(kind domain.RemoteFailureKind, err error) {
	s.publishFail(kind, err)
}

// publishFail applies a failure outcome exactly once and reports whether this
// call won the publication. It is the primitive behind Fail and the callers
// that must know they are the one publisher - for example, so a stream signals
// its peer with exactly one Reset instead of one per observer. The first
// publisher's values win; a later caller observes false and must not apply its
// own outcome.
func (s *terminalState) publishFail(kind domain.RemoteFailureKind, err error) bool {
	if kind < domain.RemoteFailureTransport || kind > domain.RemoteFailureInvalidResponse {
		kind = domain.RemoteFailureTransport
	}
	if err == nil {
		err = ErrTerminalFailure
	}
	won := false
	s.once.Do(func() {
		s.err = err
		s.kind = kind
		close(s.done)
		won = true
	})
	return won
}

// Close publishes an orderly local terminal outcome: a nil error and
// RemoteFailureNone. It wins only while the state is open.
func (s *terminalState) Close() {
	s.publish(nil, domain.RemoteFailureNone)
}

// publish applies the first outcome exactly once and then closes Done. The
// winning values are stored before close(done), so every reader that observes
// the closed channel sees them.
func (s *terminalState) publish(err error, kind domain.RemoteFailureKind) {
	s.once.Do(func() {
		s.err = err
		s.kind = kind
		close(s.done)
	})
}
