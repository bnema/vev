package daemonmux

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

// errTerminalSentinel is a stable cause used across tests so error identity
// can be checked with errors.Is.
var errTerminalSentinel = errors.New("terminal sentinel")

func terminalDone(s *terminalState) bool {
	select {
	case <-s.Done():
		return true
	default:
		return false
	}
}

func TestTerminalStatePreTerminalGetters(t *testing.T) {
	s := newTerminalState()

	if terminalDone(s) {
		t.Fatal("Done closed before any publication")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("Err() = %v before publication, want nil", err)
	}
	if kind := s.FailureKind(); kind != domain.RemoteFailureNone {
		t.Fatalf("FailureKind() = %v before publication, want none", kind)
	}
}

func TestTerminalStateLocalClose(t *testing.T) {
	s := newTerminalState()
	s.Close()

	if !terminalDone(s) {
		t.Fatal("Done not closed after local Close")
	}
	if err := s.Err(); err != nil {
		t.Fatalf("Err() = %v after local Close, want nil", err)
	}
	if kind := s.FailureKind(); kind != domain.RemoteFailureNone {
		t.Fatalf("FailureKind() = %v after local Close, want none", kind)
	}

	// A second local Close stays orderly, and a later failure must not
	// overwrite the winning outcome.
	s.Close()
	s.Fail(domain.RemoteFailureTimeout, errors.New("late failure"))
	if err := s.Err(); err != nil {
		t.Fatalf("Err() = %v after late Fail, want nil", err)
	}
	if kind := s.FailureKind(); kind != domain.RemoteFailureNone {
		t.Fatalf("FailureKind() = %v after late Fail, want none", kind)
	}
}

type wrappedError struct{ err error }

func (w wrappedError) Error() string { return w.err.Error() }
func (w wrappedError) Unwrap() error { return w.err }

func TestTerminalStateFailurePreservesErrorIdentity(t *testing.T) {
	s := newTerminalState()
	cause := wrappedError{err: fmt.Errorf("dial daemon: %w", errTerminalSentinel)}
	s.Fail(domain.RemoteFailureTimeout, cause)

	if !terminalDone(s) {
		t.Fatal("Done not closed after Fail")
	}
	if got := s.Err(); got != cause {
		t.Fatalf("Err() = %v, want the exact published error value", got)
	}
	if !errors.Is(s.Err(), errTerminalSentinel) {
		t.Fatal("errors.Is lost the wrapped sentinel")
	}
	var wrapped wrappedError
	if !errors.As(s.Err(), &wrapped) {
		t.Fatal("errors.As lost the wrapped error identity")
	}
	if kind := s.FailureKind(); kind != domain.RemoteFailureTimeout {
		t.Fatalf("FailureKind() = %v, want timeout", kind)
	}

	// A later orderly Close must not clear the published failure.
	s.Close()
	if s.Err() != cause || s.FailureKind() != domain.RemoteFailureTimeout {
		t.Fatalf("Close overwrote failure: Err()=%v FailureKind()=%v", s.Err(), s.FailureKind())
	}
}

// TestTerminalStateNilCauseUsesSentinel proves a failure publisher that
// supplies no cause still publishes a non-nil Err() carrying the package
// sentinel, paired with a nonzero valid kind.
func TestTerminalStateNilCauseUsesSentinel(t *testing.T) {
	s := newTerminalState()
	s.Fail(domain.RemoteFailureTimeout, nil)

	if !terminalDone(s) {
		t.Fatal("Done not closed after Fail with a nil cause")
	}
	if s.Err() == nil {
		t.Fatal("Err() is nil after Fail with a nil cause")
	}
	if !errors.Is(s.Err(), ErrTerminalFailure) {
		t.Fatalf("Err() = %v, want ErrTerminalFailure", s.Err())
	}
	if kind := s.FailureKind(); kind != domain.RemoteFailureTimeout {
		t.Fatalf("FailureKind() = %v, want timeout", kind)
	}
	// A failure always pairs a non-nil error with a nonzero valid kind, so a
	// concurrent reader never observes the inconsistent (nil, nonzero) pair.
	if (s.Err() == nil) != (s.FailureKind() == domain.RemoteFailureNone) {
		t.Fatal("failure published an inconsistent (err, kind) pair")
	}

	// A nil cause with an out-of-range kind still normalizes the kind and
	// substitutes the sentinel.
	serialized := newTerminalState()
	serialized.Fail(domain.RemoteFailureKind(99), nil)
	if !errors.Is(serialized.Err(), ErrTerminalFailure) {
		t.Fatalf("Err() = %v, want ErrTerminalFailure", serialized.Err())
	}
	if kind := serialized.FailureKind(); kind != domain.RemoteFailureTransport {
		t.Fatalf("FailureKind() = %v, want transport", kind)
	}
}

func TestTerminalStateFailureKindNormalization(t *testing.T) {
	tests := []struct {
		name string
		give domain.RemoteFailureKind
		want domain.RemoteFailureKind
	}{
		{name: "none normalizes to transport", give: domain.RemoteFailureNone, want: domain.RemoteFailureTransport},
		{name: "out of range normalizes to transport", give: domain.RemoteFailureKind(99), want: domain.RemoteFailureTransport},
		{name: "transport stays", give: domain.RemoteFailureTransport, want: domain.RemoteFailureTransport},
		{name: "timeout stays", give: domain.RemoteFailureTimeout, want: domain.RemoteFailureTimeout},
		{name: "trust stays", give: domain.RemoteFailureTrust, want: domain.RemoteFailureTrust},
		{name: "authentication stays", give: domain.RemoteFailureAuthentication, want: domain.RemoteFailureAuthentication},
		{name: "incompatible stays", give: domain.RemoteFailureIncompatible, want: domain.RemoteFailureIncompatible},
		{name: "invalid response stays", give: domain.RemoteFailureInvalidResponse, want: domain.RemoteFailureInvalidResponse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTerminalState()
			s.Fail(tt.give, errors.New("boom"))
			if got := s.FailureKind(); got != tt.want {
				t.Fatalf("FailureKind() = %v, want %v", got, tt.want)
			}
			if s.Err() == nil {
				t.Fatal("Err() is nil after Fail")
			}
		})
	}
}

func TestTerminalStateFirstWinsConcurrentPublishers(t *testing.T) {
	const publishers = 100
	validKinds := []domain.RemoteFailureKind{
		domain.RemoteFailureTransport,
		domain.RemoteFailureTimeout,
		domain.RemoteFailureTrust,
		domain.RemoteFailureAuthentication,
		domain.RemoteFailureIncompatible,
		domain.RemoteFailureInvalidResponse,
	}

	s := newTerminalState()
	publishedErrs := make([]error, publishers)
	publishedKinds := make([]domain.RemoteFailureKind, publishers)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < publishers; i++ {
		publishedErrs[i] = fmt.Errorf("publisher %d: %w", i, errTerminalSentinel)
		publishedKinds[i] = validKinds[i%len(validKinds)]
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			s.Fail(publishedKinds[i], publishedErrs[i])
		}(i)
	}
	close(start)
	wg.Wait()

	if !terminalDone(s) {
		t.Fatal("Done not closed after concurrent publishers")
	}
	winnerErr := s.Err()
	winnerKind := s.FailureKind()
	if winnerErr == nil {
		t.Fatal("Err() is nil after failure publishers")
	}
	if !errors.Is(winnerErr, errTerminalSentinel) {
		t.Fatal("winner error lost the sentinel identity")
	}

	matches := 0
	for i := range publishedErrs {
		if winnerErr != publishedErrs[i] {
			continue
		}
		matches++
		if winnerKind != publishedKinds[i] {
			t.Fatalf("winner error from publisher %d paired with kind %v, want %v", i, winnerKind, publishedKinds[i])
		}
	}
	if matches != 1 {
		t.Fatalf("exactly one publisher must win, got %d", matches)
	}

	// Later publications, local or failing, never change the winner.
	s.Close()
	s.Fail(domain.RemoteFailureTrust, errors.New("late failure"))
	if s.Err() != winnerErr || s.FailureKind() != winnerKind {
		t.Fatalf("terminal values changed: (%v, %v)", s.Err(), s.FailureKind())
	}
}

func TestTerminalStateConcurrentGettersAndPublishers(t *testing.T) {
	const goroutines = 100

	s := newTerminalState()
	var wg sync.WaitGroup
	var inconsistent atomic.Int32
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			switch i % 4 {
			case 0:
				s.Fail(domain.RemoteFailureTransport, fmt.Errorf("failure %d: %w", i, errTerminalSentinel))
			case 1:
				s.Close()
			default:
				select {
				case <-s.Done():
					err := s.Err()
					kind := s.FailureKind()
					// Failure pairs a non-nil error with a nonzero valid
					// kind; an orderly local Close pairs nil with none.
					if (err == nil) != (kind == domain.RemoteFailureNone) {
						inconsistent.Add(1)
					}
				default:
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if !terminalDone(s) {
		t.Fatal("Done not closed after concurrent getters and publishers")
	}
	if inconsistent.Load() != 0 {
		t.Fatalf("observed %d inconsistent terminal reads", inconsistent.Load())
	}

	// Exactly-once close: repeated publication never panics and never changes
	// the terminal outcome.
	gotErr := s.Err()
	gotKind := s.FailureKind()
	for i := 0; i < 10; i++ {
		s.Close()
		s.Fail(domain.RemoteFailureTimeout, errors.New("late failure"))
		if s.Err() != gotErr || s.FailureKind() != gotKind {
			t.Fatalf("terminal values changed: (%v, %v) -> (%v, %v)", gotErr, gotKind, s.Err(), s.FailureKind())
		}
	}
}
