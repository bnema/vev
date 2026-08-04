package daemon

import (
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// timerToken is the complete, immutable identity of one timer worker. A lane
// owns either this token or nothing; cancellation and completion are never
// retained separately where a stale callback could reacquire them.
type timerToken struct {
	generation uint64
	timer      ports.Timer
	cancel     chan struct{}
	done       chan struct{}
}

// timerLane is coordinator-owned and must be accessed while renderCoordinator.mu
// is held. Detach removes the complete token before any external Timer call.
type timerLane struct {
	generation uint64
	token      *timerToken
}

func (l *timerLane) replaceLocked() (uint64, *timerToken) {
	l.generation++
	return l.generation, l.detachLocked()
}

func (l *timerLane) detachLocked() *timerToken {
	token := l.token
	l.token = nil
	if token != nil {
		close(token.cancel)
	}
	return token
}

func (l *timerLane) publishLocked(generation uint64, timer ports.Timer) *timerToken {
	if l.generation != generation || l.token != nil {
		return nil
	}
	token := &timerToken{
		generation: generation,
		timer:      timer,
		cancel:     make(chan struct{}),
		done:       make(chan struct{}),
	}
	l.token = token
	return token
}

func (l *timerLane) clearLocked(token *timerToken) bool {
	if token == nil || l.generation != token.generation || l.token != token {
		return false
	}
	l.token = nil
	return true
}

// timerSupervisor owns worker accounting. Workers are registered only while
// coordinator ownership is held and terminal teardown rejects registrations
// before Wait begins, so Add cannot race the terminal Wait.
type timerSupervisor struct{ workers sync.WaitGroup }

func (s *timerSupervisor) startLocked(token *timerToken, timerC <-chan time.Time, fire func()) {
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		defer close(token.done)
		select {
		case <-timerC:
			fire()
		case <-token.cancel:
		}
	}()
}

func (s *timerSupervisor) wait() { s.workers.Wait() }

// stopDetachedTimer is intentionally non-blocking. Ordinary lifecycle and
// attachment transitions may cancel/stop stale workers but callback stacks
// never wait for a worker to finish.
func stopDetachedTimer(token *timerToken) {
	if token != nil && token.timer != nil {
		token.timer.Stop()
	}
}
