package daemon

import (
	"time"

	"github.com/bnema/vev/internal/ports"
)

// timerOwnership is the coordinator-side ownership protocol for a timer lane.
// Its methods ending in Locked require the owning coordinator lock. Timer.C,
// Timer.Stop, and callbacks are deliberately kept out of this type and must
// run after that lock is released: ports may synchronously re-enter the
// coordinator.
//
// The pointers let each domain lane retain its own fields and predicates while
// sharing generation tickets, cancellation, and worker completion ownership.
type timerOwnership struct {
	timer      *ports.Timer
	cancel     *chan struct{}
	done       *chan struct{}
	generation *uint64
}

func timerOwner(timer *ports.Timer, cancel, done *chan struct{}, generation *uint64) timerOwnership {
	return timerOwnership{timer: timer, cancel: cancel, done: done, generation: generation}
}

// timerWorker is a detached timer and the completion edge of the worker
// waiting on it. Stop and wait must happen after releasing the owner lock:
// both Timer.Stop and a callback completing the worker may re-enter the
// coordinator.
type timerWorker struct {
	timer ports.Timer
	done  <-chan struct{}
}

// replaceLocked invalidates the current ticket and releases its timer worker
// for an unlocked cancellation and join. A worker is always released even when
// a fake timer's channel is nil or never fires.
func (o timerOwnership) replaceLocked() (uint64, timerWorker) {
	*o.generation = *o.generation + 1
	return *o.generation, o.detachLocked()
}

// detachLocked releases the current worker and timer. The returned worker must
// be stopped and joined after dropping the owner lock.
func (o timerOwnership) detachLocked() timerWorker {
	worker := timerWorker{timer: *o.timer, done: *o.done}
	*o.timer = nil
	if *o.cancel != nil {
		close(*o.cancel)
		*o.cancel = nil
	}
	return worker
}

// publishLocked assigns a timer created and inspected outside the owner lock.
// It rejects a stale ticket or an already-replaced timer without invoking a
// port method.
func (o timerOwnership) publishLocked(ticket uint64, timer ports.Timer) (cancel, done chan struct{}, ok bool) {
	if *o.generation != ticket || *o.timer != nil {
		return nil, nil, false
	}
	cancel, done = make(chan struct{}), make(chan struct{})
	*o.timer, *o.cancel, *o.done = timer, cancel, done
	return cancel, done, true
}

// clearLocked releases only the matching ticket after its worker has consumed
// a tick. It cannot clear a replacement timer.
func (o timerOwnership) clearLocked(ticket uint64) bool {
	if *o.generation != ticket {
		return false
	}
	*o.timer, *o.cancel = nil, nil
	return true
}

// completeLocked records a completed no-worker timer path (such as a disabled
// clock) without overwriting a replacement lane's completion edge.
func (o timerOwnership) completeLocked(ticket uint64) bool {
	if *o.generation != ticket {
		return false
	}
	done := make(chan struct{})
	close(done)
	*o.done = done
	return true
}

// runTimerWorker owns the completion edge for an armed timer. A nil timer
// channel intentionally does not start a worker; callers apply their
// domain-specific disabled-clock behavior synchronously.
func runTimerWorker(timerC <-chan time.Time, cancel <-chan struct{}, done chan struct{}, fire func()) {
	go func() {
		defer close(done)
		select {
		case <-timerC:
			fire()
		case <-cancel:
		}
	}()
}

func stopTimer(timer ports.Timer) {
	if timer != nil {
		timer.Stop()
	}
}

// stopAndJoinTimerWorker prevents a cancelled timer callback from escaping a
// lifecycle boundary and touching transports or test-owned timer mocks later.
// Callers must not hold coordinator locks here. self is supplied only by a
// worker that consumed its own tick; waiting for that worker would deadlock.
func stopAndJoinTimerWorker(worker timerWorker, self <-chan struct{}) {
	stopTimer(worker.timer)
	if worker.done != nil && worker.done != self {
		<-worker.done
	}
}

func stopAndJoinTimerWorkers(workers []timerWorker) {
	for _, worker := range workers {
		stopAndJoinTimerWorker(worker, nil)
	}
}
