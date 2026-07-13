package daemon

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestTimerOwnershipRejectsStaleTicketsAndCancelsWorkers(t *testing.T) {
	var timer ports.Timer
	var cancel, done chan struct{}
	var generation uint64
	owner := timerOwner(&timer, &cancel, &done, &generation)

	ticket, stopped := owner.replaceLocked()
	require.Nil(t, stopped.timer)
	first := &portsmocks.MockTimer{}
	first.EXPECT().Stop().Return(true).Once()
	firstCancel, firstDone, ok := owner.publishLocked(ticket, first)
	require.True(t, ok)

	// A replacement releases a nil-channel worker through cancellation, rather
	// than relying on Timer.Stop to wake it.
	nilTimerC := (<-chan time.Time)(nil)
	runTimerWorker(nilTimerC, firstCancel, firstDone, func() { t.Fatal("cancelled worker fired") })
	_, stopped = owner.replaceLocked()
	require.Same(t, first, stopped.timer)
	stopAndJoinTimerWorker(stopped, nil)

	_, _, ok = owner.publishLocked(ticket, &portsmocks.MockTimer{})
	require.False(t, ok, "a stale generation must not reclaim a replacement lane")

	require.True(t, owner.completeLocked(generation))
	awaitTimerWorker(t, done)
}

func TestRunTimerWorkerOwnsCompletionAfterTick(t *testing.T) {
	timerC := make(chan time.Time, 1)
	cancel := make(chan struct{})
	done := make(chan struct{})
	fired := make(chan struct{}, 1)
	runTimerWorker(timerC, cancel, done, func() { fired <- struct{}{} })
	timerC <- time.Time{}
	awaitTimerWorker(t, done)
	select {
	case <-fired:
	default:
		t.Fatal("timer worker did not run callback")
	}
}

func awaitTimerWorker(t *testing.T, done <-chan struct{}) {
	t.Helper()
	for range 4096 {
		select {
		case <-done:
			return
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("timer worker did not complete")
}
