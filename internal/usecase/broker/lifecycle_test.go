package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// newTestSupervisor builds a supervisor over a manual clock and always drains it
// at test end so an unexpired run goroutine can never leak into another test.
func newTestSupervisor(t testing.TB, grace time.Duration) (*Supervisor, *manualClock) {
	t.Helper()
	clock := newManualClock(time.Unix(0, 0))
	s, err := NewSupervisor(clock, SupervisorConfig{IdleGrace: grace})
	require.NoError(t, err)
	// Ignore the error: a test may deliberately register a failing drain hook.
	t.Cleanup(func() { _ = s.Close() })
	return s, clock
}

// activeTimers reports the manual clock's live (armed, not yet fired) timers.
func (c *manualClock) activeTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

func supervisorState(s *Supervisor) (clients, operations int, armed, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clients, s.operations, s.timerArmed, s.closed
}

func supervisorTimerArmed(s *Supervisor) bool {
	_, _, armed, _ := supervisorState(s)
	return armed
}

func requireSupervisorOpen(t *testing.T, s *Supervisor) {
	t.Helper()
	select {
	case <-s.Done():
		t.Fatal("supervisor shut down unexpectedly")
	default:
	}
}

func requireBrokerClosed(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var admission ports.BrokerAdmissionError
	require.ErrorAs(t, err, &admission)
	require.Equal(t, ports.BrokerAdmissionClosed, admission)
}

func requireBrokerInvalid(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var admission ports.BrokerAdmissionError
	require.ErrorAs(t, err, &admission)
	require.Equal(t, ports.BrokerAdmissionInvalid, admission)
}

type closeFunc func() error

func (f closeFunc) Close() error { return f() }

// blockingRunner stands in for a broker-owned background component such as the
// observation Registry: it runs until cancellation and reports what it saw.
type blockingRunner struct {
	started  chan struct{}
	observed chan error
}

func newBlockingRunner() *blockingRunner {
	return &blockingRunner{started: make(chan struct{}), observed: make(chan error, 1)}
}

func (r *blockingRunner) Run(ctx context.Context) {
	close(r.started)
	<-ctx.Done()
	r.observed <- ctx.Err()
}

func TestSupervisorInitialIdleExpires(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	_, _, armed, closed := supervisorState(s)
	require.True(t, armed, "the idle timer must be armed at construction")
	require.False(t, closed)

	clock.Advance(time.Minute - time.Nanosecond)
	requireSupervisorOpen(t, s)

	clock.Advance(time.Nanosecond)
	await(t, s.Done())
	require.ErrorIs(t, s.RootContext().Err(), context.Canceled)
	require.False(t, supervisorTimerArmed(s))
	require.Equal(t, 0, clock.activeTimers())

	_, err := s.AdmitClient()
	requireBrokerClosed(t, err)
	_, err = s.AdmitOperation()
	requireBrokerClosed(t, err)
	requireBrokerClosed(t, s.Register("late", func(context.Context) error { return nil }))
	requireBrokerClosed(t, s.RegisterRunner("late-runner", newBlockingRunner()))
	requireBrokerClosed(t, s.RegisterCloseable("late-close", closeFunc(func() error { return nil })))
}

func TestSupervisorAdmissionBeforeDeadlineCancelsIdleShutdown(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	clock.Advance(30 * time.Second)

	lease, err := s.AdmitClient()
	require.NoError(t, err)
	require.False(t, supervisorTimerArmed(s))

	// Past the original deadline the broker is still open because the admission
	// cancelled the pending idle shutdown.
	clock.Advance(time.Hour)
	requireSupervisorOpen(t, s)

	lease.Release()
	lease.Release() // idempotent
	require.True(t, supervisorTimerArmed(s))

	clock.Advance(time.Minute - time.Nanosecond)
	requireSupervisorOpen(t, s)
	clock.Advance(time.Nanosecond)
	await(t, s.Done())
}

func TestSupervisorClientLeaseProtectsIdle(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	lease, err := s.AdmitClient()
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		clock.Advance(time.Minute)
		requireSupervisorOpen(t, s)
		require.False(t, supervisorTimerArmed(s), "the timer must be inactive while a client lease is held")
	}

	lease.Release()
	require.True(t, supervisorTimerArmed(s))
	clock.Advance(time.Minute)
	await(t, s.Done())
}

func TestSupervisorOperationLeaseProtectsIdle(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	lease, err := s.AdmitOperation()
	require.NoError(t, err)

	clock.Advance(10 * time.Minute)
	requireSupervisorOpen(t, s)

	lease.Release()
	clock.Advance(time.Minute)
	await(t, s.Done())
}

func TestSupervisorIdleTimerTracksBothLeaseKinds(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	require.True(t, supervisorTimerArmed(s))

	client, err := s.AdmitClient()
	require.NoError(t, err)
	require.False(t, supervisorTimerArmed(s))

	operation, err := s.AdmitOperation()
	require.NoError(t, err)
	require.False(t, supervisorTimerArmed(s))

	// Releasing one kind leaves the other; the timer stays inactive until both
	// counts are zero.
	client.Release()
	clients, operations, armed, closed := supervisorState(s)
	require.Equal(t, 0, clients)
	require.Equal(t, 1, operations)
	require.False(t, armed)
	require.False(t, closed)

	operation.Release()
	require.True(t, supervisorTimerArmed(s))

	clock.Advance(time.Minute)
	await(t, s.Done())
}

// TestSupervisorSupersededArmDoesNotCommit pins the guarantee behind the
// concurrent race test: a fire from an arm that an admission/release pair has
// superseded must be ignored so the broker still gets a full fresh grace.
func TestSupervisorSupersededArmDoesNotCommit(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)

	s.mu.Lock()
	stale := s.arm
	s.mu.Unlock()

	lease, err := s.AdmitClient()
	require.NoError(t, err)
	lease.Release()
	require.True(t, supervisorTimerArmed(s))
	require.False(t, s.tryCommitIdleExpiry(stale), "a fire from a superseded arm must not commit")
	requireSupervisorOpen(t, s)

	clock.Advance(time.Minute - time.Nanosecond)
	requireSupervisorOpen(t, s)
	clock.Advance(time.Nanosecond)
	await(t, s.Done())
}

// TestSupervisorCurrentArmCommits pins the positive half: a fire from the arm the
// run goroutine is still observing commits while the broker is idle.
func TestSupervisorCurrentArmCommits(t *testing.T) {
	s, _ := newTestSupervisor(t, time.Minute)
	s.mu.Lock()
	current := s.arm
	s.mu.Unlock()
	require.True(t, s.tryCommitIdleExpiry(current))
	await(t, s.Done())
}

func TestSupervisorRepeatedLeaseCyclesStillExpire(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	for i := 0; i < 50; i++ {
		lease, err := s.AdmitClient()
		require.NoError(t, err)
		lease.Release()
	}

	require.True(t, supervisorTimerArmed(s))
	require.Equal(t, 1, clock.activeTimers(), "idle cycles must not accumulate timers")
	clock.Advance(time.Minute)
	await(t, s.Done())
	require.Equal(t, 0, clock.activeTimers())
}

// TestSupervisorConcurrentExpiryAndAdmission races the idle expiry against a
// burst of admissions. Because commitment and admission share the mutex, the
// outcome is all-or-nothing: either the expiry committed first and every
// admission is rejected with the typed broker-closed error, or an admission won
// first and every admission is accepted and the broker stays open while a lease
// is held.
func TestSupervisorConcurrentExpiryAndAdmission(t *testing.T) {
	const (
		admitters = 8
		rounds    = 50
	)
	for round := 0; round < rounds; round++ {
		s, clock := newTestSupervisor(t, time.Minute)

		var mu sync.Mutex
		var accepted []*Lease
		var rejected []error
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < admitters; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				lease, err := s.AdmitClient()
				mu.Lock()
				if err != nil {
					rejected = append(rejected, err)
				} else {
					accepted = append(accepted, lease)
				}
				mu.Unlock()
			}()
		}

		clock.Advance(time.Minute) // fire the idle timer concurrently with admissions
		close(start)
		wg.Wait()

		if len(accepted) > 0 {
			require.Empty(t, rejected, "an accepted admission must prevent every later rejection")
			requireSupervisorOpen(t, s)
		} else {
			require.Len(t, rejected, admitters)
			for _, err := range rejected {
				requireBrokerClosed(t, err)
			}
			await(t, s.Done())
		}

		for _, lease := range accepted {
			lease.Release()
		}
		if len(accepted) > 0 {
			clock.Advance(time.Minute)
			await(t, s.Done())
		}
		require.Equal(t, 0, clock.activeTimers())
		require.NoError(t, s.Close())
	}
}

func TestSupervisorBlockedRunnerObservesCancellation(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	runner := newBlockingRunner()
	require.NoError(t, s.RegisterRunner("registry", runner))
	<-runner.started

	clock.Advance(time.Minute)
	select {
	case observed := <-runner.observed:
		require.Equal(t, context.Canceled, observed)
	case <-time.After(3 * time.Second):
		t.Fatal("runner never observed cancellation")
	}
	await(t, s.Done())
	require.ErrorIs(t, s.RootContext().Err(), context.Canceled)
}

func TestSupervisorBlockedHookObservesCancellation(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	observed := make(chan error, 1)
	require.NoError(t, s.Register("blocked", func(ctx context.Context) error {
		<-ctx.Done()
		observed <- ctx.Err()
		return nil
	}))

	clock.Advance(time.Minute)
	select {
	case got := <-observed:
		require.Equal(t, context.Canceled, got)
	case <-time.After(3 * time.Second):
		t.Fatal("hook never observed cancellation")
	}
	await(t, s.Done())
}

func TestSupervisorBackgroundComponentsDoNotPinIdle(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	runner := newBlockingRunner()
	poolDrained := make(chan struct{})
	require.NoError(t, s.RegisterRunner("registry", runner))
	require.NoError(t, s.RegisterCloseable("pool", closeFunc(func() error {
		close(poolDrained)
		return nil
	})))
	<-runner.started

	// Registering background components takes no lease: the timer stays armed
	// and the broker still expires at the plain deadline.
	require.True(t, supervisorTimerArmed(s))
	clock.Advance(time.Minute)
	await(t, s.Done())
	await(t, poolDrained)
	require.Equal(t, context.Canceled, <-runner.observed)
}

func TestSupervisorDrainsHooksInReverseRegistrationOrder(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	var mu sync.Mutex
	var order []string
	for _, name := range []string{"pool", "registry", "store"} {
		name := name
		require.NoError(t, s.Register(name, func(context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}))
	}

	clock.Advance(time.Minute)
	await(t, s.Done())

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"store", "registry", "pool"}, order)
}

func TestSupervisorCloseIsIdempotentAndForceCloses(t *testing.T) {
	s, _ := newTestSupervisor(t, time.Hour)
	lease, err := s.AdmitClient()
	require.NoError(t, err)

	const closers = 8
	errs := make(chan error, closers)
	var wg sync.WaitGroup
	for i := 0; i < closers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Close()
		}()
	}
	wg.Wait()
	for i := 0; i < closers; i++ {
		require.NoError(t, <-errs)
	}

	await(t, s.Done())
	require.ErrorIs(t, s.RootContext().Err(), context.Canceled)
	require.NoError(t, s.Close())

	// A held lease that drains after the forced close must neither re-arm the
	// timer nor change the terminal state.
	lease.Release()
	lease.Release()
	clients, operations, armed, closed := supervisorState(s)
	require.Equal(t, 0, clients)
	require.Equal(t, 0, operations)
	require.False(t, armed)
	require.True(t, closed)
}

func TestSupervisorCloseReportsHookErrorsOnly(t *testing.T) {
	s, _ := newTestSupervisor(t, time.Hour)
	boom := errors.New("pool close failed")
	require.NoError(t, s.RegisterCloseable("pool", closeFunc(func() error { return boom })))
	require.NoError(t, s.Register("ok", func(context.Context) error { return nil }))

	err := s.Close()
	require.Error(t, err)
	require.ErrorIs(t, err, boom)
	// Repeated Close observes the exact same terminal error.
	require.ErrorIs(t, s.Close(), boom)
}

func TestSupervisorRegistrationValidation(t *testing.T) {
	s, _ := newTestSupervisor(t, time.Hour)
	requireBrokerInvalid(t, s.Register("", func(context.Context) error { return nil }))
	requireBrokerInvalid(t, s.Register("nil-hook", nil))
	requireBrokerInvalid(t, s.RegisterRunner("nil-runner", nil))
	requireBrokerInvalid(t, s.RegisterCloseable("nil-closeable", nil))
	require.NoError(t, s.Register("hook", func(context.Context) error { return nil }))
}

// TestSupervisorIdleExpiryLeavesNoTimer pins that an idle expiry shuts the
// supervisor down without leaving an armed timer behind.
func TestSupervisorIdleExpiryLeavesNoTimer(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)

	clock.Advance(time.Minute)
	await(t, s.Done())
	await(t, s.stopped)

	require.Equal(t, 0, clock.activeTimers())
}

// TestSupervisorCloseLeavesNoTimer pins that Close disarms the idle timer and
// still reports shutdown through Done and stopped.
func TestSupervisorCloseLeavesNoTimer(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)

	require.NoError(t, s.Close())
	await(t, s.Done())
	await(t, s.stopped)

	require.Equal(t, 0, clock.activeTimers())
}

// TestSupervisorCloseRacingIdleExpiryDrainsOnce pins the atomic commit: whether
// Close or an idle fire wins, the drain hook runs exactly once and every caller
// observes the same terminal error.
func TestSupervisorCloseRacingIdleExpiryDrainsOnce(t *testing.T) {
	boom := errors.New("pool drain failed")
	for range 25 {
		s, clock := newTestSupervisor(t, time.Minute)

		var mu sync.Mutex
		drains := 0
		require.NoError(t, s.RegisterCloseable("pool", closeFunc(func() error {
			mu.Lock()
			drains++
			mu.Unlock()
			return boom
		})))

		start := make(chan struct{})
		closed := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			clock.Advance(time.Minute)
		}()
		go func() {
			defer wg.Done()
			<-start
			closed <- s.Close()
		}()
		close(start)
		wg.Wait()
		await(t, s.Done())
		await(t, s.stopped)

		mu.Lock()
		got := drains
		mu.Unlock()
		require.Equal(t, 1, got, "the drain hook must run exactly once")
		require.ErrorIs(t, <-closed, boom)
		require.ErrorIs(t, s.Close(), boom, "every Close caller observes the same terminal error")
		require.Equal(t, 0, clock.activeTimers())
	}
}

// TestSupervisorFailingHookDoesNotAbortRemainingDrains pins that a hook error is
// collected rather than fatal: the remaining hooks still drain in reverse
// registration order and the joined error surfaces through Close.
func TestSupervisorFailingHookDoesNotAbortRemainingDrains(t *testing.T) {
	s, clock := newTestSupervisor(t, time.Minute)
	boom := errors.New("registry drain failed")
	var mu sync.Mutex
	var order []string
	register := func(name string, err error) {
		t.Helper()
		require.NoError(t, s.Register(name, func(context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return err
		}))
	}
	register("pool", nil)
	register("registry", boom)
	register("store", nil)

	clock.Advance(time.Minute)
	await(t, s.Done())
	await(t, s.stopped)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"store", "registry", "pool"}, order, "a failing hook must not skip later drains")
	require.ErrorIs(t, s.Close(), boom)
}
