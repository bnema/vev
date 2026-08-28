package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func awaitMoveTeardownState(t *testing.T, sess *session, reservations, waiters uint) {
	t.Helper()
	for {
		sess.teardownMu.Lock()
		gotReservations := sess.moveReservations
		gotWaiters := sess.teardownWaiters
		changed := sess.teardownChangeLocked()
		sess.teardownMu.Unlock()
		if gotReservations == reservations && gotWaiters == waiters {
			return
		}
		select {
		case <-changed:
		case <-time.After(testWaitTimeout):
			t.Fatalf("move reservations/waiters = %d/%d, want %d/%d", gotReservations, gotWaiters, reservations, waiters)
		}
	}
}

func awaitDaemonMoveActive(t *testing.T, d *Daemon, want uint) {
	t.Helper()
	for {
		d.moveLifecycleMu.Lock()
		active := d.moveLifecycleActive
		changed := d.moveLifecycleChangeLocked()
		d.moveLifecycleMu.Unlock()
		if active == want {
			return
		}
		select {
		case <-changed:
		case <-time.After(testWaitTimeout):
			t.Fatalf("active move lifecycles = %d, want %d", active, want)
		}
	}
}

func awaitDaemonMoveClosing(t *testing.T, d *Daemon) {
	t.Helper()
	for {
		d.moveLifecycleMu.Lock()
		closing := d.moveLifecycleClosing
		changed := d.moveLifecycleChangeLocked()
		d.moveLifecycleMu.Unlock()
		if closing {
			return
		}
		select {
		case <-changed:
		case <-time.After(testWaitTimeout):
			t.Fatal("daemon move lifecycle gate did not begin closing")
		}
	}
}

func TestMoveLifecycleReservationMakesTeardownWait(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := &session{sessionCore: sessionCore{id: "source"}}

	reservation, err := d.reserveMoveLifecycles(sess, sess)
	require.NoError(t, err)
	awaitMoveTeardownState(t, sess, 1, 0)

	teardown := make(chan bool, 1)
	go func() { teardown <- sess.beginTeardown(nil) }()
	awaitMoveTeardownState(t, sess, 1, 1)
	select {
	case <-teardown:
		t.Fatal("teardown claimed ownership while the move lifecycle was reserved")
	default:
	}

	reservation.Release()
	require.True(t, awaitTestValue(t, teardown, "teardown did not wake after move release"))
	sess.finishTeardown()
}

func TestMoveLifecycleReservationsUseStableSessionOrder(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	first := &session{sessionCore: sessionCore{id: "a"}}
	second := &session{sessionCore:

	// Both opposite-direction reservations must block on a before either takes
	// b. This proves order from immutable IDs rather than argument direction.
	sessionCore{id: "b"}}

	first.teardownMu.Lock()
	beforeTeardownLocks := make(chan struct{}, 2)
	d.afterMoveLifecycleGateBeforeTeardownLocks = func() { beforeTeardownLocks <- struct{}{} }
	defer func() { d.afterMoveLifecycleGateBeforeTeardownLocks = nil }()
	results := make(chan *moveLifecycleReservation, 2)
	errs := make(chan error, 2)
	for _, pair := range [][2]*session{{first, second}, {second, first}} {
		go func() {
			reservation, err := d.reserveMoveLifecycles(pair[0], pair[1])
			results <- reservation
			errs <- err
		}()
	}
	awaitDaemonMoveActive(t, d, 2)
	for range 2 {
		awaitTestCompletion(t, beforeTeardownLocks, "move did not reach ordered teardown-lock admission")
	}
	require.True(t, second.teardownMu.TryLock(), "opposite move locked b before the shared lowest-ID session")
	second.teardownMu.Unlock()
	first.teardownMu.Unlock()

	for range 2 {
		require.NoError(t, awaitTestValue(t, errs, "opposite move reservation did not finish"))
		reservation := awaitTestValue(t, results, "opposite move reservation token missing")
		require.NotNil(t, reservation)
		reservation.Release()
	}
	awaitDaemonMoveActive(t, d, 0)
}

func TestMoveLifecycleReservationRejectsTeardownWithoutLeakingCounts(t *testing.T) {
	for _, teardownSession := range []string{"source", "destination"} {
		t.Run(teardownSession, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			source := &session{sessionCore: sessionCore{id: "source"}}
			destination := &session{sessionCore: sessionCore{id: "destination"}}
			tearingDown := source
			if teardownSession == "destination" {
				tearingDown = destination
			}
			require.True(t, tearingDown.beginTeardown(nil))

			reservation, err := d.reserveMoveLifecycles(source, destination)
			require.ErrorIs(t, err, errMoveLifecycleUnavailable)
			require.Nil(t, reservation)
			awaitMoveTeardownState(t, source, 0, 0)
			awaitMoveTeardownState(t, destination, 0, 0)
			awaitDaemonMoveActive(t, d, 0)
			tearingDown.finishTeardown()
		})
	}
}

func TestMoveLifecycleReservationDeduplicatesAndReleasesExactlyOnce(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := &session{sessionCore: sessionCore{id: "same"}}
	reservation, err := d.reserveMoveLifecycles(sess, sess)
	require.NoError(t, err)
	awaitMoveTeardownState(t, sess, 1, 0)

	const releasers = 8
	released := make(chan struct{}, releasers)
	for range releasers {
		go func() {
			reservation.Release()
			released <- struct{}{}
		}()
	}
	for range releasers {
		awaitTestCompletion(t, released, "concurrent reservation release did not return")
	}
	awaitMoveTeardownState(t, sess, 0, 0)
	awaitDaemonMoveActive(t, d, 0)
}

func TestMoveLifecycleReservationWaitCanBeCancelledWithoutChangingCounts(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := &session{sessionCore: sessionCore{id: "source"}}
	reservation, err := d.reserveMoveLifecycles(sess, sess)
	require.NoError(t, err)
	deadline := &snapshotShutdownDeadline{done: make(chan struct{})}

	teardown := make(chan bool, 1)
	go func() { teardown <- sess.beginTeardown(deadline) }()
	awaitMoveTeardownState(t, sess, 1, 1)
	close(deadline.done)
	require.False(t, awaitTestValue(t, teardown, "cancelled teardown waiter did not return"))
	awaitMoveTeardownState(t, sess, 1, 0)
	awaitDaemonMoveActive(t, d, 1)

	reservation.Release()
	awaitMoveTeardownState(t, sess, 0, 0)
	awaitDaemonMoveActive(t, d, 0)
}

func TestMoveLifecycleReservationWakesAllTeardownWaiters(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := &session{sessionCore: sessionCore{id: "source"}}
	reservation, err := d.reserveMoveLifecycles(sess, sess)
	require.NoError(t, err)

	claimed := make(chan struct{}, 2)
	finish := make(chan struct{}, 2)
	done := make(chan struct{}, 2)
	for range 2 {
		go func() {
			if sess.beginTeardown(nil) {
				claimed <- struct{}{}
				<-finish
				sess.finishTeardown()
			}
			done <- struct{}{}
		}()
	}
	awaitMoveTeardownState(t, sess, 1, 2)
	reservation.Release()
	for range 2 {
		awaitTestCompletion(t, claimed, "teardown waiter did not claim ownership")
		finish <- struct{}{}
	}
	for range 2 {
		awaitTestCompletion(t, done, "teardown waiter did not complete")
	}
	awaitMoveTeardownState(t, sess, 0, 0)
}

func TestDaemonShutdownDrainsMoveBeforePaneLifetimeCancellation(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	source := &session{sessionCore: sessionCore{id: "source"}}
	destination := &session{sessionCore: sessionCore{id: "destination"}}
	reservation, err := d.reserveMoveLifecycles(source, destination)
	require.NoError(t, err)
	paneLifetime := d.paneProcessCtx
	require.NotNil(t, paneLifetime)

	shutdown := make(chan bool, 1)
	go func() { shutdown <- d.shutdownAll(ports.ReasonServerShutdown) }()
	awaitDaemonMoveClosing(t, d)
	select {
	case <-paneLifetime.Done():
		t.Fatal("shutdown cancelled transferable pane lifetime before the move released")
	default:
	}
	select {
	case <-shutdown:
		t.Fatal("shutdown completed while a move lifecycle remained reserved")
	default:
	}

	reservation.Release()
	awaitTestCompletion(t, paneLifetime.Done(), "shutdown did not cancel pane lifetime after move drain")
	require.False(t, awaitTestValue(t, shutdown, "shutdown did not continue after move drain"))

	reservation, err = d.reserveMoveLifecycles(source, destination)
	require.ErrorIs(t, err, errMoveLifecycleUnavailable)
	require.Nil(t, reservation)
	awaitDaemonMoveActive(t, d, 0)
}
