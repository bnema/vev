package client

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// pickerLeaseTestState builds an applied output state for barrier checks.
func pickerLeaseTestState(epoch, state uint64) outputApplyState {
	return outputApplyState{epoch: epoch, state: state, initialized: true}
}

// pickerLeaseTestSnapshot builds one validated source publication for the
// lease. The acquisition barrier travels in the offer, so it is passed
// separately to admitSnapshot.
func pickerLeaseTestSnapshot(interaction, revision uint64) protocol.PickerSnapshot {
	return protocol.PickerSnapshot{
		InteractionID: interaction, SourceID: "serving", SourceRevision: revision,
		Status: protocol.PickerSourceOK,
		Lines: []protocol.PickerLine{
			{Key: "aa/first", Kind: protocol.PickerLineSession, Label: "first", Actions: protocol.PickerCanNavigate},
		},
		Cursor: protocol.PickerCursor{Key: "aa/first", Index: 0},
	}
}

func pickerLeaseTestLoop() *pickerLoop {
	return &pickerLoop{interaction: 1, sourceID: "serving", sourceRevision: 1}
}

func TestPickerLeaseAcquiresOnlyAtTheBarrier(t *testing.T) {
	tests := []struct {
		name         string
		applied      outputApplyState
		barrierEpoch uint64
		barrierState uint64
		ready        bool
		owned        bool
	}{
		{name: "uninitialized zero barrier is already applied", applied: outputApplyState{}, barrierEpoch: 1, ready: true, owned: true},
		{name: "applied state equals the barrier", applied: pickerLeaseTestState(4, 3), barrierEpoch: 4, barrierState: 3, ready: true, owned: true},
		{name: "applied state passed the barrier", applied: pickerLeaseTestState(4, 5), barrierEpoch: 4, barrierState: 3, ready: true, owned: true},
		{name: "newer epoch passed the barrier", applied: pickerLeaseTestState(6, 1), barrierEpoch: 4, barrierState: 3, ready: true, owned: true},
		{name: "barrier still ahead", applied: pickerLeaseTestState(4, 1), barrierEpoch: 4, barrierState: 3, ready: false, owned: false},
		{name: "barrier in a future epoch", applied: pickerLeaseTestState(4, 9), barrierEpoch: 5, barrierState: 1, ready: false, owned: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var lease pickerLease
			ready, superseded := lease.admitSnapshot(pickerLeaseTestSnapshot(7, 1), pickerBarrier{epoch: test.barrierEpoch, state: test.barrierState}, pickerLeaseTestLoop(), test.applied, 3)
			require.Nil(t, superseded)
			require.Equal(t, test.ready, ready)
			require.Equal(t, test.owned, lease.state == pickerLeaseOwned)
			require.Equal(t, uint64(7), lease.interaction)
			require.Equal(t, uint64(3), lease.generation)
		})
	}
}

func TestPickerLeaseAcquiresWhenAHeldBarrierArrives(t *testing.T) {
	var lease pickerLease
	ready, _ := lease.admitSnapshot(pickerLeaseTestSnapshot(7, 1), pickerBarrier{epoch: 4, state: 2}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.False(t, ready)
	require.Equal(t, pickerLeaseAcquiring, lease.state)

	// A still-short frame keeps the lease acquiring and displays the frame.
	event := lease.observe(protocol.Output{Epoch: 4, New: 2, Full: true}, pickerLeaseTestState(4, 1))
	require.Equal(t, pickerLeaseIdle, event)
	require.Equal(t, pickerLeaseAcquiring, lease.state)

	// The exact barrier is applied: the frame is displayed first and the
	// picker follows it. The pending model stays for the caller to install.
	event = lease.observe(protocol.Output{Epoch: 4, New: 3, Base: 2}, pickerLeaseTestState(4, 3))
	require.Equal(t, pickerLeaseAcquire, event)
	require.Equal(t, pickerLeaseOwned, lease.state)
	require.NotNil(t, lease.pending)
	require.Equal(t, uint64(1), lease.pending.interaction)
}

func TestPickerLeaseSuppressesOwnedOutputAndPublishesBoundary(t *testing.T) {
	var lease pickerLease
	require.True(t, func() bool {
		ready, _ := lease.admitSnapshot(pickerLeaseTestSnapshot(7, 1), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
		return ready
	}())

	require.Equal(t, pickerLeaseSuppress, lease.observe(protocol.Output{Epoch: 4, New: 2, Base: 1}, pickerLeaseTestState(4, 2)))
	require.Equal(t, pickerLeaseSuppress, lease.observe(protocol.Output{Epoch: 5, New: 1, Full: true}, pickerLeaseTestState(5, 1)))
	require.Equal(t, pickerLeaseOwned, lease.state)
}

func TestPickerLeaseReleasesOnlyAfterTheDaemonCloseAndANewerFullPaint(t *testing.T) {
	var lease pickerLease
	ready, _ := lease.admitSnapshot(pickerLeaseTestSnapshot(7, 4), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 2)
	require.True(t, ready)

	// The client cancels: the client-side retire alone never releases, because
	// it cannot tell a post-close restore from a paint already in flight.
	require.True(t, lease.beginRelease(7, 42, true, false, pickerLeaseTestState(4, 1)))
	require.Equal(t, pickerLeaseReleasing, lease.state)
	require.Equal(t, pickerLeaseSuppress, lease.observe(protocol.Output{Epoch: 4, New: 2, Base: 1}, pickerLeaseTestState(4, 2)))
	require.Equal(t, pickerLeaseSuppress, lease.observe(protocol.Output{Epoch: 5, New: 1, Full: true}, pickerLeaseTestState(5, 1)),
		"a Full in flight before the daemon close must not release")

	// The daemon confirms the close at the applied boundary it was rendering:
	// only paints accepted after that boundary release the terminal.
	require.True(t, lease.confirmDaemonClose(7, pickerLeaseTestState(5, 1)))
	require.False(t, lease.confirmDaemonClose(8, pickerLeaseTestState(5, 1)), "a foreign confirmation must not move the boundary")
	require.Equal(t, pickerLeaseSuppress, lease.observe(protocol.Output{Epoch: 5, New: 1, Full: true}, pickerLeaseTestState(5, 1)),
		"a paint at the close boundary is not newer than it")
	require.Equal(t, pickerLeaseRelease, lease.observe(protocol.Output{Epoch: 5, New: 2, Full: true}, pickerLeaseTestState(5, 2)))

	actionID, local := lease.finishRelease()
	require.Equal(t, uint64(42), actionID)
	require.True(t, local)
	require.False(t, lease.active())
}

func TestPickerLeaseDaemonCloseReleasesWithoutAClientRetire(t *testing.T) {
	var lease pickerLease
	ready, _ := lease.admitSnapshot(pickerLeaseTestSnapshot(7, 1), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.True(t, ready)

	// A daemon-initiated close (a resolved selection) carries its own barrier.
	require.True(t, lease.beginRelease(7, 0, false, true, pickerLeaseTestState(4, 1)))
	require.Equal(t, pickerLeaseRelease, lease.observe(protocol.Output{Epoch: 5, New: 1, Full: true}, pickerLeaseTestState(5, 1)))
}

func TestPickerLeaseForeignAndDuplicateCloseNeverRelease(t *testing.T) {
	var lease pickerLease
	ready, _ := lease.admitSnapshot(pickerLeaseTestSnapshot(7, 1), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.True(t, ready)

	require.False(t, lease.beginRelease(6, 42, true, false, pickerLeaseTestState(4, 1)), "foreign interaction must not release")
	require.False(t, lease.beginRelease(0, 42, true, false, pickerLeaseTestState(4, 1)), "zero interaction must not release")
	require.True(t, lease.beginRelease(7, 42, true, false, pickerLeaseTestState(4, 1)))
	require.False(t, lease.beginRelease(7, 43, true, false, pickerLeaseTestState(4, 1)), "duplicate close must not restart the release")
	require.Equal(t, uint64(42), lease.releaseAction)
}

func TestPickerLeaseSupersedingSnapshotStartsANewNamespace(t *testing.T) {
	var lease pickerLease
	ready, _ := lease.admitSnapshot(pickerLeaseTestSnapshot(7, 1), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.True(t, ready)
	require.True(t, lease.beginRelease(7, 42, true, false, pickerLeaseTestState(4, 1)))

	// A superseding interaction cannot be released by the prior close.
	ready, superseded := lease.admitSnapshot(pickerLeaseTestSnapshot(8, 1), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.True(t, ready)
	require.NotNil(t, superseded)
	require.Equal(t, uint64(7), superseded.interaction)
	require.Equal(t, uint64(8), lease.interaction)
	require.Equal(t, pickerLeaseOwned, lease.state)
	require.False(t, lease.beginRelease(7, 42, true, false, pickerLeaseTestState(4, 1)))
}

func TestPickerLeaseSameInteractionRevisionRules(t *testing.T) {
	var lease pickerLease
	ready, _ := lease.admitSnapshot(pickerLeaseTestSnapshot(7, 3), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.True(t, ready)

	// Duplicate and older revisions never replace the displayed model.
	ready, superseded := lease.admitSnapshot(pickerLeaseTestSnapshot(7, 3), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.False(t, ready)
	require.Nil(t, superseded)
	ready, superseded = lease.admitSnapshot(pickerLeaseTestSnapshot(7, 2), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.False(t, ready)
	require.Nil(t, superseded)

	// A newer revision refreshes in place.
	ready, superseded = lease.admitSnapshot(pickerLeaseTestSnapshot(7, 4), pickerBarrier{epoch: 4, state: 2}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.False(t, ready)
	require.Nil(t, superseded)
	require.Equal(t, uint64(4), lease.revision)

	// A newer revision that also moved the barrier to an applied state
	// acquires immediately.
	ready, _ = lease.admitSnapshot(pickerLeaseTestSnapshot(7, 5), pickerBarrier{epoch: 4, state: 2}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 2), 1)
	require.True(t, ready)

	// A release in progress refuses revisions: the close/full pair cannot be
	// replaced by a late refresh.
	require.True(t, lease.beginRelease(7, 0, false, true, pickerLeaseTestState(4, 2)))
	ready, superseded = lease.admitSnapshot(pickerLeaseTestSnapshot(7, 6), pickerBarrier{epoch: 4, state: 2}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 2), 1)
	require.False(t, ready)
	require.Nil(t, superseded)
}

func TestPickerLeaseAbortReportsPendingLocalAction(t *testing.T) {
	var lease pickerLease
	lease.admitSnapshot(pickerLeaseTestSnapshot(7, 1), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	require.True(t, lease.beginRelease(7, 42, true, false, pickerLeaseTestState(4, 1)))

	actionID, local := lease.abort()
	require.Equal(t, uint64(42), actionID)
	require.True(t, local)
	require.False(t, lease.active())

	// A handoff-bound action stays unresolved: the daemon owns its outcome.
	lease.admitSnapshot(pickerLeaseTestSnapshot(9, 1), pickerBarrier{epoch: 4, state: 1}, pickerLeaseTestLoop(), pickerLeaseTestState(4, 1), 1)
	lease.beginRelease(9, 77, false, false, pickerLeaseTestState(4, 1))
	actionID, local = lease.abort()
	require.Zero(t, actionID)
	require.False(t, local)
}

func TestPickerLeaseRejectsUnusableSnapshots(t *testing.T) {
	var lease pickerLease
	ready, superseded := lease.admitSnapshot(pickerLeaseTestSnapshot(0, 1), pickerBarrier{epoch: 1, state: 0}, pickerLeaseTestLoop(), outputApplyState{}, 1)
	require.False(t, ready)
	require.Nil(t, superseded)
	require.False(t, lease.active())
}

func TestPickerLeaseClosedPresentationNeverSuppresses(t *testing.T) {
	var lease pickerLease
	require.False(t, lease.active())
	require.Equal(t, pickerLeaseIdle, lease.observe(protocol.Output{Epoch: 1, New: 1, Full: true}, pickerLeaseTestState(1, 1)))
	require.Equal(t, pickerLeaseIdle, lease.observe(protocol.Output{Epoch: 1, New: 2, Base: 1}, pickerLeaseTestState(1, 2)))
}
