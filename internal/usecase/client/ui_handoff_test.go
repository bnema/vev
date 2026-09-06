package client

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestUIHandoffRequiresDestinationFullAndCompleteDispatch(t *testing.T) {
	for _, dispatchFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "full first", true: "dispatch first"}[dispatchFirst], func(t *testing.T) {
			u, terminal, _, ctx := newUITestService(t)
			snapshot, err := terminal.Snapshot()
			require.NoError(t, err)
			u.pending = 1
			u.reservedContext = snapshot.Context
			require.True(t, u.accept(1, 1))
			u.dispatched[1] = dispatchFirst
			require.True(t, u.follow(1, 1))
			u.receipt(1, protocol.UIReceipt{ActionID: 1, Epoch: 1, State: 1, ViewPublication: 1, Outcome: protocol.UIReceiptProcessed})
			require.Equal(t, ports.UIActionPending, u.records[1].Status, "source receipt cannot complete a correlated handoff")
			generation := u.bindForeground(ctx, u.input, u.consumer)
			require.Equal(t, uint64(2), generation)
			view := snapshot.Context
			view.Generation = generation
			view.Route.Target = protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "destination"}
			view.OutputEpoch = 2
			require.NoError(t, terminal.PublishContext(view))
			u.published(generation)
			require.Equal(t, ports.UIActionPending, u.records[1].Status, "route identity or metadata is not destination full output")
			u.destinationFull(generation)
			if !dispatchFirst {
				require.Equal(t, ports.UIActionPending, u.records[1].Status)
				u.dispatched[1] = true
				u.completeHandoffLocked()
			}
			require.Equal(t, ports.UIActionProcessed, u.records[1].Status)
			require.Equal(t, generation, u.records[1].Context.Generation)
			require.Equal(t, "destination", u.records[1].Context.Route.Target.SessionName)
		})
	}
}

func TestUILocalCompletionUsesAppliedCallbackBoundary(t *testing.T) {
	u, terminal, _, _ := newUITestService(t)
	snapshot, err := terminal.Snapshot()
	require.NoError(t, err)
	u.pending = 1
	u.reservedContext = snapshot.Context
	require.True(t, u.accept(1, 1))
	u.completeLocal(2, 1)
	require.Equal(t, ports.UIActionPending, u.records[1].Status)
	u.completeLocal(1, 1)
	require.Equal(t, ports.UIActionProcessed, u.records[1].Status)
	require.Equal(t, snapshot.Revision, u.records[1].Revision)
}

func TestUILateHandoffAndUnrelatedReplacement(t *testing.T) {
	u, terminal, _, ctx := newUITestService(t)
	snapshot, err := terminal.Snapshot()
	require.NoError(t, err)
	u.pending = 1
	u.reservedContext = snapshot.Context
	require.True(t, u.accept(1, 1))
	u.receipt(1, protocol.UIReceipt{ActionID: 1, Epoch: 1, State: 1, ViewPublication: 1, Outcome: protocol.UIReceiptProcessed})
	require.Equal(t, ports.UIActionProcessed, u.records[1].Status)
	require.True(t, u.follow(1, 1), "a deferred cause retains its original action")
	u.failHandoff()
	require.Equal(t, ports.UIActionNavigationFailed, u.records[1].Status)
	u.pending = 2
	u.reservedContext = snapshot.Context
	require.True(t, u.accept(2, 1))
	require.False(t, u.follow(1, 0))
	u.bindForeground(ctx, u.input, u.consumer)
	require.Equal(t, ports.UIActionOutcomeUnknown, u.records[2].Status)
}

func TestSamePeerGateDiscardsOnlyRetiredAutomatedInput(t *testing.T) {
	u, terminal, _, _ := newUITestService(t)
	snapshot, err := terminal.Snapshot()
	require.NoError(t, err)
	u.pending = 1
	u.reservedContext = snapshot.Context
	require.True(t, u.accept(1, 1))
	require.True(t, u.follow(1, 1))
	gate := newSamePeerInputGate()
	gate.ui = u
	gate.retiredUI.Store(1)
	require.True(t, gate.discardRetiredUI(protocol.UIFence{ActionID: 1}))
	require.Equal(t, ports.UIActionPending, u.records[1].Status, "a superseded source fence is not lost input")
	require.False(t, gate.discardRetiredUI(protocol.Input{Data: []byte("human")}))
	require.False(t, gate.discardRetiredUI(protocol.Input{ActionID: 2, Data: []byte("new generation")}))
	require.True(t, gate.discardRetiredUI(protocol.Input{ActionID: 1, Data: []byte("unsent suffix")}))
	require.Equal(t, ports.UIActionOutcomeUnknown, u.records[1].Status)
	require.Nil(t, u.handoff)
	require.False(t, gate.discardRetiredUI(protocol.Ack{Epoch: 1, State: 1}))
}
