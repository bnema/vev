package client

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func newUITestService(t *testing.T) (*UI, *uiterm.Terminal, *attachPaletteClock, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 8, Rows: 2}}, "")
	require.NoError(t, err)
	t.Cleanup(terminal.Close)
	clock := newAttachPaletteClock()
	u := NewUI(terminal, clock)
	input := newTerminalInputPump(nil)
	consumer := input.claim()
	generation := u.bindForeground(ctx, input, consumer)
	view := ports.UIContext{AttachmentHandle: u.Handle(), Generation: generation, Status: ports.UIStatusAttached, OutputEpoch: 1, OutputState: 1, ViewPublication: 1, TabID: "tab", FocusedPaneID: "pane"}
	view.Route.Target = protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "fixture"}
	terminal.BeginOutput(view)
	_, err = terminal.Write([]byte("ready"))
	require.NoError(t, err)
	require.NoError(t, terminal.Flush())
	terminal.EndOutput(true)
	u.published(generation)
	return u, terminal, clock, ctx
}

func TestUIWaitConjunctiveCurrentSnapshot(t *testing.T) {
	u, terminal, _, ctx := newUITestService(t)
	snapshot, err := terminal.Snapshot()
	require.NoError(t, err)
	text := "ready"
	status := ports.UIStatusAttached
	result, err := u.Wait(ctx, ports.UIWaitRequest{Attachment: u.Handle(), Expect: ports.UIExpect{TextContains: &text, Session: &snapshot.Context.Route.Target, Focus: &ports.UIFocus{TabID: "tab", PaneID: "pane"}, Status: &status}})
	require.NoError(t, err)
	require.Equal(t, snapshot.Revision, result.Revision)
	require.Equal(t, "ready   \n        ", uiSnapshotText(snapshot))
	wrong := "wrong"
	require.False(t, uiMatches(snapshot, ports.UIExpect{TextContains: &text, Focus: &ports.UIFocus{TabID: "tab", PaneID: domain.PaneStableID(wrong)}}))
}

func TestUIActionTimeoutRetainsExactReceiptBoundary(t *testing.T) {
	u, terminal, clock, ctx := newUITestService(t)
	errors := make(chan error, 1)
	go func() {
		_, err := u.Action(ctx, ports.UIActionRequest{Attachment: u.Handle(), Generation: 1, Text: "x"})
		errors <- err
	}()
	var batch terminalAutomationRequest
	select {
	case batch = <-u.input.automation:
	case <-time.After(time.Second):
		t.Fatal("action did not reach owner")
	}
	requestTimer := requireTimer(t, clock.timers, "missing request deadline")
	require.True(t, batch.owner.accept(batch.record.actionID, batch.record.generation))
	batch.admitted <- true
	batch.dispatched <- true
	requireTimer(t, clock.timers, "missing retained action expiry")
	requestTimer.fire()
	var actionErr *ports.UIError
	select {
	case err := <-errors:
		require.ErrorAs(t, err, &actionErr)
	case <-time.After(time.Second):
		t.Fatal("request deadline stalled")
	}
	require.Equal(t, "timeout", actionErr.Code)
	require.True(t, actionErr.Accepted)
	require.Equal(t, batch.record.actionID, actionErr.ActionID)
	// A receipt for another publication cannot use an earlier matching screen.
	u.receipt(1, protocol.UIReceipt{ActionID: actionErr.ActionID, Epoch: 1, State: 1, ViewPublication: 2, Outcome: protocol.UIReceiptProcessed})
	u.mu.Lock()
	require.Equal(t, "pending", u.records[actionErr.ActionID].Status)
	u.mu.Unlock()
	snapshot, err := terminal.Snapshot()
	require.NoError(t, err)
	view := snapshot.Context
	view.ViewPublication = 2
	require.NoError(t, terminal.PublishContext(view))
	u.published(1)
	boundary, err := terminal.Snapshot()
	require.NoError(t, err)
	// Later presentation-only publication must not shift the action lower bound.
	view.Status = ports.UIStatusReconnecting
	require.NoError(t, terminal.PublishContext(view))
	u.receipt(1, protocol.UIReceipt{ActionID: actionErr.ActionID, Epoch: 1, State: 1, ViewPublication: 2, Outcome: protocol.UIReceiptProcessed})
	u.mu.Lock()
	record := u.records[actionErr.ActionID]
	u.mu.Unlock()
	require.Equal(t, "processed", record.Status)
	require.Equal(t, boundary.Revision, record.Revision)
	text := "ready"
	result, err := u.Wait(ctx, ports.UIWaitRequest{Attachment: u.Handle(), AfterAction: record.ActionID, Expect: ports.UIExpect{TextContains: &text}})
	require.NoError(t, err)
	require.GreaterOrEqual(t, result.Revision, record.Revision)
}

func TestUIWaitBroadcastAndAttachmentQuota(t *testing.T) {
	u, terminal, clock, ctx := newUITestService(t)
	observeCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); u.Observe(observeCtx) }()
	t.Cleanup(func() { cancel(); requireSignal(t, done, "observation worker did not stop") })
	status := ports.UIStatusReconnecting
	results := make(chan error, 4)
	request := ports.UIWaitRequest{Attachment: u.Handle(), Expect: ports.UIExpect{Status: &status}}
	for range 4 {
		go func() { _, err := u.Wait(ctx, request); results <- err }()
		requireTimer(t, clock.timers, "wait was not admitted")
	}
	_, err := u.Wait(ctx, request)
	var uiErr *ports.UIError
	require.ErrorAs(t, err, &uiErr)
	require.Equal(t, "busy", uiErr.Code)
	snapshot, err := terminal.Snapshot()
	require.NoError(t, err)
	snapshot.Context.Status = status
	require.NoError(t, terminal.PublishContext(snapshot.Context))
	for range 4 {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("coalesced publication did not wake every waiter")
		}
	}
}

func TestUIActionHistoryEvictsOnlyOnAcceptance(t *testing.T) {
	u, _, _, _ := newUITestService(t)
	for id := uint64(1); id <= uiActionHistory; id++ {
		u.pending = id
		u.reservedContext = ports.UIContext{Generation: 1}
		require.True(t, u.accept(id, 1))
	}
	u.pending = 65
	require.False(t, u.accept(65, 2))
	require.Len(t, u.records, uiActionHistory)
	require.Contains(t, u.records, uint64(1), "a refused generation cannot evict a retained result")
	require.True(t, u.accept(65, 1))
	require.Len(t, u.records, uiActionHistory)
	require.NotContains(t, u.records, uint64(1))
}

func TestUIWaitFakeDeadlineAndExpiredAction(t *testing.T) {
	u, _, clock, ctx := newUITestService(t)
	text := "not present"
	errors := make(chan error, 1)
	go func() {
		_, err := u.Wait(ctx, ports.UIWaitRequest{Attachment: u.Handle(), Expect: ports.UIExpect{TextContains: &text}})
		errors <- err
	}()
	requireTimer(t, clock.timers, "missing wait timer").fire()
	var uiErr *ports.UIError
	select {
	case err := <-errors:
		require.ErrorAs(t, err, &uiErr)
	case <-time.After(time.Second):
		t.Fatal("wait deadline stalled")
	}
	require.Equal(t, "timeout", uiErr.Code)
	_, err := u.Wait(ctx, ports.UIWaitRequest{Attachment: u.Handle(), AfterAction: 99, Expect: ports.UIExpect{TextContains: &text}})
	require.ErrorAs(t, err, &uiErr)
	require.Equal(t, "action_expired", uiErr.Code)
}
