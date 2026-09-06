package client

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestUIRunnerAppliesFenceReceiptAfterMetadataPublication(t *testing.T) {
	testUIRunnerAction(t, false)
}

func TestUIRunnerSamePeerActionWaitsForDestinationFull(t *testing.T) {
	testUIRunnerAction(t, true)
}

func testUIRunnerAction(t *testing.T, handoff bool) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 8, Rows: 2}}, "")
	require.NoError(t, err)
	t.Cleanup(terminal.Close)
	ui := NewUI(terminal, systemClock{})
	incoming := make(chan protocol.ServerMessage, 16)
	closed := make(chan struct{})
	var once sync.Once
	transport := portsmocks.NewMockClientConnection(t)
	transport.EXPECT().Close().RunAndReturn(func() error { once.Do(func() { close(closed) }); return nil }).Maybe()
	transport.EXPECT().Capabilities().Return(protocol.ConnectionCapabilities{}).Maybe()
	transport.EXPECT().LinkState().Return(ports.LinkStateConnected).Maybe()
	transport.EXPECT().LinkEvents().Return(nil).Maybe()
	transport.EXPECT().ReceiveServer().RunAndReturn(func() (protocol.ServerMessage, error) {
		select {
		case message := <-incoming:
			return message, nil
		case <-closed:
			return nil, io.EOF
		}
	}).Maybe()
	view := protocol.ViewContext{Publication: 1, Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "fixture"}}, TabID: "tab", FocusedPaneID: "pane"}
	var themeCount int
	sentInput := make(chan protocol.Input, 16)
	fences := make(chan protocol.UIFence, 1)
	switches := make(chan protocol.SamePeerSwitchRequest, 1)
	routeSnapshots := make(chan struct{}, 8)
	transport.EXPECT().SendClient(mock.Anything).RunAndReturn(func(message protocol.ClientMessage) error {
		switch message := message.(type) {
		case protocol.Theme:
			themeCount++
			if themeCount == 2 {
				initial := view
				incoming <- protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 8, Rows: 2}, Context: &initial, Data: []byte("\x1b[Hready")}
			}
		case protocol.Input:
			sentInput <- message
		case protocol.UIFence:
			fences <- message
		case protocol.SamePeerSwitchRequest:
			switches <- message
		case protocol.RecentRouteSnapshot:
			routeSnapshots <- struct{}{}
		}
		return nil
	}).Maybe()
	dialer := portsmocks.NewMockClientDialer(t)
	dialer.EXPECT().Dial(mock.Anything).Return(transport, nil).Once()
	runner := NewRunner(Dependencies{Dialer: dialer, Terminal: terminal, Clock: systemClock{}, UI: ui, DisableCapabilityProbe: true})
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx, AttachRequest{Intent: protocol.IntentAttach, SessionName: "fixture"}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Error("Runner did not detach")
		}
	})
	incoming <- protocol.Welcome{SessionID: "session", SessionName: "fixture", CommittedIdentity: &view.Route}
	attached := ports.UIStatusAttached
	_, err = ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached}})
	require.NoError(t, err)
	result := make(chan ports.UIActionResult, 1)
	failed := make(chan error, 1)
	go func() {
		action, err := ui.Action(ctx, ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Keys: []string{"Escape"}})
		if err != nil {
			failed <- err
			return
		}
		result <- action
	}()
	var fence protocol.UIFence
	select {
	case fence = <-fences:
	case err := <-failed:
		t.Fatalf("action failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("missing labeled fence")
	}
	input := <-sentInput
	require.Equal(t, []byte("\x1b"), input.Data)
	require.Equal(t, fence.ActionID, input.ActionID)
	select {
	case <-result:
		t.Fatal("input dispatch alone completed action")
	default:
	}
	view.Publication = 2
	if handoff {
		for len(routeSnapshots) > 0 {
			<-routeSnapshots
		}
		target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "destination"}
		incoming <- protocol.AttachTarget{SamePeer: true, ExactTarget: &target, CauseActionID: fence.ActionID}
		select {
		case request := <-switches:
			require.Equal(t, target, request.Target)
		case <-time.After(time.Second):
			t.Fatal("missing existing same-peer switch request")
		}
		incoming <- protocol.CommittedRouteIdentity{Target: target}
		requireSignal(t, routeSnapshots, "identity commit was not handled")
		select {
		case <-result:
			t.Fatal("identity alone completed the action")
		case err := <-failed:
			t.Fatalf("handoff failed: %v", err)
		default:
		}
		view.Route.Target = target
		incoming <- protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 8, Rows: 2}, Context: &view, Data: []byte("\x1b[2J\x1b[Hready")}
	} else {
		incoming <- protocol.UIViewUpdate{Epoch: 1, State: 1, Context: view}
		incoming <- protocol.UIReceipt{ActionID: fence.ActionID, Epoch: 1, State: 1, ViewPublication: 2, Outcome: protocol.UIReceiptProcessed}
	}
	var action ports.UIActionResult
	select {
	case action = <-result:
	case err := <-failed:
		t.Fatalf("action failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("applied receipt did not complete action")
	}
	require.True(t, action.Accepted)
	require.Equal(t, ports.UIActionProcessed, action.Status)
	require.Equal(t, uint64(2), action.Context.ViewPublication)
	require.Equal(t, ui.Handle(), action.Context.AttachmentHandle)
	if handoff {
		require.Equal(t, uint64(2), action.Context.Generation)
		require.Equal(t, "destination", action.Context.Route.Target.SessionName)
	}
	text := "ready"
	matched, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), AfterAction: action.ActionID, Expect: ports.UIExpect{TextContains: &text, Status: &attached}})
	require.NoError(t, err)
	require.GreaterOrEqual(t, matched.Revision, action.Revision)
}
