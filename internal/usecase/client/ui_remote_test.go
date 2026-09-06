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

type uiTestPeer struct {
	connection *portsmocks.MockClientConnection
	incoming   chan protocol.ServerMessage
	sent       chan protocol.ClientMessage
}

func newUITestPeer(t *testing.T, capabilities protocol.ConnectionCapabilities) *uiTestPeer {
	t.Helper()
	p := &uiTestPeer{connection: portsmocks.NewMockClientConnection(t), incoming: make(chan protocol.ServerMessage, 16), sent: make(chan protocol.ClientMessage, 256)}
	closed := make(chan struct{})
	var once sync.Once
	p.connection.EXPECT().Close().RunAndReturn(func() error { once.Do(func() { close(closed) }); return nil }).Maybe()
	t.Cleanup(func() { _ = p.connection.Close() })
	p.connection.EXPECT().Capabilities().Return(capabilities).Maybe()
	p.connection.EXPECT().LinkState().Return(ports.LinkStateConnected).Maybe()
	p.connection.EXPECT().LinkEvents().Return(nil).Maybe()
	p.connection.EXPECT().ReceiveServer().RunAndReturn(func() (protocol.ServerMessage, error) {
		select {
		case message := <-p.incoming:
			return message, nil
		case <-closed:
			return nil, io.EOF
		}
	}).Maybe()
	p.connection.EXPECT().SendClient(mock.Anything).RunAndReturn(func(message protocol.ClientMessage) error {
		select {
		case p.sent <- message:
			return nil
		case <-closed:
			return io.EOF
		}
	}).Maybe()
	return p
}

func awaitUIClientMessage[T protocol.ClientMessage](t *testing.T, p *uiTestPeer) T {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case message := <-p.sent:
			if typed, ok := message.(T); ok {
				return typed
			}
		case <-deadline:
			t.Fatal("missing client message")
			var zero T
			return zero
		}
	}
}

func TestUIRunnerRemoteAndParkedHandoffsRequireDestinationFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 8, Rows: 2}}, "")
	require.NoError(t, err)
	t.Cleanup(terminal.Close)
	ui := NewUI(terminal, systemClock{})
	source := newUITestPeer(t, protocol.ConnectionCapabilities{})
	destination := newUITestPeer(t, protocol.ConnectionCapabilities{PreferredOutputWindow: 1})
	picker := newUITestPeer(t, protocol.ConnectionCapabilities{})
	localDialer := portsmocks.NewMockClientDialer(t)
	localDialer.EXPECT().Dial(mock.Anything).Return(source.connection, nil).Once()
	localDialer.EXPECT().Dial(mock.Anything).Return(picker.connection, nil).Once()
	remoteDialer := portsmocks.NewMockClientDialer(t)
	remoteDialer.EXPECT().Dial(mock.Anything).Return(destination.connection, nil).Once()
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "remote"}
	runner := NewRunner(Dependencies{Dialer: localDialer, Terminal: terminal, Clock: systemClock{}, UI: ui, DisableCapabilityProbe: true, AttachHandoff: func(offer protocol.AttachTarget) (ports.ClientDialer, AttachRequest, error) {
		require.Equal(t, "isolated-fixture", offer.Endpoint)
		return remoteDialer, AttachRequest{Intent: protocol.IntentAttach, SessionName: "remote", Remote: true, Origin: protocol.RouteOriginRemote, OriginKey: "isolated-fixture", ExactTarget: &target}, nil
	}})
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx, AttachRequest{Intent: protocol.IntentAttach, SessionName: "local"}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Error("Runner did not stop")
		}
	})
	view := protocol.ViewContext{Publication: 1, Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "local"}}, TabID: "tab", FocusedPaneID: "pane"}
	source.incoming <- protocol.Welcome{SessionID: "local-session", SessionName: "local", CommittedIdentity: &view.Route}
	awaitUIClientMessage[protocol.Theme](t, source)
	awaitUIClientMessage[protocol.Theme](t, source)
	source.incoming <- protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 8, Rows: 2}, Context: &view, Data: []byte("\x1b[Hlocal")}
	attached := ports.UIStatusAttached
	_, err = ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached}})
	require.NoError(t, err)
	results := make(chan ports.UIActionResult, 1)
	failures := make(chan error, 1)
	go func() {
		result, err := ui.Action(ctx, ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Keys: []string{"Enter"}})
		if err != nil {
			failures <- err
			return
		}
		results <- result
	}()
	fence := awaitUIClientMessage[protocol.UIFence](t, source)
	source.incoming <- protocol.AttachTarget{Endpoint: "isolated-fixture", Session: "remote", ExactTarget: &target, CauseActionID: fence.ActionID}
	awaitUIClientMessage[protocol.Hello](t, destination)
	remoteView := view
	remoteView.Route.Target = target
	destination.incoming <- protocol.Welcome{SessionID: "remote-session", SessionName: "remote", CommittedIdentity: &remoteView.Route}
	awaitUIClientMessage[protocol.Theme](t, destination)
	select {
	case <-results:
		t.Fatal("Welcome completed the action before its viewport")
	case err := <-failures:
		t.Fatalf("handoff lost correlation: %v", err)
	default:
	}
	destination.incoming <- protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 8, Rows: 2}, Context: &remoteView, Data: []byte("\x1b[2J\x1b[Hremote")}
	select {
	case result := <-results:
		require.Equal(t, ports.UIActionProcessed, result.Status)
		require.Equal(t, uint64(2), result.Context.Generation)
		require.Equal(t, target, result.Context.Route.Target)
	case err := <-failures:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("destination full did not complete handoff")
	}
	awaitUIClientMessage[protocol.Theme](t, destination)
	startAction := func(generation uint64) {
		go func() {
			result, err := ui.Action(ctx, ports.UIActionRequest{Attachment: ui.Handle(), Generation: generation, Keys: []string{"Enter"}})
			if err != nil {
				failures <- err
				return
			}
			results <- result
		}()
	}
	awaitAction := func(generation uint64, target protocol.ExactSessionTarget) {
		t.Helper()
		select {
		case result := <-results:
			require.Equal(t, ports.UIActionProcessed, result.Status)
			require.Equal(t, generation, result.Context.Generation)
			require.Equal(t, target, result.Context.Route.Target)
		case err := <-failures:
			t.Fatal(err)
		case <-time.After(time.Second):
			t.Fatal("parked handoff did not complete")
		}
	}
	startAction(2)
	openPicker := awaitUIClientMessage[protocol.UIFence](t, destination)
	destination.incoming <- protocol.NavigationDirective{Action: protocol.NavigationOpenHomePicker, LeaseID: protocol.ParkedRouteLeaseID{1}, CauseActionID: openPicker.ActionID}
	prepare := awaitUIClientMessage[protocol.ParkedRouteRequest](t, destination)
	require.Equal(t, protocol.ParkedRoutePrepare, prepare.Action)
	destination.incoming <- protocol.ParkedRouteResponse{RequestID: prepare.RequestID, Status: protocol.ParkedRouteReady}
	awaitUIClientMessage[protocol.Hello](t, picker)
	picker.incoming <- protocol.Welcome{SessionID: "local-session", SessionName: "local", CommittedIdentity: &view.Route}
	awaitUIClientMessage[protocol.Theme](t, picker)
	awaitUIClientMessage[protocol.Theme](t, picker)
	picker.incoming <- protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 8, Rows: 2}, Context: &view, Data: []byte("\x1b[2J\x1b[Hpicker")}
	awaitAction(3, view.Route.Target)
	startAction(3)
	selectRemote := awaitUIClientMessage[protocol.UIFence](t, picker)
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "isolated-fixture", DisplayOrigin: "isolated-fixture", LifecycleID: target.LifecycleID, SessionName: target.SessionName, LiveTabID: "tab"}
	picker.incoming <- protocol.AttachTarget{Endpoint: "isolated-fixture", Session: "remote", RemoteTarget: &remoteTarget, CauseActionID: selectRemote.ActionID}
	switchRequest := awaitUIClientMessage[protocol.ParkedRouteRequest](t, destination)
	require.Equal(t, protocol.ParkedRouteSwitch, switchRequest.Action)
	destination.incoming <- protocol.ParkedRouteResponse{RequestID: switchRequest.RequestID, Status: protocol.ParkedRouteSwitched}
	resumedView := remoteView
	resumedView.Publication = 2
	destination.incoming <- protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 8, Rows: 2}, Context: &resumedView, Data: []byte("\x1b[2J\x1b[Hresumed")}
	awaitAction(4, target)
}
