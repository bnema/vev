package client_test

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
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRunnerPublishesCapturedContextAndMetadataWithoutTerminalBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 6, Rows: 2}}, "public-handle")
	require.NoError(t, err)
	t.Cleanup(terminal.Close)
	transport := portsmocks.NewMockClientConnection(t)
	incoming := make(chan protocol.ServerMessage, 8)
	closed := make(chan struct{})
	var closeOnce sync.Once
	transport.EXPECT().Close().RunAndReturn(func() error { closeOnce.Do(func() { close(closed) }); return nil }).Maybe()
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
	acks := make(chan protocol.Ack, 4)
	resets := make(chan struct{}, 4)
	transport.EXPECT().SendClient(mock.Anything).RunAndReturn(func(message protocol.ClientMessage) error {
		switch message := message.(type) {
		case protocol.Ack:
			acks <- message
		case protocol.OutputResetRequest:
			resets <- struct{}{}
		}
		return nil
	}).Maybe()
	dialer := portsmocks.NewMockClientDialer(t)
	dialer.EXPECT().Dial(mock.Anything).Return(transport, nil).Once()
	runner := client.NewRunner(client.Dependencies{Dialer: dialer, Terminal: terminal, Clock: realClock{}, DisableCapabilityProbe: true})
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx, client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "work"})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Error("Runner did not release its owned attachment")
		}
	})
	view := protocol.ViewContext{
		Publication: 1,
		Route:       protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}},
		TabID:       "tab-1", FocusedPaneID: "pane-1",
	}
	incoming <- protocol.Welcome{SessionID: "session", SessionName: "work"}
	initialView := view
	incoming <- protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 6, Rows: 2}, Context: &initialView, Data: []byte("\x1b[Hhello")}
	initial := awaitUIPublication(t, terminal, func(snapshot ports.UISnapshot) bool { return snapshot.Context.ViewPublication == 1 })
	require.Equal(t, "public-handle", initial.Context.AttachmentHandle)
	require.Equal(t, view.Route, initial.Context.Route)
	require.Equal(t, "hello", uiFirstCells(initial, 5))
	select {
	case ack := <-acks:
		require.Equal(t, protocol.Ack{Epoch: 1, State: 1}, ack)
	case <-time.After(time.Second):
		t.Fatal("initial applied output was not acknowledged")
	}

	// A typed connection must not bypass semantic validation. The following
	// valid metadata is an ordered witness that the rejected frame was handled.
	incoming <- protocol.Output{Epoch: 1, Base: 1, New: 2, Size: domain.Size{Cols: 6, Rows: 2}, Data: []byte("bad")}
	select {
	case <-resets:
	case <-time.After(time.Second):
		t.Fatal("missing context did not request an authoritative reset")
	}
	view.Publication, view.FocusedPaneID = 2, "pane-2"
	incoming <- protocol.UIViewUpdate{Epoch: 1, State: 1, Context: view}
	focused := awaitUIPublication(t, terminal, func(snapshot ports.UISnapshot) bool { return snapshot.Context.ViewPublication == 2 })
	require.Equal(t, initial.Cells, focused.Cells)
	require.Equal(t, initial.Cursor, focused.Cursor)
	require.Equal(t, domain.PaneStableID("pane-2"), focused.Context.FocusedPaneID)
	require.Equal(t, initial.Context.OutputState, focused.Context.OutputState)
	require.Greater(t, focused.Revision, initial.Revision)
	require.Equal(t, "public-handle", focused.Context.AttachmentHandle)

	incoming <- protocol.UIViewUpdate{Epoch: 1, State: 1, Context: view} // duplicate
	incoming <- protocol.UIViewUpdate{Epoch: 9, State: 7, Context: protocol.ViewContext{Publication: 3, Route: view.Route, TabID: "tab-1", FocusedPaneID: "future"}}
	view.Publication = 3
	incoming <- protocol.UIViewUpdate{Epoch: 1, State: 1, Context: view}
	latest := awaitUIPublication(t, terminal, func(snapshot ports.UISnapshot) bool { return snapshot.Context.ViewPublication == 3 })
	require.Equal(t, initial.Cells, latest.Cells)
	require.Empty(t, acks, "metadata and rejected output must not create ACK obligations")
	require.Empty(t, resets, "future dependencies must coalesce with the pending reset")

	incoming <- protocol.Output{Epoch: 99, Size: domain.Size{Cols: 6, Rows: 2}, Data: []byte("\x1b[2;1H!")}
	sideEffect := awaitUIPublication(t, terminal, func(snapshot ports.UISnapshot) bool { return len(snapshot.Cells) > 6 && snapshot.Cells[6].Text == "!" })
	require.Equal(t, latest.Context, sideEffect.Context, "side effects retain the committed view instead of inventing epoch 99")

	view.Publication = 4
	view.Route.Target = protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "destination"}
	incoming <- protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 6, Rows: 2}, Context: &view, Data: []byte("\x1b[2J\x1b[Hnew")}
	final := awaitUIPublication(t, terminal, func(snapshot ports.UISnapshot) bool { return snapshot.Context.ViewPublication == 4 })
	require.Equal(t, view.Route, final.Context.Route, "captured output identity wins over older route bookkeeping")
	require.Equal(t, "new", uiFirstCells(final, 3))
}

func awaitUIPublication(t *testing.T, state ports.UIState, matches func(ports.UISnapshot) bool) ports.UISnapshot {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		if snapshot, err := state.Snapshot(); err == nil && matches(snapshot) {
			return snapshot
		}
		select {
		case <-state.Changes():
		case <-deadline.C:
			t.Fatal("expected UI publication was not committed")
		}
	}
}

func uiFirstCells(snapshot ports.UISnapshot, count int) string {
	var text string
	for _, cell := range snapshot.Cells[:count] {
		text += cell.Text
	}
	return text
}
