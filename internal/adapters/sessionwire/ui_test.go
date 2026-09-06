package sessionwire

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestUISynchronizationClientDispatch(t *testing.T) {
	for _, message := range []protocol.ClientMessage{
		protocol.Input{InputSeq: 3, ActionID: 7, Data: []byte("x")},
		protocol.UIFence{ActionID: 7},
	} {
		raw := &scriptedTransport{}
		require.NoError(t, NewClientConnection(raw).SendClient(message))
		raw.recv = raw.sent
		got, err := NewServerConnection(raw).ReceiveClient()
		require.NoError(t, err)
		require.Equal(t, message, got)
		headerLen := len(raw.sent.Payload)
		if input, ok := message.(protocol.Input); ok {
			// Input consumes the remaining frame as bytes; only its fixed header
			// has strict prefix truncation semantics.
			headerLen -= len(input.Data)
		}
		for size := range headerLen {
			raw.recv.Payload = raw.sent.Payload[:size]
			_, err := NewServerConnection(raw).ReceiveClient()
			require.Error(t, err, "type %d prefix %d", raw.sent.Type, size)
		}
	}
}

func TestUISynchronizationServerDispatch(t *testing.T) {
	context := protocol.ViewContext{Publication: 4,
		Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}},
		TabID: "tab-1", FocusedPaneID: "pane-1"}
	for _, test := range []struct {
		name    string
		message protocol.ServerMessage
	}{
		{"output", protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: &context}},
		{"receipt", protocol.UIReceipt{ActionID: 7, Epoch: 2, State: 1, ViewPublication: 4, Outcome: protocol.UIReceiptProcessed}},
		{"view update", protocol.UIViewUpdate{Epoch: 2, State: 1, Context: context}},
		{"attach target", protocol.AttachTarget{CauseActionID: 7, Session: "work", Intent: protocol.IntentAttach}},
		{"navigation", protocol.NavigationDirective{CauseActionID: 7, Action: protocol.NavigationBack}},
		{"route action", protocol.RouteNavigationAction{CauseActionID: 7, SnapshotGeneration: 1, Key: 2, Generation: 3}},
		{"route create", protocol.RouteCreateSessionAction{CauseActionID: 7, RequestID: 4, SnapshotGeneration: 1, Key: 2, Generation: 3, SessionName: "work"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, async := range []bool{false, true} {
				raw := &capableTransport{}
				server := NewServerConnection(raw)
				var frame wire.Frame
				if async {
					require.NoError(t, server.SendServerAsync(test.message))
					frame = raw.async
				} else {
					require.NoError(t, server.SendServer(test.message))
					frame = raw.sent
				}
				raw.recv = frame
				got, err := NewClientConnection(raw).ReceiveServer()
				require.NoError(t, err)
				require.Equal(t, test.message, got)
				for size := range len(frame.Payload) {
					raw.recv.Payload = frame.Payload[:size]
					_, err := NewClientConnection(raw).ReceiveServer()
					require.Error(t, err, "prefix %d", size)
				}
				raw.recv.Payload = append(append([]byte(nil), frame.Payload...), 0)
				_, err = NewClientConnection(raw).ReceiveServer()
				require.Error(t, err)
			}
		})
	}
}
