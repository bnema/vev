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
		clientRaw := &scriptedTransport{recv: []wire.Envelope{mustPreambleResponse(t)}}
		require.NoError(t, NewClientConnection(clientRaw).SendClient(message))
		sent := mustSingleAppPayload(t, clientRaw)
		serverRaw := &scriptedTransport{recv: mustServerPreambleQueue(t, sent)}
		got, err := NewServerConnection(serverRaw).ReceiveClient()
		require.NoError(t, err)
		require.Equal(t, message, got)
		headerLen := len(sent)
		if input, ok := message.(protocol.Input); ok {
			// Input carries opaque bytes after the envelope header; only
			// the envelope framing has strict prefix truncation semantics.
			headerLen -= len(input.Data)
		}
		for size := range headerLen {
			raw := &scriptedTransport{recv: mustServerPreambleQueue(t, sent[:size])}
			_, err := NewServerConnection(raw).ReceiveClient()
			require.Error(t, err, "prefix %d", size)
		}
	}
}

func testUIOutputContext() *protocol.ViewContext {
	return &protocol.ViewContext{Publication: 4,
		Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}},
		TabID: "tab-1", FocusedPaneID: "pane-1"}
}

func TestUISynchronizationServerDispatch(t *testing.T) {
	context := *testUIOutputContext()
	for _, test := range []struct {
		name    string
		message protocol.ServerMessage
	}{
		{"output", protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: &context}},
		{"receipt", protocol.UIReceipt{ActionID: 7, Epoch: 2, State: 1, ViewPublication: 4, Outcome: protocol.UIReceiptProcessed}},
		{"view update", protocol.UIViewUpdate{Epoch: 2, State: 1, Context: context}},
		{"attach target", protocol.AttachTarget{CauseActionID: 7, Session: "work", Intent: protocol.IntentAttach}},
		{"route action", protocol.RouteNavigationAction{CauseActionID: 7, SnapshotGeneration: 1, Key: 2, Generation: 3}},
		{"route create", protocol.RouteCreateSessionAction{CauseActionID: 7, RequestID: 4, SnapshotGeneration: 1, Key: 2, Generation: 3, SessionName: "work"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, async := range []bool{false, true} {
				serverRaw := &capableTransport{}
				server := NewServerConnection(serverRaw)
				var sent []byte
				if async {
					require.NoError(t, server.SendServerAsync(test.message))
					require.Equal(t, 1, len(serverRaw.async))
					sent = serverRaw.async[0].Payload
				} else {
					require.NoError(t, server.SendServer(test.message))
					require.Equal(t, 1, serverRaw.sentLen())
					sent = serverRaw.sentPayload(0)
				}
				clientRaw := &scriptedTransport{recv: mustClientPreambleQueue(t, sent)}
				got, err := NewClientConnection(clientRaw).ReceiveServer()
				require.NoError(t, err)
				require.Equal(t, test.message, got)
				for size := range len(sent) {
					raw := &scriptedTransport{recv: mustClientPreambleQueue(t, sent[:size])}
					_, err := NewClientConnection(raw).ReceiveServer()
					require.Error(t, err, "prefix %d", size)
				}
				raw := &scriptedTransport{recv: mustClientPreambleQueue(t, append(append([]byte(nil), sent...), 0))}
				_, err = NewClientConnection(raw).ReceiveServer()
				require.Error(t, err)
			}
		})
	}
}
