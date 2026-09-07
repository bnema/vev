package wire

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func testViewContext() protocol.ViewContext {
	return protocol.ViewContext{
		Publication: 1,
		Route:       protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}},
		TabID:       "tab-1", FocusedPaneID: "pane-1",
	}
}

// testViewContextGolden is independent of the codec; output golden tests also
// use its literal length to target compression fields after semantic context.
func testViewContextGolden() []byte {
	return []byte{
		0, 0, 0, 0, 0, 0, 0, 1,
		1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 4, 'w', 'o', 'r', 'k', 0,
		0, 5, 't', 'a', 'b', '-', '1',
		0, 6, 'p', 'a', 'n', 'e', '-', '1',
	}
}

func TestUIWireRoundTripsAndRejectsTruncation(t *testing.T) {
	tests := []struct {
		name             string
		marshal          func() ([]byte, error)
		unmarshal        func([]byte) error
		mutate           func([]byte)
		wantMarshalError bool
		wantUnmarshalErr bool
	}{
		{
			name:      "fence",
			marshal:   func() ([]byte, error) { return MarshalUIFence(protocol.UIFence{ActionID: 7}) },
			unmarshal: func(data []byte) error { _, err := UnmarshalUIFence(data); return err },
		},
		{
			name: "receipt",
			marshal: func() ([]byte, error) {
				return MarshalUIReceipt(protocol.UIReceipt{ActionID: 7, Epoch: 2, State: 3, ViewPublication: 4, Outcome: protocol.UIReceiptProcessed})
			},
			unmarshal: func(data []byte) error { _, err := UnmarshalUIReceipt(data); return err },
		},
		{
			name: "unavailable receipt",
			marshal: func() ([]byte, error) {
				return MarshalUIReceipt(protocol.UIReceipt{ActionID: 7, Outcome: protocol.UIReceiptUnavailable})
			},
			unmarshal: func(data []byte) error { _, err := UnmarshalUIReceipt(data); return err },
		},
		{
			name: "view update",
			marshal: func() ([]byte, error) {
				return MarshalUIViewUpdate(protocol.UIViewUpdate{Epoch: 2, State: 3, Context: testViewContext()})
			},
			unmarshal: func(data []byte) error { _, err := UnmarshalUIViewUpdate(data); return err },
		},
		{
			name: "view update zero state marshal",
			marshal: func() ([]byte, error) {
				return MarshalUIViewUpdate(protocol.UIViewUpdate{Epoch: 1, Context: testViewContext()})
			},
			wantMarshalError: true,
		},
		{
			name: "view update zero state wire",
			marshal: func() ([]byte, error) {
				return MarshalUIViewUpdate(protocol.UIViewUpdate{Epoch: 1, State: 1, Context: testViewContext()})
			},
			unmarshal:        func(data []byte) error { _, err := UnmarshalUIViewUpdate(data); return err },
			mutate:           func(data []byte) { clear(data[8:16]) },
			wantUnmarshalErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := test.marshal()
			if test.wantMarshalError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if test.mutate != nil {
				test.mutate(encoded)
				if test.wantUnmarshalErr {
					require.Error(t, test.unmarshal(encoded))
				} else {
					require.NoError(t, test.unmarshal(encoded))
				}
				return
			}
			require.NoError(t, test.unmarshal(encoded))
			for size := range len(encoded) {
				require.Error(t, test.unmarshal(encoded[:size]), "prefix %d", size)
			}
			require.Error(t, test.unmarshal(append(append([]byte(nil), encoded...), 0)))
		})
	}
}
