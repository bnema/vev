package wire

import (
	"bytes"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestOutputContextRoundTripStrict(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "delta"
		if full {
			name = "compressed full"
		}
		t.Run(name, func(t *testing.T) {
			context := testViewContext()
			output := protocol.Output{Epoch: 2, Base: 1, New: 2, Echo: 9, ViewRevision: 4,
				Size: domain.Size{Cols: 80, Rows: 24}, Context: &context, Data: []byte("view")}
			if full {
				output.Base, output.New, output.Full = 0, 1, true
				output.Data = bytes.Repeat([]byte("view"), 1024)
			}
			encoded, err := MarshalOutput(output)
			require.NoError(t, err)
			decoded, err := UnmarshalOutput(encoded)
			require.NoError(t, err)
			require.Equal(t, output, decoded)
			for size := range len(encoded) {
				_, err := UnmarshalOutput(encoded[:size])
				require.Error(t, err, "prefix %d", size)
			}
			_, err = UnmarshalOutput(append(append([]byte(nil), encoded...), 0))
			require.Error(t, err)
		})
	}
}

func TestOutputContextFitsMaximumFrame(t *testing.T) {
	context := testViewContext()
	context.Route.Target.SessionName = strings.Repeat("s", 64)
	context.TabID = domain.TabStableID(strings.Repeat("t", 128))
	context.FocusedPaneID = domain.PaneStableID(strings.Repeat("p", 128))
	output := protocol.Output{Epoch: 1, Base: 1, New: 2, Size: domain.Size{Cols: 1, Rows: 1}, Context: &context,
		Data: make([]byte, protocol.MaxOutputDataLen)}
	encoded, err := MarshalOutput(output)
	require.NoError(t, err)
	require.Equal(t, MaxFrameLen-1, len(encoded), "type byte also consumes frame capacity")
	decoded, err := UnmarshalOutput(encoded)
	require.NoError(t, err)
	require.Equal(t, output, decoded)
	output.Data = append(output.Data, 0)
	_, err = MarshalOutput(output)
	require.ErrorIs(t, err, protocol.ErrInvalidOutput)
}

func TestOutputRejectsInvalidSemanticContext(t *testing.T) {
	for _, scenario := range []string{"side effect", "zero publication", "missing lifecycle", "missing tab", "missing pane"} {
		t.Run(scenario, func(t *testing.T) {
			context := testViewContext()
			output := protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 1, Rows: 1}, Context: &context}
			switch scenario {
			case "side effect":
				output.New, output.Full = 0, false
			case "zero publication":
				context.Publication = 0
			case "missing lifecycle":
				context.Route.Target.LifecycleID = domain.SessionLifecycleID{}
			case "missing tab":
				context.TabID = ""
			case "missing pane":
				context.FocusedPaneID = ""
			}
			require.ErrorIs(t, protocol.ValidateOutput(output), protocol.ErrInvalidOutput)
			_, err := MarshalOutput(output)
			require.ErrorIs(t, err, protocol.ErrInvalidOutput)
		})
	}
}
