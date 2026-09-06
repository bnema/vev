package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPasteCoalescerBatchBoundaryPreservesOrdinaryBytes(t *testing.T) {
	for _, input := range []string{"", "hello", "\x1b", "word\x1b[20"} {
		t.Run(input, func(t *testing.T) {
			coalescer, _, collector := newTestCoalescer()
			t.Cleanup(coalescer.Close)
			require.True(t, coalescer.Idle())
			coalescer.Scan([]byte(input))
			require.True(t, coalescer.EndBatch())
			require.True(t, coalescer.Idle())
			var got string
			for _, emitted := range collector.snapshot() {
				got += string(emitted)
			}
			require.Equal(t, input, got)
			before := collector.snapshot()
			require.True(t, coalescer.EndBatch())
			require.Equal(t, before, collector.snapshot(), "batch end cannot duplicate an already emitted prefix")
			coalescer.Scan([]byte("next"))
			require.Equal(t, []byte("next"), collector.snapshot()[len(before)], "batch end must not close the input owner")
		})
	}
}

func TestPasteCoalescerBatchBoundaryDoesNotTerminateHumanPaste(t *testing.T) {
	coalescer, _, collector := newTestCoalescer()
	t.Cleanup(coalescer.Close)
	coalescer.Scan([]byte("\x1b[200~human"))
	require.False(t, coalescer.Idle())
	require.False(t, coalescer.EndBatch(), "automation cannot end somebody else's open paste")
	require.Empty(t, collector.snapshot())
	require.True(t, coalescer.Buffering())
	coalescer.Scan([]byte("\x1b[201~"))
	require.True(t, coalescer.Idle())
	require.Equal(t, [][]byte{[]byte("\x1b[200~human\x1b[201~")}, collector.snapshot())
	coalescer.Close()
	require.False(t, coalescer.Idle())
	require.False(t, coalescer.EndBatch())
}
