package vt

import (
	"encoding/binary"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestHistoryCodecRejectsMalformedBounds(t *testing.T) {
	blob, err := MarshalHistory(HistoryView{
		chunks:    []*HistoryChunk{{rows: [][]renderer.Cell{historyRow("abcd")}, bounds: []LineBound{{End: 4, Soft: true}}, rowIDs: []RowID{1}}},
		rows:      1,
		cells:     4,
		nextRowID: 2,
	})
	require.NoError(t, err)
	boundOffset := len(blob) - historyBoundBytes

	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{
			name: "end exceeds row width",
			mutate: func(data []byte) {
				binary.BigEndian.PutUint32(data[boundOffset:boundOffset+4], 5)
			},
		},
		{
			name: "soft flag is not canonical",
			mutate: func(data []byte) {
				data[boundOffset+4] = 2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := append([]byte(nil), blob...)
			tt.mutate(data)

			_, preflightErr := PreflightHistoryBlob(data)
			require.Error(t, preflightErr)
			_, unmarshalErr := UnmarshalHistory(data)
			require.Error(t, unmarshalErr)
		})
	}
}
