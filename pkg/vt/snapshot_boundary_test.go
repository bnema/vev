package vt

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreflightVisibleRejectsMalformedBoundariesAndLegacyFormat(t *testing.T) {
	s := NewScreen(4, 2)
	s.Write([]byte("abcdef"))
	blob, err := s.MarshalPrimaryVisible()
	require.NoError(t, err)

	boundaryOffset := 13 + 2*4*historyCellBytes
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{
			name: "boundary end exceeds row width",
			mutate: func(data []byte) {
				binary.BigEndian.PutUint32(data[boundaryOffset:boundaryOffset+4], 5)
			},
		},
		{
			name: "soft flag is not canonical",
			mutate: func(data []byte) {
				data[boundaryOffset+4] = 2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := append([]byte(nil), blob...)
			tt.mutate(data)
			_, err := PreflightVisibleBlob(data)
			require.Error(t, err)
		})
	}

	t.Run("legacy VTV1 omits boundaries", func(t *testing.T) {
		legacy := append([]byte(nil), blob[:boundaryOffset]...)
		copy(legacy[:4], "VTV1")
		_, err := PreflightVisibleBlob(legacy)
		require.Error(t, err)
	})
}
