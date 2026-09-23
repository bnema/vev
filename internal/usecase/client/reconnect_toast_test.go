package client

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestDrawClientToastPlacement(t *testing.T) {
	size := domain.Size{Cols: 100, Rows: 30}
	tests := []struct {
		name   string
		anchor domain.Anchor
		check  func(t *testing.T, bounds domain.Rect)
	}{
		{name: "notice sits top-right", anchor: domain.AnchorTopRight, check: func(t *testing.T, bounds domain.Rect) {
			require.Less(t, bounds.Y, 3)
			require.Greater(t, bounds.X+bounds.Width, size.Cols-3)
		}},
		{name: "transition stays centered", anchor: domain.AnchorCenter, check: func(t *testing.T, bounds domain.Rect) {
			require.Equal(t, (size.Rows-bounds.Height)/2, bounds.Y)
			require.Equal(t, (size.Cols-bounds.Width)/2, bounds.X)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			bounds, err := drawClientToast(&out, size, "SSH authentication failed", tt.anchor)
			require.NoError(t, err)
			require.NotZero(t, out.Len())
			tt.check(t, bounds)
		})
	}
}
