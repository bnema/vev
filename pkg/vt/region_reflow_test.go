package vt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResizeSeversSoftWrapsAtRegionalScrollEdges(t *testing.T) {
	tests := []struct {
		name string
		seq  string
		want []string
	}{
		{
			name: "CSI S",
			seq:  "\x1b[2;4r\x1b[S",
			want: []string{"ABC", "IJK", "LMN", "OP ", "   ", "QRS"},
		},
		{
			name: "CSI T",
			seq:  "\x1b[2;4r\x1b[T",
			want: []string{"ABC", "   ", "EFG", "HIJ", "KL ", "QRS"},
		},
		{
			name: "insert lines",
			seq:  "\x1b[2;4r\x1b[2;1H\x1b[L",
			want: []string{"ABC", "   ", "EFG", "HIJ", "KL ", "QRS"},
		},
		{
			name: "delete lines",
			seq:  "\x1b[2;4r\x1b[2;1H\x1b[M",
			want: []string{"ABC", "IJK", "LMN", "OP ", "   ", "QRS"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(4, 5)
			s.Write([]byte("ABCDEFGHIJKLMNOPQRST"))
			s.Write([]byte(tt.seq))

			s.Resize(3, 6)

			got := make([]string, s.Frame.Height)
			for y := range s.Frame.Height {
				got[y] = rowString(s.Frame.Row(y))
			}
			require.Equal(t, tt.want, got)
		})
	}
}
