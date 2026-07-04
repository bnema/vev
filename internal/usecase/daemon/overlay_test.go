package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouteOverlayBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		chunks        [][]byte
		wantRunes     []rune
		wantPending   []byte
		wantBackspace int
		wantEnter     int
		wantCancel    int
		wantUp        int
		wantDown      int
	}{
		{
			name:        "text and split utf8",
			chunks:      [][]byte{{'w', 0xc3}, {0xb8, 'k'}},
			wantRunes:   []rune{'w', 'ø', 'k'},
			wantPending: nil,
		},
		{
			name:        "incomplete utf8 remains pending",
			chunks:      [][]byte{{'w', 0xc3}},
			wantRunes:   []rune{'w'},
			wantPending: []byte{0xc3},
		},
		{
			name:     "incomplete csi resumes as arrow",
			chunks:   [][]byte{[]byte("\x1b["), []byte("B")},
			wantDown: 1,
		},
		{
			name:        "incomplete csi remains pending",
			chunks:      [][]byte{[]byte("\x1b[")},
			wantPending: []byte("\x1b["),
		},
		{
			name:     "incomplete ss3 resumes as arrow",
			chunks:   [][]byte{[]byte("\x1bO"), []byte("A\x1bOB")},
			wantUp:   1,
			wantDown: 1,
		},
		{
			name:        "incomplete ss3 remains pending",
			chunks:      [][]byte{[]byte("\x1bO")},
			wantPending: []byte("\x1bO"),
		},
		{
			name:     "arrows and ctrl navigation",
			chunks:   [][]byte{[]byte("\x1b[A\x1b[B\x0e\x10")},
			wantUp:   2,
			wantDown: 2,
		},
		{
			name:      "unhandled escapes are consumed",
			chunks:    [][]byte{[]byte("a\x1b[D\x1b[3~b\x1bOQ")},
			wantRunes: []rune{'a', 'b'},
		},
		{
			name:          "submit cancel and editing",
			chunks:        [][]byte{{0x7f, 0x08, '\r', '\n', 0x1b, 0x03}},
			wantBackspace: 2,
			wantEnter:     2,
			wantCancel:    2,
		},
		{
			name:      "controls and invalid utf8 ignored",
			chunks:    [][]byte{{'a', 0x01, 0xff, 'b'}},
			wantRunes: []rune{'a', 'b'},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pending []byte
			var gotRunes []rune
			var gotBackspace, gotEnter, gotCancel, gotUp, gotDown int
			ev := overlayEvents{
				rune:      func(r rune) { gotRunes = append(gotRunes, r) },
				backspace: func() { gotBackspace++ },
				enter:     func() { gotEnter++ },
				cancel:    func() { gotCancel++ },
				up:        func() { gotUp++ },
				down:      func() { gotDown++ },
			}

			for _, chunk := range tt.chunks {
				routeOverlayBytes(chunk, &pending, ev)
			}

			require.Equal(t, tt.wantRunes, gotRunes)
			require.Equal(t, tt.wantPending, pending)
			require.Equal(t, tt.wantBackspace, gotBackspace)
			require.Equal(t, tt.wantEnter, gotEnter)
			require.Equal(t, tt.wantCancel, gotCancel)
			require.Equal(t, tt.wantUp, gotUp)
			require.Equal(t, tt.wantDown, gotDown)
		})
	}
}
