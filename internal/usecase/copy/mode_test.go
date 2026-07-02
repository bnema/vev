package copy

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func snapshot(lines []string, height int) Snapshot {
	rows := make([][]renderer.Cell, len(lines))
	for i, line := range lines {
		rows[i] = row(line)
	}
	return Snapshot{Rows: rows, Width: 8, Height: height}
}

func TestCopyModeNavigationBounds(t *testing.T) {
	tests := []struct {
		name       string
		lines      []string
		height     int
		ops        func(*Mode, Snapshot)
		wantCursor int
		wantTop    int
	}{
		{
			name:       "initial viewport starts at bottom",
			lines:      []string{"00", "01", "02", "03", "04"},
			height:     3,
			ops:        func(*Mode, Snapshot) {},
			wantCursor: 4,
			wantTop:    2,
		},
		{
			name:   "k clamps at top and scrolls viewport",
			lines:  []string{"00", "01", "02", "03", "04"},
			height: 3,
			ops: func(m *Mode, s Snapshot) {
				m.Move(s, -99)
			},
			wantCursor: 0,
			wantTop:    0,
		},
		{
			name:   "j clamps at bottom",
			lines:  []string{"00", "01", "02", "03", "04"},
			height: 3,
			ops: func(m *Mode, s Snapshot) {
				m.Move(s, 99)
			},
			wantCursor: 4,
			wantTop:    2,
		},
		{
			name:   "page up moves by viewport height",
			lines:  []string{"00", "01", "02", "03", "04", "05", "06"},
			height: 3,
			ops: func(m *Mode, s Snapshot) {
				m.Page(s, -1)
			},
			wantCursor: 3,
			wantTop:    3,
		},
		{
			name:   "g and G jump to document bounds",
			lines:  []string{"00", "01", "02", "03", "04"},
			height: 3,
			ops: func(m *Mode, s Snapshot) {
				m.Top(s)
				m.Bottom(s)
			},
			wantCursor: 4,
			wantTop:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := snapshot(tt.lines, tt.height)
			m := NewMode(s)
			tt.ops(m, s)
			if m.Cursor != tt.wantCursor || m.ViewportTop != tt.wantTop {
				t.Fatalf("cursor/top = %d/%d, want %d/%d", m.Cursor, m.ViewportTop, tt.wantCursor, tt.wantTop)
			}
		})
	}
}

func TestCopyModeSelectionPayloadAndInverse(t *testing.T) {
	s := snapshot([]string{"alpha   ", "beta    ", "gamma   "}, 2)
	m := &Mode{ViewportTop: 0, Cursor: 0, Anchor: 0, Selecting: true}
	m.Move(s, 1)

	if got, want := m.SelectedText(s), "alpha\nbeta"; got != want {
		t.Fatalf("SelectedText() = %q, want %q", got, want)
	}
	frame := m.Render(s)
	for y := range 2 {
		if !frame.At(0, y).Style.Inverse {
			t.Fatalf("row %d first cell not inverse", y)
		}
	}
}

func TestOSC52Base64Encoding(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "small payload", text: "hello\nworld"},
		{name: "maximum payload", text: strings.Repeat("x", OSC52MaxPayloadBytes)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := OSC52(tt.text)
			if len(chunks) != 1 {
				t.Fatalf("len(chunks) = %d, want 1", len(chunks))
			}
			for _, chunk := range chunks {
				decoded := decodeOSC52Chunk(t, chunk)
				if string(decoded) != tt.text {
					t.Fatalf("decoded = %q, want %q", string(decoded), tt.text)
				}
			}
		})
	}
}

func TestOSC52OversizedPayloadIsDeferred(t *testing.T) {
	chunks := OSC52(strings.Repeat("x", OSC52MaxPayloadBytes+1))
	if len(chunks) != 0 {
		t.Fatalf("len(chunks) = %d, want 0 to avoid corrupt multi-sequence clipboard replacement", len(chunks))
	}
}

func decodeOSC52Chunk(t *testing.T, chunk []byte) []byte {
	t.Helper()
	s := string(chunk)
	if !strings.HasPrefix(s, "\x1b]52;c;") || !strings.HasSuffix(s, "\x07") {
		t.Fatalf("chunk %q missing OSC52 wrapper", s)
	}
	encoded := strings.TrimSuffix(strings.TrimPrefix(s, "\x1b]52;c;"), "\x07")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("DecodeString(%q) error = %v", encoded, err)
	}
	return decoded
}
