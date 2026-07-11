package copy

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func snapshot(lines []string, height int) Snapshot {
	rows := make([][]renderer.Cell, len(lines))
	for i, line := range lines {
		rows[i] = row(line)
	}
	return NewSnapshotFromRows(rows, 16, height)
}

func inverseRow(text string) []renderer.Cell {
	cells := row(text)
	for i := range cells {
		cells[i].Style.Inverse = true
	}
	return cells
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

func TestCopyModeAtBottom(t *testing.T) {
	s := snapshot([]string{"00", "01", "02", "03", "04"}, 3)
	m := NewMode(s)
	if !m.AtBottom(s) {
		t.Fatalf("AtBottom() after NewMode = false, want true; cursor/top=%d/%d", m.Cursor, m.ViewportTop)
	}

	m.Move(s, -1)
	if m.AtBottom(s) {
		t.Fatalf("AtBottom() after moving above bottom = true, want false; cursor/top=%d/%d", m.Cursor, m.ViewportTop)
	}

	m.Bottom(s)
	if !m.AtBottom(s) {
		t.Fatalf("AtBottom() after Bottom = false, want true; cursor/top=%d/%d", m.Cursor, m.ViewportTop)
	}

	m.Top(s)
	if m.AtBottom(s) {
		t.Fatalf("AtBottom() after Top = true, want false; cursor/top=%d/%d", m.Cursor, m.ViewportTop)
	}
}

func TestCopyModeAtBottomWithShortSnapshot(t *testing.T) {
	s := snapshot([]string{"00", "01"}, 5)
	m := NewMode(s)
	if !m.AtBottom(s) {
		t.Fatalf("AtBottom() after NewMode on short snapshot = false, want true; cursor/top=%d/%d", m.Cursor, m.ViewportTop)
	}

	m.Top(s)
	if m.AtBottom(s) {
		t.Fatalf("AtBottom() after Top on short snapshot = true, want false; cursor/top=%d/%d", m.Cursor, m.ViewportTop)
	}

	m.Bottom(s)
	if !m.AtBottom(s) {
		t.Fatalf("AtBottom() after Bottom on short snapshot = false, want true; cursor/top=%d/%d", m.Cursor, m.ViewportTop)
	}
}

func TestCopyModeSelectionAPI(t *testing.T) {
	tests := []struct {
		name          string
		ops           func(*Mode, Snapshot)
		wantCursor    int
		wantTop       int
		wantAnchor    int
		wantSelecting bool
	}{
		{
			name: "SetCursor clamps and scrolls viewport",
			ops: func(m *Mode, s Snapshot) {
				m.SetCursor(s, -10)
			},
			wantCursor: 0,
			wantTop:    0,
			wantAnchor: -1,
		},
		{
			name: "StartSelectionAt sets cursor anchor and selecting",
			ops: func(m *Mode, s Snapshot) {
				m.StartSelectionAt(s, 2)
			},
			wantCursor:    2,
			wantTop:       2,
			wantAnchor:    2,
			wantSelecting: true,
		},
		{
			name: "ExtendTo preserves anchor while moving cursor",
			ops: func(m *Mode, s Snapshot) {
				m.StartSelectionAt(s, 1)
				m.ExtendTo(s, 4)
			},
			wantCursor:    4,
			wantTop:       2,
			wantAnchor:    1,
			wantSelecting: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := snapshot([]string{"00", "01", "02", "03", "04"}, 3)
			m := NewMode(s)
			tt.ops(m, s)
			if m.Cursor != tt.wantCursor || m.ViewportTop != tt.wantTop || m.Anchor != tt.wantAnchor || m.Selecting != tt.wantSelecting {
				t.Fatalf("mode = cursor:%d top:%d anchor:%d selecting:%v, want cursor:%d top:%d anchor:%d selecting:%v", m.Cursor, m.ViewportTop, m.Anchor, m.Selecting, tt.wantCursor, tt.wantTop, tt.wantAnchor, tt.wantSelecting)
			}
		})
	}
}

func TestCopyModeExtendToAboveAnchorSwapsSelectedBounds(t *testing.T) {
	s := snapshot([]string{"00", "01", "02", "03", "04"}, 3)
	m := NewMode(s)

	m.StartSelectionAt(s, 4)
	m.ExtendTo(s, 1)

	if m.Cursor != 1 || m.ViewportTop != 1 || m.Anchor != 4 || !m.Selecting {
		t.Fatalf("mode = cursor:%d top:%d anchor:%d selecting:%v, want cursor:1 top:1 anchor:4 selecting:true", m.Cursor, m.ViewportTop, m.Anchor, m.Selecting)
	}
	lo, hi, ok := m.SelectedBounds()
	if !ok || lo != 1 || hi != 4 {
		t.Fatalf("SelectedBounds = (%d,%d,%v), want (1,4,true)", lo, hi, ok)
	}
}

func TestScrollbackModeStatusDistinguishesPassiveAndVisual(t *testing.T) {
	s := snapshot([]string{"alpha   ", "beta    ", "gamma   "}, 2)
	m := NewMode(s)

	frame := m.Render(s)
	if got := frameText(frame.Row(s.Height)); !strings.Contains(got, "[SCROLL]") || !strings.Contains(got, "3/3") || strings.Contains(got, "[VISUAL]") || strings.Contains(got, "[SELECT]") {
		t.Fatalf("passive status = %q, want [SCROLL] with N/M and no visual/select label", got)
	}

	m.ToggleSelection()
	frame = m.Render(s)
	if got := frameText(frame.Row(s.Height)); !strings.Contains(got, "[SELECT]") || !strings.Contains(got, "3/3") || strings.Contains(got, "[SCROLL]") || strings.Contains(got, "[VISUAL]") {
		t.Fatalf("selection status = %q, want [SELECT] with N/M and no passive label", got)
	}
}

func TestCopyModeSelectionPayloadAndInverse(t *testing.T) {
	s := snapshot([]string{"alpha   ", "beta    ", "gamma   "}, 2)
	s = NewSnapshotFromRows([][]renderer.Cell{row("alpha   "), inverseRow("beta    "), row("gamma   ")}, 16, 2)
	m := &Mode{ViewportTop: 0, Cursor: 0, Anchor: 0, Selecting: true}
	m.Move(s, 1)

	if got, want := m.SelectedText(s), "alpha\nbeta"; got != want {
		t.Fatalf("SelectedText() = %q, want %q", got, want)
	}
	frame := m.Render(s)
	for y := range 2 {
		for x, c := range frame.Row(y) {
			if !c.Style.Inverse {
				t.Fatalf("row %d cell %d not inverse", y, x)
			}
		}
	}
}

func TestCopyModeRenderLineCursorMarker(t *testing.T) {
	s := snapshot([]string{"alpha   ", "beta    ", "gamma   "}, 3)
	m := &Mode{ViewportTop: 0, Cursor: 1, Anchor: -1}

	frame := m.Render(s)
	if !frame.At(0, 1).Style.Inverse {
		t.Fatalf("cursor line first cell inverse = false, want true")
	}
	if frame.At(1, 1).Style.Inverse {
		t.Fatalf("cursor line second cell inverse = true, want marker on first cell only")
	}
	if frame.At(0, 0).Style.Inverse || frame.At(0, 2).Style.Inverse {
		t.Fatalf("non-cursor lines have inverse marker, want none")
	}

	m.StartSelectionAt(s, 1)
	frame = m.Render(s)
	for x, c := range frame.Row(1) {
		if !c.Style.Inverse {
			t.Fatalf("selected cursor row cell %d inverse = false, want full-line selection highlight", x)
		}
	}
}

func TestCopyModeSelectionFallbackKeepsAlreadyInverseCellInverse(t *testing.T) {
	// A cell that was already rendered inverse by the child program (e.g. its
	// own reverse-video attribute) must stay visibly selected: the fallback
	// selection style is a solid "force inverse", not an XOR toggle that would
	// flip it back to non-inverse and hide the highlight.
	rows := [][]renderer.Cell{row("alpha   "), row("beta    ")}
	rows[0][0].Style.Inverse = true
	s := NewSnapshotFromRows(rows, 16, 2)
	m := &Mode{ViewportTop: 0, Cursor: 0, Anchor: 0, Selecting: true}

	frame := m.Render(s)

	if !frame.At(0, 0).Style.Inverse {
		t.Fatalf("already-inverse selected cell became non-inverse; want Inverse=true")
	}
}

func TestCopyModeSelectionUsesProvidedStyle(t *testing.T) {
	s := snapshot([]string{"alpha   ", "beta    "}, 2)
	m := &Mode{ViewportTop: 0, Cursor: 0, Anchor: 0, Selecting: true}
	selection := renderer.DefaultStyle()
	selection.HasBackgroundRGB = true
	selection.BackgroundRGB = renderer.RGB{R: 1, G: 2, B: 3}

	frame := m.Render(s, renderer.DefaultStyle(), selection)

	got := frame.At(0, 0).Style
	if got.Inverse || !got.HasBackgroundRGB || got.BackgroundRGB != selection.BackgroundRGB {
		t.Fatalf("selected cell style = %+v, want themed selection %+v", got, selection)
	}
}

func frameText(row []renderer.Cell) string {
	var b strings.Builder
	for _, c := range row {
		if c.Rune == 0 {
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(c.Rune)
	}
	return b.String()
}

func TestCopyModeSearchMovesAndCyclesMatches(t *testing.T) {
	s := snapshot([]string{"alpha", "beta alpha", "gamma", "alpha omega"}, 2)
	m := NewMode(s)

	require.True(t, m.Search(s, "alpha"))
	require.Equal(t, 0, m.Cursor)
	require.Equal(t, 0, m.ViewportTop)

	require.True(t, m.NextSearchMatch(s, 1))
	require.Equal(t, 1, m.Cursor)
	require.Equal(t, 0, m.ViewportTop)

	require.True(t, m.NextSearchMatch(s, 1))
	require.Equal(t, 3, m.Cursor)
	require.Equal(t, 2, m.ViewportTop)

	require.True(t, m.NextSearchMatch(s, 1))
	require.Equal(t, 0, m.Cursor)

	require.True(t, m.NextSearchMatch(s, -1))
	require.Equal(t, 3, m.Cursor)
}

func TestFindMatchesReturnsNonOverlappingRepeatedMatches(t *testing.T) {
	s := snapshot([]string{"aaa"}, 1)

	matches := FindMatches(s, "aa")

	require.Len(t, matches, 1)
	require.Equal(t, 0, matches[0].Start)
	require.Equal(t, 2, matches[0].End)
}

func TestFindMatchesKeepsRuneAndCellIndexesAlignedWhenLowercasing(t *testing.T) {
	s := snapshot([]string{"İa"}, 1)

	matches := FindMatches(s, "İa")

	require.Len(t, matches, 1)
	require.Equal(t, 0, matches[0].Start)
	require.Equal(t, 2, matches[0].End)
}

func TestCopyModeSearchUsesDisplayCellOffsetsForWideRows(t *testing.T) {
	row := []renderer.Cell{
		{Rune: '界'},
		{Continuation: true},
		{Rune: ' '},
		{Rune: 'a'},
		{Rune: 'l'},
		{Rune: 'p'},
		{Rune: 'h'},
		{Rune: 'a'},
	}
	s := NewSnapshotFromRows([][]renderer.Cell{row}, 8, 1)

	wideMatches := FindMatches(s, "界")
	require.Len(t, wideMatches, 1)
	require.Equal(t, 0, wideMatches[0].Start)
	require.Equal(t, 2, wideMatches[0].End)

	matches := FindMatches(s, "alpha")
	require.Len(t, matches, 1)
	require.Equal(t, 3, matches[0].Start)
	require.Equal(t, 8, matches[0].End)
}

func TestCopyModeRenderHighlightsCurrentSearchMatch(t *testing.T) {
	s := snapshot([]string{"alpha", "beta alpha"}, 2)
	s.Width = 32
	m := NewMode(s)
	require.True(t, m.Search(s, "alpha"))

	frame := m.Render(s)

	for x := range len("alpha") {
		if !frame.At(x, 0).Style.Inverse {
			t.Fatalf("search match cell %d inverse = false, want highlighted", x)
		}
	}
	if !strings.Contains(frameText(frame.Row(s.Height)), "/alpha") {
		t.Fatalf("status row = %q, want search query", frameText(frame.Row(s.Height)))
	}
}

func TestCopyModeMoveWhileSelectingKeepsAnchorAndExtendsSelection(t *testing.T) {
	s := snapshot([]string{"00", "01", "02", "03", "04", "05"}, 3)
	m := NewMode(s)
	m.Move(s, -2)
	m.ToggleSelection()
	anchor := m.Anchor

	m.Move(s, 3)
	if m.Anchor != anchor {
		t.Fatalf("Anchor after Move(+3) while selecting = %d, want %d", m.Anchor, anchor)
	}
	lo, hi, ok := m.SelectedBounds()
	if !ok || lo != anchor || hi != m.Cursor {
		t.Fatalf("SelectedBounds after Move(+3) = %d/%d/%v, want %d/%d/true", lo, hi, ok, anchor, m.Cursor)
	}

	m.Move(s, -3)
	if m.Anchor != anchor {
		t.Fatalf("Anchor after Move(-3) while selecting = %d, want %d", m.Anchor, anchor)
	}
	lo, hi, ok = m.SelectedBounds()
	if !ok || lo != m.Cursor || hi != anchor {
		t.Fatalf("SelectedBounds after Move(-3) = %d/%d/%v, want %d/%d/true", lo, hi, ok, m.Cursor, anchor)
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

func TestOSC52FromBase64(t *testing.T) {
	valid := base64.StdEncoding.EncodeToString([]byte("hello"))
	oversized := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", OSC52MaxPayloadBytes+1)))

	tests := []struct {
		name string
		in   string
		want []byte
	}{
		{name: "valid", in: valid, want: []byte("\x1b]52;c;" + valid + "\x07")},
		{name: "empty payload", in: "", want: []byte("\x1b]52;c;\x07")},
		{name: "invalid base64", in: "not-valid-base64!!", want: nil},
		{name: "oversized decoded payload", in: oversized, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OSC52FromBase64(tt.in)
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("OSC52FromBase64(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
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
