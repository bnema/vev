package protoconv

import (
	"encoding/binary"
	"fmt"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestPackedPreviewRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		width, height, styles int
		wide                  bool
	}{
		{"ascii", 2, 1, 1, false}, {"wide and zero", 4, 1, 1, true}, {"many styles", 80, 24, 400, false},
		{"style limit", 256, 4, 1024, false}, {"capped maximum", 256, 128, 32768, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testRemotePreview()
			p.Width = uint16(tc.width)
			p.Height = uint16(tc.height)
			p.Cells = make([]renderer.Cell, tc.width*tc.height)
			for i := range p.Cells {
				s := renderer.DefaultStyle()
				s.HasForegroundRGB = true
				s.ForegroundRGB = renderer.RGB{R: uint8(i % tc.styles), G: uint8((i % tc.styles) >> 8), B: 255}
				p.Cells[i] = renderer.Cell{Rune: 'x', Style: s}
			}
			if tc.wide {
				p.Cells[0].Rune = '界'
				p.Cells[1].Rune = 0
				p.Cells[1].Continuation = true
				p.Cells[2].Rune = 0
			}
			if tc.styles > protocol.RemotePreviewMaxStyles {
				_, err := RemotePreviewToWire(p)
				require.ErrorIs(t, err, protocol.ErrInvalidRemotePreview)
			}
			protocol.CapRemotePreviewStyles(p.Cells)
			w, err := RemotePreviewToWire(p)
			require.NoError(t, err)
			require.LessOrEqual(t, len(w.Styles), 1024)
			got, err := RemotePreviewFromWire(w)
			require.NoError(t, err)
			require.Equal(t, p, got)
		})
	}
	for status := protocol.RemotePreviewUnavailable; status <= protocol.RemotePreviewTooLarge; status++ {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			p := protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: status}
			w, err := RemotePreviewToWire(p)
			require.NoError(t, err)
			require.Empty(t, w.Styles)
			require.Empty(t, w.Runes)
			require.Empty(t, w.StyleIds)
			got, err := RemotePreviewFromWire(w)
			require.NoError(t, err)
			require.Equal(t, p, got)
		})
	}
}

// TestPackedPreviewBytes pins the packed layout independently of the
// encoder: little-endian uint32 runes with 0xFFFFFFFF marking a
// continuation, and little-endian uint16 style indexes.
func TestPackedPreviewBytes(t *testing.T) {
	styles := make([]renderer.Style, 300)
	for i := range styles {
		styles[i] = renderer.DefaultStyle()
		styles[i].HasForegroundRGB = true
		styles[i].ForegroundRGB = renderer.RGB{R: uint8(i), G: uint8(i >> 8)}
	}
	cells := make([]renderer.Cell, 0, 300)
	for i := 0; i < 296; i++ {
		cells = append(cells, renderer.Cell{Rune: 'a', Style: styles[i]})
	}
	cells = append(cells,
		renderer.Cell{Rune: '界', Style: styles[296]},
		renderer.Cell{Continuation: true, Style: styles[297]},
		renderer.Cell{Rune: 0, Style: styles[298]},
		renderer.Cell{Rune: 'é', Style: styles[299]},
	)
	preview := testRemotePreview()
	preview.Width, preview.Height, preview.Cells = 150, 2, cells

	w, err := RemotePreviewToWire(preview)
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		cell      int
		wantRune  []byte
		wantStyle []byte
	}{
		{"ascii first", 0, []byte{'a', 0, 0, 0}, []byte{0, 0}},
		{"wide", 296, []byte{0x4c, 0x75, 0, 0}, []byte{0x28, 0x01}},
		{"continuation", 297, []byte{0xff, 0xff, 0xff, 0xff}, []byte{0x29, 0x01}},
		{"zero rune", 298, []byte{0, 0, 0, 0}, []byte{0x2a, 0x01}},
		{"non-ascii", 299, []byte{0xe9, 0, 0, 0}, []byte{0x2b, 0x01}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantRune, w.Runes[tc.cell*4:tc.cell*4+4])
			require.Equal(t, tc.wantStyle, w.StyleIds[tc.cell*2:tc.cell*2+2])
		})
	}

	// Decode a hand-built message to prove the layout is read the same way.
	hand := &wire.RemotePreview{
		Version: w.Version, Status: w.Status, LifecycleId: w.LifecycleId, TabId: w.TabId, Revision: w.Revision,
		Width: 3, Height: 1,
		Styles:   []*wire.CellStyle{CellStyleToWire(renderer.DefaultStyle()), CellStyleToWire(styles[1])},
		Runes:    []byte{0x4c, 0x75, 0, 0, 0xff, 0xff, 0xff, 0xff, 'z', 0, 0, 0},
		StyleIds: []byte{1, 0, 1, 0, 0, 0},
	}
	got, err := RemotePreviewFromWire(hand)
	require.NoError(t, err)
	require.Equal(t, []renderer.Cell{
		{Rune: '界', Style: styles[1]},
		{Continuation: true, Style: styles[1]},
		{Rune: 'z', Style: renderer.DefaultStyle()},
	}, got.Cells)
}

func TestPackedPreviewMalformed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*wire.RemotePreview)
	}{
		{"index", func(w *wire.RemotePreview) { binary.LittleEndian.PutUint16(w.StyleIds, 1024) }},
		{"short runes", func(w *wire.RemotePreview) { w.Runes = w.Runes[:4] }},
		{"long runes", func(w *wire.RemotePreview) { w.Runes = append(w.Runes, 0, 0, 0, 0) }},
		{"odd runes", func(w *wire.RemotePreview) { w.Runes = w.Runes[:len(w.Runes)-1] }},
		{"short ids", func(w *wire.RemotePreview) { w.StyleIds = w.StyleIds[:2] }},
		{"long ids", func(w *wire.RemotePreview) { w.StyleIds = append(w.StyleIds, 0, 0) }},
		{"odd ids", func(w *wire.RemotePreview) { w.StyleIds = w.StyleIds[:len(w.StyleIds)-1] }},
		{"empty styles", func(w *wire.RemotePreview) { w.Styles = nil }},
		{"too many styles", func(w *wire.RemotePreview) { w.Styles = make([]*wire.CellStyle, 1025) }},
		{"invalid rune", func(w *wire.RemotePreview) { binary.LittleEndian.PutUint32(w.Runes, 0x110000) }},
		{"surrogate", func(w *wire.RemotePreview) { binary.LittleEndian.PutUint32(w.Runes, 0xD800) }},
		{"non OK styles", func(w *wire.RemotePreview) { w.Status = 1; w.Width = 0; w.Height = 0; w.Runes = nil; w.StyleIds = nil }},
		{"unreferenced bad underline", func(w *wire.RemotePreview) { w.Styles = append(w.Styles, &wire.CellStyle{UnderlineStyle: 255}) }},
		{"unreferenced bad attrs", func(w *wire.RemotePreview) { w.Styles = append(w.Styles, &wire.CellStyle{Attrs: 1 << 15}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, err := RemotePreviewToWire(testRemotePreview())
			require.NoError(t, err)
			tc.mutate(w)
			_, err = RemotePreviewFromWire(w)
			require.Error(t, err)
		})
	}
}
