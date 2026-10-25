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
