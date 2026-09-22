package protoconv

import (
	"errors"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// TestRangeHelpers proves Uint8/Uint16 accept every in-range value and
// refuse every value whose high bits would otherwise truncate into an
// aliased in-range value.
func TestRangeHelpers(t *testing.T) {
	t.Run("Uint8", func(t *testing.T) {
		tests := []struct {
			name    string
			in      uint32
			want    uint8
			wantErr bool
		}{
			{"zero", 0, 0, false},
			{"max", 255, 255, false},
			{"overflow by one", 256, 0, true},
			{"far overflow", 1 << 20, 0, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := Uint8[uint8](tt.in)
				if tt.wantErr {
					require.ErrorIs(t, err, ErrOutOfRange)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("Uint16", func(t *testing.T) {
		tests := []struct {
			name    string
			in      uint32
			want    uint16
			wantErr bool
		}{
			{"zero", 0, 0, false},
			{"max", 65535, 65535, false},
			{"overflow by one", 65536, 0, true},
			{"aliasing overflow", 1<<16 + 1, 0, true},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := Uint16[uint16](tt.in)
				if tt.wantErr {
					require.ErrorIs(t, err, ErrOutOfRange)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("Uint8 instantiated over a named enum", func(t *testing.T) {
		got, err := Uint8[domain.TabSelectorKind](uint32(domain.TabSelectorByStableID))
		require.NoError(t, err)
		require.Equal(t, domain.TabSelectorByStableID, got)
		_, err = Uint8[domain.TabSelectorKind](256)
		require.ErrorIs(t, err, ErrOutOfRange)
	})
}

// TestLifecycleRoundTrip proves lifecycle IDs survive an encode/decode
// cycle byte for byte and that decode refuses a nil message or a
// wrong-length value.
func TestLifecycleRoundTrip(t *testing.T) {
	id := domain.SessionLifecycleID{1, 2, 3, 4}
	wireID := LifecycleToWire(id)
	require.Equal(t, id[:], wireID.GetValue())

	decoded, err := LifecycleFromWire(wireID)
	require.NoError(t, err)
	require.Equal(t, id, decoded)

	t.Run("nil message refused", func(t *testing.T) {
		_, err := LifecycleFromWire(nil)
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("wrong length refused", func(t *testing.T) {
		_, err := LifecycleFromWire(&wire.LifecycleID{Value: []byte{1, 2, 3}})
		require.ErrorIs(t, err, ErrOutOfRange)
	})
}

// TestTabSelectorRoundTrip proves a tab selector survives an encode/decode
// cycle, that a nil message decodes as the zero selector (it composes into
// an optional stopped-tab field), and that every narrowing field is
// range-checked.
func TestTabSelectorRoundTrip(t *testing.T) {
	selector := domain.TabSelector{
		Kind: domain.TabSelectorByStableID, StableID: "tab-1",
		Ordinal: 3, RawName: "work", ExpectedCount: 4,
	}
	wireSelector := TabSelectorToWire(selector)
	decoded, err := TabSelectorFromWire(wireSelector)
	require.NoError(t, err)
	require.Equal(t, selector, decoded)

	t.Run("nil message tolerated", func(t *testing.T) {
		decoded, err := TabSelectorFromWire(nil)
		require.NoError(t, err)
		require.Equal(t, domain.TabSelector{}, decoded)
	})

	tests := []struct {
		name string
		msg  *wire.TabSelector
	}{
		{"kind out of range", &wire.TabSelector{Kind: 256}},
		{"ordinal out of range", &wire.TabSelector{Ordinal: 1 << 16}},
		{"expected count out of range", &wire.TabSelector{ExpectedCount: 1 << 16}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := TabSelectorFromWire(tt.msg)
			require.ErrorIs(t, err, ErrOutOfRange)
		})
	}
}

func testRemoteTarget() domain.RemoteSessionTarget {
	return domain.RemoteSessionTarget{
		Endpoint: "dev@host:22", DisplayOrigin: "dev@host",
		LifecycleID: domain.SessionLifecycleID{5, 6, 7},
		SessionName: "work", LiveTabID: "tab-1",
	}
}

// TestRemoteTargetRoundTrip proves a remote target survives an
// encode/decode cycle and that decode refuses a nil message and every
// nested narrowing failure.
func TestRemoteTargetRoundTrip(t *testing.T) {
	target := testRemoteTarget()
	wireTarget := RemoteTargetToWire(target)
	decoded, err := RemoteTargetFromWire(wireTarget)
	require.NoError(t, err)
	require.Equal(t, target, decoded)

	t.Run("nil message refused", func(t *testing.T) {
		_, err := RemoteTargetFromWire(nil)
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("invalid lifecycle refused", func(t *testing.T) {
		broken := RemoteTargetToWire(target)
		broken.LifecycleId = &wire.LifecycleID{Value: []byte{1}}
		_, err := RemoteTargetFromWire(broken)
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("invalid stopped tab refused", func(t *testing.T) {
		broken := RemoteTargetToWire(target)
		broken.StoppedTab.Kind = 256
		_, err := RemoteTargetFromWire(broken)
		require.ErrorIs(t, err, ErrOutOfRange)
	})
}

// TestRGBRoundTrip proves an RGB color survives an encode/decode cycle,
// that a nil message decodes as the zero color, and every component is
// range-checked.
func TestRGBRoundTrip(t *testing.T) {
	color := renderer.RGB{R: 10, G: 20, B: 30}
	wireColor := RGBToWire(color)
	decoded, err := RGBFromWire(wireColor)
	require.NoError(t, err)
	require.Equal(t, color, decoded)

	t.Run("nil message tolerated", func(t *testing.T) {
		decoded, err := RGBFromWire(nil)
		require.NoError(t, err)
		require.Equal(t, renderer.RGB{}, decoded)
	})

	tests := []struct {
		name string
		msg  *wire.RGB
	}{
		{"red out of range", &wire.RGB{R: 256}},
		{"green out of range", &wire.RGB{G: 256}},
		{"blue out of range", &wire.RGB{B: 256}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := RGBFromWire(tt.msg)
			require.ErrorIs(t, err, ErrOutOfRange)
		})
	}
}

func testStyle() renderer.Style {
	return renderer.Style{
		Bold: true, Italic: true, Attrs: renderer.AttrDim,
		Foreground: 1, Background: 2,
		UnderlineStyle: renderer.UnderlineSingle, UnderlineColor: 3, HasUnderlineColor: true,
		ForegroundRGB: renderer.RGB{R: 1}, BackgroundRGB: renderer.RGB{G: 1}, UnderlineColorRGB: renderer.RGB{B: 1},
	}
}

// TestCellStyleRoundTrip proves a cell style survives an encode/decode
// cycle, that a nil message decodes as the zero style, and every narrowing
// field is range-checked.
func TestCellStyleRoundTrip(t *testing.T) {
	style := testStyle()
	wireStyle := CellStyleToWire(style)
	decoded, err := CellStyleFromWire(wireStyle)
	require.NoError(t, err)
	require.Equal(t, style, decoded)

	t.Run("nil message tolerated", func(t *testing.T) {
		decoded, err := CellStyleFromWire(nil)
		require.NoError(t, err)
		require.Equal(t, renderer.Style{}, decoded)
	})

	tests := []struct {
		name string
		msg  *wire.CellStyle
	}{
		{"foreground out of range", &wire.CellStyle{Foreground: 1 << 20}},
		{"background out of range", &wire.CellStyle{Background: -(1 << 20)}},
		{"underline color out of range", &wire.CellStyle{UnderlineColor: 1 << 20}},
		{"underline style out of range", &wire.CellStyle{UnderlineStyle: 256}},
		{"attrs out of range", &wire.CellStyle{Attrs: 1 << 16}},
		{"foreground rgb out of range", &wire.CellStyle{ForegroundRgb: &wire.RGB{R: 256}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CellStyleFromWire(tt.msg)
			require.ErrorIs(t, err, ErrOutOfRange)
		})
	}
}

// TestPreviewCellRoundTrip proves a cell survives an encode/decode cycle,
// that decode refuses a nil message (unlike RGB/CellStyle, a single grid
// cell is never optional), and that an out-of-range rune is refused in
// both directions.
func TestPreviewCellRoundTrip(t *testing.T) {
	cell := renderer.Cell{Rune: 'A', Style: testStyle()}
	wireCell, err := PreviewCellToWire(cell)
	require.NoError(t, err)
	decoded, err := PreviewCellFromWire(wireCell)
	require.NoError(t, err)
	require.Equal(t, cell, decoded)

	t.Run("nil message refused", func(t *testing.T) {
		_, err := PreviewCellFromWire(nil)
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("encode rejects rune above max code point", func(t *testing.T) {
		_, err := PreviewCellToWire(renderer.Cell{Rune: 0x110000})
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("encode rejects negative rune", func(t *testing.T) {
		_, err := PreviewCellToWire(renderer.Cell{Rune: -1})
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("decode rejects rune above max code point", func(t *testing.T) {
		_, err := PreviewCellFromWire(&wire.PreviewCell{RuneValue: 0x110000})
		require.ErrorIs(t, err, ErrOutOfRange)
	})
}

// TestPreviewCellsRoundTrip proves a slice of cells survives an
// encode/decode cycle, that an empty/nil slice stays nil rather than an
// empty allocation, and that one bad cell fails the whole slice.
func TestPreviewCellsRoundTrip(t *testing.T) {
	cells := []renderer.Cell{{Rune: 'a'}, {Rune: 'b'}}
	wireCells, err := PreviewCellsToWire(cells)
	require.NoError(t, err)
	decoded, err := PreviewCellsFromWire(wireCells)
	require.NoError(t, err)
	require.Equal(t, cells, decoded)

	t.Run("nil in, nil out", func(t *testing.T) {
		out, err := PreviewCellsToWire(nil)
		require.NoError(t, err)
		require.Nil(t, out)
		decoded, err := PreviewCellsFromWire(nil)
		require.NoError(t, err)
		require.Nil(t, decoded)
	})

	t.Run("bad cell fails the slice on encode", func(t *testing.T) {
		_, err := PreviewCellsToWire([]renderer.Cell{{Rune: 'a'}, {Rune: 0x110000}})
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("nil member fails the slice on decode", func(t *testing.T) {
		_, err := PreviewCellsFromWire([]*wire.PreviewCell{{RuneValue: 'a'}, nil})
		require.ErrorIs(t, err, ErrOutOfRange)
	})
}

func testPreviewRequest() protocol.RemotePreviewRequest {
	return protocol.RemotePreviewRequest{
		Version: protocol.RemotePreviewSchemaVersion,
		Target:  testRemoteTarget(),
		Width:   2, Height: 1,
	}
}

// TestRemotePreviewRequestRoundTrip proves a remote preview request
// survives an encode/decode cycle byte-for-byte (via re-marshal), that
// decode refuses a nil message, and that both an invalid semantic value and
// an out-of-range wire field are refused with the right error shape.
func TestRemotePreviewRequestRoundTrip(t *testing.T) {
	request := testPreviewRequest()
	wireRequest, err := RemotePreviewRequestToWire(request)
	require.NoError(t, err)
	decoded, err := RemotePreviewRequestFromWire(wireRequest)
	require.NoError(t, err)
	require.Equal(t, request, decoded)

	t.Run("nil message refused", func(t *testing.T) {
		_, err := RemotePreviewRequestFromWire(nil)
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("encode refuses invalid semantic value", func(t *testing.T) {
		invalid := request
		invalid.Width = 0
		_, err := RemotePreviewRequestToWire(invalid)
		require.ErrorIs(t, err, protocol.ErrInvalidRemotePreviewRequest)
	})

	t.Run("decode refuses out-of-range wire field", func(t *testing.T) {
		broken, err := RemotePreviewRequestToWire(request)
		require.NoError(t, err)
		broken.Width = 1 << 16
		_, err = RemotePreviewRequestFromWire(broken)
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("decode re-validates the fully decoded request", func(t *testing.T) {
		broken, err := RemotePreviewRequestToWire(request)
		require.NoError(t, err)
		broken.Width = 0
		_, err = RemotePreviewRequestFromWire(broken)
		require.ErrorIs(t, err, protocol.ErrInvalidRemotePreviewRequest)
	})
}

func testRemotePreview() protocol.RemotePreview {
	target := testRemoteTarget()
	return protocol.RemotePreview{
		Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK,
		LifecycleID: target.LifecycleID, TabID: target.LiveTabID,
		Revision: 11, Width: 2, Height: 1,
		Cells: []renderer.Cell{{Rune: 'a'}, {Rune: 'b'}},
	}
}

// TestRemotePreviewRoundTrip proves a remote preview survives an
// encode/decode cycle, that decode refuses a nil message, and that a
// too-large preview is refused with protocol.ErrRemotePreviewTooLarge on
// both directions distinctly from an out-of-range wire field.
func TestRemotePreviewRoundTrip(t *testing.T) {
	preview := testRemotePreview()
	wirePreview, err := RemotePreviewToWire(preview)
	require.NoError(t, err)
	decoded, err := RemotePreviewFromWire(wirePreview)
	require.NoError(t, err)
	require.Equal(t, preview, decoded)

	t.Run("nil message refused", func(t *testing.T) {
		_, err := RemotePreviewFromWire(nil)
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("encode refuses a too-large preview", func(t *testing.T) {
		invalid := preview
		invalid.Cells = invalid.Cells[:1]
		_, err := RemotePreviewToWire(invalid)
		require.ErrorIs(t, err, protocol.ErrRemotePreviewTooLarge)
	})

	t.Run("decode refuses out-of-range wire field", func(t *testing.T) {
		broken, err := RemotePreviewToWire(preview)
		require.NoError(t, err)
		broken.Status = 1 << 16
		_, err = RemotePreviewFromWire(broken)
		require.ErrorIs(t, err, ErrOutOfRange)
	})

	t.Run("decode refuses a too-large preview", func(t *testing.T) {
		broken, err := RemotePreviewToWire(preview)
		require.NoError(t, err)
		broken.Cells = broken.Cells[:1]
		_, err = RemotePreviewFromWire(broken)
		require.ErrorIs(t, err, protocol.ErrRemotePreviewTooLarge)
	})

	t.Run("decode refuses cell encoding out of range", func(t *testing.T) {
		broken, err := RemotePreviewToWire(preview)
		require.NoError(t, err)
		broken.Cells[0].RuneValue = 0x110000
		_, err = RemotePreviewFromWire(broken)
		require.ErrorIs(t, err, ErrOutOfRange)
	})
}

// TestErrOutOfRangeIsDistinctFromProtocolSentinels proves protoconv never
// aliases its own range sentinel with a protocol validation sentinel, since
// callers (sessionwire, brokerwire) switch behavior on which one they see.
func TestErrOutOfRangeIsDistinctFromProtocolSentinels(t *testing.T) {
	require.False(t, errors.Is(ErrOutOfRange, protocol.ErrInvalidRemotePreview))
	require.False(t, errors.Is(ErrOutOfRange, protocol.ErrInvalidRemotePreviewRequest))
	require.False(t, errors.Is(ErrOutOfRange, protocol.ErrRemotePreviewTooLarge))
}
