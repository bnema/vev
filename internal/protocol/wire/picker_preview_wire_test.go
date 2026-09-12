package wire

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/protocol"
)

func pickerPreviewRow(runes ...rune) []renderer.Cell {
	cellWidth := 0
	for _, r := range runes {
		cellWidth += renderer.RuneWidth(r)
	}
	cells := make([]renderer.Cell, 0, cellWidth)
	for _, r := range runes {
		cells = append(cells, renderer.Cell{Rune: r})
		for range renderer.RuneWidth(r) - 1 {
			cells = append(cells, renderer.Cell{Continuation: true})
		}
	}
	return cells
}

func pickerPreviewRequestFixture() protocol.PickerPreviewRequest {
	return protocol.PickerPreviewRequest{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving", Key: "aa/first",
		Width: 80, Height: 24,
	}
}

func pickerPreviewFixture() protocol.PickerPreview {
	return protocol.PickerPreview{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving", Key: "aa/first",
		Status: protocol.PickerPreviewOK,
		Width:  4, Height: 2,
		Cells: append(pickerPreviewRow('a', '界', 'b'), pickerPreviewRow('c', 'd', 'e', 'f')...),
	}
}

func TestPickerPreviewRequestGoldenAndRoundTrip(t *testing.T) {
	request := pickerPreviewRequestFixture()
	want := []byte{
		0x00, 0x01, // schema version
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // interaction ID
		0x00, 0x07, 's', 'e', 'r', 'v', 'i', 'n', 'g',
		0x00, 0x08, 'a', 'a', '/', 'f', 'i', 'r', 's', 't',
		0x00, 0x50, 0x00, 0x18,
	}
	got := MarshalPickerPreviewRequest(request)
	if !bytes.Equal(got, want) {
		t.Fatalf("PickerPreviewRequest bytes = %x, want %x", got, want)
	}
	back, err := UnmarshalPickerPreviewRequest(got)
	if err != nil {
		t.Fatalf("UnmarshalPickerPreviewRequest() error = %v", err)
	}
	if !reflect.DeepEqual(back, request) {
		t.Fatalf("request = %+v, want %+v", back, request)
	}
	assertAllPrefixesFail(t, got, UnmarshalPickerPreviewRequest)
	assertTrailingGarbageFails(t, got, UnmarshalPickerPreviewRequest)
}

func TestPickerPreviewRequestRejectsInvalid(t *testing.T) {
	if MarshalPickerPreviewRequest(protocol.PickerPreviewRequest{Version: protocol.PickerPreviewSchemaVersion}) != nil {
		t.Fatal("MarshalPickerPreviewRequest accepted an invalid request")
	}
	tests := []struct {
		name   string
		mutate func(*protocol.PickerPreviewRequest)
	}{
		{name: "schema version", mutate: func(r *protocol.PickerPreviewRequest) { r.Version = 0 }},
		{name: "zero interaction", mutate: func(r *protocol.PickerPreviewRequest) { r.InteractionID = 0 }},
		{name: "zero size", mutate: func(r *protocol.PickerPreviewRequest) { r.Height = 0 }},
		{name: "oversized", mutate: func(r *protocol.PickerPreviewRequest) { r.Width = protocol.PickerPreviewMaxWidth + 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := pickerPreviewRequestFixture()
			tt.mutate(&request)
			payload := MarshalPickerPreviewRequest(request)
			if payload == nil {
				// The writer itself refuses to emit an invalid request.
				return
			}
			if _, err := UnmarshalPickerPreviewRequest(payload); !errors.Is(err, protocol.ErrInvalidPickerPreviewRequest) {
				t.Fatalf("UnmarshalPickerPreviewRequest() error = %v, want ErrInvalidPickerPreviewRequest", err)
			}
		})
	}
}

func TestPickerPreviewGoldenAndRoundTrip(t *testing.T) {
	preview := pickerPreviewFixture()
	got := MarshalPickerPreview(preview)
	if got == nil {
		t.Fatal("MarshalPickerPreview() = nil for a valid viewport")
	}
	back, err := UnmarshalPickerPreview(got)
	if err != nil {
		t.Fatalf("UnmarshalPickerPreview() error = %v", err)
	}
	if !reflect.DeepEqual(back, preview) {
		t.Fatalf("preview = %+v, want %+v", back, preview)
	}
	rows := back.FrameRows()
	if len(rows) != 2 || len(rows[0]) != 4 || rows[0][1].Rune != '界' || !rows[0][2].Continuation {
		t.Fatalf("FrameRows() = %+v, want the encoded double-width pair", rows)
	}
	assertAllPrefixesFail(t, got, UnmarshalPickerPreview)
	assertTrailingGarbageFails(t, got, UnmarshalPickerPreview)
}

func TestPickerPreviewRoundTripsStatusOnly(t *testing.T) {
	preview := protocol.PickerPreview{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving", Key: "aa/first",
		Status: protocol.PickerPreviewNoSuchTarget,
	}
	got := MarshalPickerPreview(preview)
	if got == nil {
		t.Fatal("MarshalPickerPreview() = nil for a status-only preview")
	}
	back, err := UnmarshalPickerPreview(got)
	if err != nil {
		t.Fatalf("UnmarshalPickerPreview() error = %v", err)
	}
	if !reflect.DeepEqual(back, preview) {
		t.Fatalf("preview = %+v, want %+v", back, preview)
	}
}

func TestPickerPreviewRejectsInvalid(t *testing.T) {
	valid := pickerPreviewFixture()
	if MarshalPickerPreview(protocol.PickerPreview{Version: protocol.PickerPreviewSchemaVersion}) != nil {
		t.Fatal("MarshalPickerPreview accepted a preview without a key")
	}

	// A viewport with a cell count that disagrees with its dimensions must not
	// decode, and the reader must reject the oversized count before allocating.
	truncatedCells := append([]byte(nil), MarshalPickerPreview(valid)...)
	countOffset := len(truncatedCells) - len(valid.Cells)*previewCellWireSize - 4
	for i := range 4 {
		truncatedCells[countOffset+i] = 0
	}
	if _, err := UnmarshalPickerPreview(truncatedCells); err == nil {
		t.Fatal("UnmarshalPickerPreview accepted an empty cell run for a sized viewport")
	}

	// An undefined cell flag bit is a picker-invalid payload, not a remote one.
	flagged := append([]byte(nil), MarshalPickerPreview(valid)...)
	flagOffset := len(flagged) - len(valid.Cells)*previewCellWireSize + 4
	flagged[flagOffset] = 0x80
	if _, err := UnmarshalPickerPreview(flagged); !errors.Is(err, protocol.ErrInvalidPickerPreview) {
		t.Fatalf("UnmarshalPickerPreview(flag byte) error = %v, want ErrInvalidPickerPreview", err)
	}

	// An oversized cell count fails closed before any allocation, and oversized
	// dimensions fail closed before the cell run is read.
	oversizedCount := append([]byte(nil), MarshalPickerPreview(valid)...)
	for i := range 4 {
		oversizedCount[countOffset+i] = 0xff
	}
	if _, err := UnmarshalPickerPreview(oversizedCount); !errors.Is(err, protocol.ErrInvalidPickerPreview) {
		t.Fatalf("UnmarshalPickerPreview(oversized count) error = %v, want ErrInvalidPickerPreview", err)
	}
	oversizedDims := append([]byte(nil), MarshalPickerPreview(valid)...)
	const widthOffset = 2 + 8 + 2 + 7 + 2 + 8 + 1
	oversizedDims[widthOffset], oversizedDims[widthOffset+1] = 0xff, 0xff
	if _, err := UnmarshalPickerPreview(oversizedDims); !errors.Is(err, protocol.ErrInvalidPickerPreview) {
		t.Fatalf("UnmarshalPickerPreview(oversized dims) error = %v, want ErrInvalidPickerPreview", err)
	}
	if _, err := UnmarshalPickerPreview(make([]byte, protocol.PickerPreviewMaxBytes+1)); !errors.Is(err, protocol.ErrInvalidPickerPreview) {
		t.Fatalf("UnmarshalPickerPreview(oversize payload) error = %v, want ErrInvalidPickerPreview", err)
	}
}

// TestPreviewCellCodecIsShared pins that both preview kinds encode a cell
// identically: one layout, one style block.
func TestPreviewCellCodecIsShared(t *testing.T) {
	cell := renderer.Cell{Rune: '界', Style: renderer.Style{Bold: true, Foreground: 3, HasForegroundRGB: true, ForegroundRGB: renderer.RGB{R: 1, G: 2, B: 3}}}
	var w payloadWriter
	putPreviewCell(&w, cell)
	r := payloadReader{b: w.b}
	back, err := getPreviewCell(&r)
	if err != nil {
		t.Fatalf("getPreviewCell() error = %v", err)
	}
	if back.Rune != cell.Rune || !back.Style.Bold || back.Style.ForegroundRGB != cell.Style.ForegroundRGB {
		t.Fatalf("cell = %+v, want %+v", back, cell)
	}
	if err := r.done(); err != nil {
		t.Fatalf("cell payload has trailing bytes: %v", err)
	}
}
