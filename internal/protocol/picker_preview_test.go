package protocol

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
)

func pickerPreviewCells(width uint16) []renderer.Cell {
	cells := make([]renderer.Cell, width)
	for i := range cells {
		cells[i] = renderer.Cell{Rune: 'x'}
	}
	return cells
}

func TestValidatePickerPreviewRequest(t *testing.T) {
	valid := PickerPreviewRequest{
		Version: PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving", Key: "aa/first",
		Width: 80, Height: 24,
	}
	tests := []struct {
		name    string
		mutate  func(*PickerPreviewRequest)
		wantErr bool
	}{
		{name: "valid"},
		{name: "schema version", mutate: func(r *PickerPreviewRequest) { r.Version = 2 }, wantErr: true},
		{name: "zero interaction", mutate: func(r *PickerPreviewRequest) { r.InteractionID = 0 }, wantErr: true},
		{name: "empty source", mutate: func(r *PickerPreviewRequest) { r.SourceID = "" }, wantErr: true},
		{name: "control source", mutate: func(r *PickerPreviewRequest) { r.SourceID = "serv\x1bing" }, wantErr: true},
		{name: "empty key", mutate: func(r *PickerPreviewRequest) { r.Key = "" }, wantErr: true},
		{name: "control key", mutate: func(r *PickerPreviewRequest) { r.Key = "aa\x1bfirst" }, wantErr: true},
		{name: "zero width", mutate: func(r *PickerPreviewRequest) { r.Width = 0 }, wantErr: true},
		{name: "zero height", mutate: func(r *PickerPreviewRequest) { r.Height = 0 }, wantErr: true},
		{name: "width over bound", mutate: func(r *PickerPreviewRequest) { r.Width = PickerPreviewMaxWidth + 1 }, wantErr: true},
		{name: "height over bound", mutate: func(r *PickerPreviewRequest) { r.Height = PickerPreviewMaxHeight + 1 }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := valid
			if tt.mutate != nil {
				tt.mutate(&request)
			}
			err := ValidatePickerPreviewRequest(request)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidatePickerPreviewRequest() = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestValidatePickerPreview(t *testing.T) {
	valid := PickerPreview{
		Version: PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving", Key: "aa/first",
		Status: PickerPreviewOK, Width: 2, Height: 1, Cells: pickerPreviewCells(2),
	}
	wide := PickerPreview{
		Version: PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving", Key: "aa/first",
		Status: PickerPreviewOK,
		Width:  2, Height: 1,
		Cells: []renderer.Cell{{Rune: '界'}, {Continuation: true}},
	}
	tests := []struct {
		name    string
		preview PickerPreview
		wantErr bool
	}{
		{name: "valid", preview: valid},
		{name: "double width continuation", preview: wide},
		{name: "schema version", preview: func() PickerPreview { p := valid; p.Version = 2; return p }(), wantErr: true},
		{name: "zero interaction", preview: func() PickerPreview { p := valid; p.InteractionID = 0; return p }(), wantErr: true},
		{name: "empty key", preview: func() PickerPreview { p := valid; p.Key = ""; return p }(), wantErr: true},
		{name: "unknown status", preview: func() PickerPreview { p := valid; p.Status = 99; return p }(), wantErr: true},
		{name: "unavailable carries a viewport", preview: func() PickerPreview {
			p := valid
			p.Status = PickerPreviewUnavailable
			return p
		}(), wantErr: true},
		{name: "unavailable empty", preview: PickerPreview{
			Version: PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving", Key: "aa/first",
			Status: PickerPreviewNoSuchTarget,
		}},
		{name: "zero width", preview: func() PickerPreview {
			p := valid
			p.Width = 0
			return p
		}(), wantErr: true},
		{name: "cell count mismatch", preview: func() PickerPreview {
			p := valid
			p.Cells = pickerPreviewCells(1)
			return p
		}(), wantErr: true},
		{name: "control rune", preview: func() PickerPreview {
			p := valid
			p.Cells = []renderer.Cell{{Rune: '\x1b'}, {Rune: 'x'}}
			return p
		}(), wantErr: true},
		{name: "orphan continuation", preview: func() PickerPreview {
			p := valid
			p.Cells = []renderer.Cell{{Continuation: true}, {Rune: 'x'}}
			return p
		}(), wantErr: true},
		{name: "unpaired double width", preview: func() PickerPreview {
			p := valid
			p.Cells = []renderer.Cell{{Rune: '界'}, {Rune: 'x'}}
			return p
		}(), wantErr: true},
		{name: "unsupported style", preview: func() PickerPreview {
			p := valid
			p.Cells = []renderer.Cell{{Rune: 'x', Style: renderer.Style{Attrs: 1 << 7, UnderlineStyle: renderer.UnderlineDashed + 1}}, {Rune: 'x'}}
			return p
		}(), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePickerPreview(tt.preview)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidatePickerPreview() = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestPickerPreviewFrameRows(t *testing.T) {
	preview := PickerPreview{
		Version: PickerPreviewSchemaVersion, InteractionID: 7, SourceID: "serving", Key: "aa/first",
		Status: PickerPreviewOK, Width: 2, Height: 2, Cells: pickerPreviewCells(4),
	}
	rows := preview.FrameRows()
	if len(rows) != 2 || len(rows[0]) != 2 || rows[0][0].Rune != 'x' {
		t.Fatalf("FrameRows() = %+v, want two 2-cell rows", rows)
	}
	rows[0][0].Rune = 'y'
	if preview.Cells[0].Rune != 'x' {
		t.Fatal("FrameRows must copy the cells, not alias them")
	}
	empty := PickerPreview{Version: PickerPreviewSchemaVersion, Status: PickerPreviewUnavailable}
	if rows := empty.FrameRows(); rows != nil {
		t.Fatalf("FrameRows() without a viewport = %+v, want nil", rows)
	}
}
