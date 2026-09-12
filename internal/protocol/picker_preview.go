package protocol

import (
	"errors"
	"unicode"
	"unicode/utf8"

	renderer "github.com/bnema/vev-vt"
)

// PickerPreviewSchemaVersion versions the picker preview payload independently
// from the attachment protocol version.
const PickerPreviewSchemaVersion uint16 = 1

// PickerPreviewStatus reports whether a preview carries a viewport.
type PickerPreviewStatus uint8

const (
	PickerPreviewOK PickerPreviewStatus = iota
	// PickerPreviewNoSuchTarget means the key is no longer part of the
	// interaction: the row was retired, replaced, or the revision is stale.
	PickerPreviewNoSuchTarget
	// PickerPreviewUnavailable means the row exists but its content cannot be
	// captured: a stopped session, no renderable tab, or a failed remote fetch.
	PickerPreviewUnavailable
	PickerPreviewTooLarge
)

// PickerPreviewRequest asks the serving daemon for the preview of one displayed
// row. The client debounces its own cursor movement before sending it; the
// daemon registers that row as the viewer's preview source and pushes a new
// PickerPreview whenever the row's content changes.
type PickerPreviewRequest struct {
	Version       uint16
	InteractionID uint64
	SourceID      string
	Key           string
	Width         uint16
	Height        uint16
}

// PickerPreview is the bounded row-major styled-cell viewport of one picker
// row. It is never persisted, logged, or traced.
type PickerPreview struct {
	Version       uint16
	InteractionID uint64
	SourceID      string
	Key           string
	Status        PickerPreviewStatus
	Width         uint16
	Height        uint16
	Cells         []renderer.Cell
}

// Preview bounds are shared with the remote preview: both publish the same
// bounded viewport shape, only one is fetched from a remote daemon.
const (
	PickerPreviewMaxWidth  = RemotePreviewMaxWidth
	PickerPreviewMaxHeight = RemotePreviewMaxHeight
	PickerPreviewMaxCells  = RemotePreviewMaxCells
	PickerPreviewMaxBytes  = RemotePreviewMaxBytes
)

var (
	ErrInvalidPickerPreviewRequest = errors.New("vev: invalid picker preview request")
	ErrInvalidPickerPreview        = errors.New("vev: invalid picker preview")
)

func ValidatePickerPreviewRequest(request PickerPreviewRequest) error {
	if request.Version != PickerPreviewSchemaVersion || request.InteractionID == 0 {
		return ErrInvalidPickerPreviewRequest
	}
	if !validPickerSourceID(request.SourceID) || !validPickerKey(request.Key) {
		return ErrInvalidPickerPreviewRequest
	}
	if request.Width == 0 || request.Height == 0 ||
		int(request.Width) > PickerPreviewMaxWidth || int(request.Height) > PickerPreviewMaxHeight {
		return ErrInvalidPickerPreviewRequest
	}
	return nil
}

func ValidatePickerPreview(preview PickerPreview) error {
	if preview.Version != PickerPreviewSchemaVersion || preview.Status > PickerPreviewTooLarge {
		return ErrInvalidPickerPreview
	}
	if preview.InteractionID == 0 || !validPickerSourceID(preview.SourceID) || !validPickerKey(preview.Key) {
		return ErrInvalidPickerPreview
	}
	if preview.Status != PickerPreviewOK {
		if preview.Width != 0 || preview.Height != 0 || len(preview.Cells) != 0 {
			return ErrInvalidPickerPreview
		}
		return nil
	}
	return ValidatePickerPreviewViewport(preview.Width, preview.Height, preview.Cells)
}

// ValidatePickerPreviewViewport checks only the viewport shape, so a capture can
// be validated before its interaction identity is stamped onto it.
func ValidatePickerPreviewViewport(width, height uint16, cells []renderer.Cell) error {
	if width == 0 || height == 0 ||
		int(width) > PickerPreviewMaxWidth || int(height) > PickerPreviewMaxHeight {
		return ErrInvalidPickerPreview
	}
	want := int(width) * int(height)
	if want > PickerPreviewMaxCells || len(cells) != want {
		return ErrInvalidPickerPreview
	}
	if !previewCellRunValid(int(width), cells) {
		return ErrInvalidPickerPreview
	}
	return nil
}

func (p PickerPreview) FrameRows() [][]renderer.Cell {
	if p.Width == 0 || p.Height == 0 || len(p.Cells) != int(p.Width)*int(p.Height) {
		return nil
	}
	rows := make([][]renderer.Cell, p.Height)
	width := int(p.Width)
	for y := range rows {
		rows[y] = append([]renderer.Cell(nil), p.Cells[y*width:(y+1)*width]...)
	}
	return rows
}

// previewCellRunValid reports whether cells form the row-major run of a
// width-wide viewport: every rune and style is displayable, and each
// continuation column follows a double-width cell on the same row.
func previewCellRunValid(width int, cells []renderer.Cell) bool {
	if width <= 0 {
		return false
	}
	for i, cell := range cells {
		if !utf8.ValidRune(cell.Rune) || (cell.Rune != 0 && unicode.IsControl(cell.Rune)) || !validPreviewCellStyle(cell.Style) {
			return false
		}
		rowStart := (i / width) * width
		rowEnd := rowStart + width
		if cell.Continuation {
			if cell.Rune != 0 || i == rowStart || cells[i-1].Continuation || renderer.RuneWidth(cells[i-1].Rune) != 2 {
				return false
			}
		} else if renderer.RuneWidth(cell.Rune) == 2 && (i+1 >= rowEnd || !cells[i+1].Continuation) {
			return false
		}
	}
	return true
}
