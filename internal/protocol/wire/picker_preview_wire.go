package wire

import (
	"errors"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/protocol"
)

// This file carries the picker preview pair: the client's request for the
// preview of one displayed row, and the serving daemon's bounded viewport of
// that row. It reuses the remote preview's cell layout and style codec so both
// preview kinds encode identically.

func MarshalPickerPreviewRequest(request protocol.PickerPreviewRequest) []byte {
	if protocol.ValidatePickerPreviewRequest(request) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint16(request.Version)
	w.putUint64(request.InteractionID)
	w.putString(request.SourceID)
	w.putString(request.Key)
	w.putUint16(request.Width)
	w.putUint16(request.Height)
	return w.b
}

func UnmarshalPickerPreviewRequest(data []byte) (protocol.PickerPreviewRequest, error) {
	var request protocol.PickerPreviewRequest
	if len(data) > protocol.PickerPreviewMaxBytes {
		return request, protocol.ErrInvalidPickerPreviewRequest
	}
	r := payloadReader{b: data}
	var err error
	if request.Version, err = r.getUint16(); err != nil {
		return request, err
	}
	if request.InteractionID, err = r.getUint64(); err != nil {
		return request, err
	}
	if request.SourceID, err = r.getString(); err != nil {
		return request, err
	}
	if request.Key, err = r.getString(); err != nil {
		return request, err
	}
	if request.Width, err = r.getUint16(); err != nil {
		return request, err
	}
	if request.Height, err = r.getUint16(); err != nil {
		return request, err
	}
	if err := r.done(); err != nil {
		return request, err
	}
	if err := protocol.ValidatePickerPreviewRequest(request); err != nil {
		return request, err
	}
	return request, nil
}

func MarshalPickerPreview(preview protocol.PickerPreview) []byte {
	if protocol.ValidatePickerPreview(preview) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint16(preview.Version)
	w.putUint64(preview.InteractionID)
	w.putString(preview.SourceID)
	w.putString(preview.Key)
	w.putUint8(uint8(preview.Status))
	w.putUint16(preview.Width)
	w.putUint16(preview.Height)
	w.putUint32(uint32(len(preview.Cells)))
	for _, cell := range preview.Cells {
		putPreviewCell(&w, cell)
	}
	return w.b
}

func UnmarshalPickerPreview(data []byte) (protocol.PickerPreview, error) {
	var preview protocol.PickerPreview
	if len(data) > protocol.PickerPreviewMaxBytes {
		return preview, protocol.ErrInvalidPickerPreview
	}
	r := payloadReader{b: data}
	var err error
	if preview.Version, err = r.getUint16(); err != nil {
		return preview, err
	}
	if preview.InteractionID, err = r.getUint64(); err != nil {
		return preview, err
	}
	if preview.SourceID, err = r.getString(); err != nil {
		return preview, err
	}
	if preview.Key, err = r.getString(); err != nil {
		return preview, err
	}
	status, err := r.getUint8()
	if err != nil {
		return preview, err
	}
	preview.Status = protocol.PickerPreviewStatus(status)
	if preview.Width, err = r.getUint16(); err != nil {
		return preview, err
	}
	if preview.Height, err = r.getUint16(); err != nil {
		return preview, err
	}
	count, err := r.getUint32()
	if err != nil {
		return preview, err
	}
	if count > protocol.PickerPreviewMaxCells || uint64(count) > uint64(len(r.b)/previewCellWireSize) {
		return preview, protocol.ErrInvalidPickerPreview
	}
	if count > 0 {
		preview.Cells = make([]renderer.Cell, 0, count)
		for range count {
			cell, cellErr := getPreviewCell(&r)
			if cellErr != nil {
				if errors.Is(cellErr, errPreviewCellFlags) {
					return preview, protocol.ErrInvalidPickerPreview
				}
				return preview, cellErr
			}
			preview.Cells = append(preview.Cells, cell)
		}
	}
	if err := r.done(); err != nil {
		return preview, err
	}
	if err := protocol.ValidatePickerPreview(preview); err != nil {
		return preview, err
	}
	return preview, nil
}

// errPreviewCellFlags reports a cell whose flag byte carries a bit this
// protocol does not define. Each preview kind maps it to its own error.
var errPreviewCellFlags = errors.New("wire: unsupported preview cell flags")

// putPreviewCell and getPreviewCell are the shared cell layout of both preview
// kinds: rune, continuation flag, then the bounded style block.
func putPreviewCell(w *payloadWriter, cell renderer.Cell) {
	var flags uint8
	if cell.Continuation {
		flags = 1
	}
	w.putUint32(uint32(cell.Rune))
	w.putUint8(flags)
	putPreviewStyle(w, cell.Style)
}

func getPreviewCell(r *payloadReader) (renderer.Cell, error) {
	runeValue, err := r.getUint32()
	if err != nil {
		return renderer.Cell{}, err
	}
	flags, err := r.getUint8()
	if err != nil {
		return renderer.Cell{}, err
	}
	if flags&^uint8(1) != 0 {
		return renderer.Cell{}, errPreviewCellFlags
	}
	style, err := getPreviewStyle(r)
	if err != nil {
		return renderer.Cell{}, err
	}
	return renderer.Cell{Rune: rune(runeValue), Continuation: flags&1 != 0, Style: style}, nil
}
