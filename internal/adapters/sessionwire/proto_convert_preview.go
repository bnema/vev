package sessionwire

// RemotePreviewRequest/RemotePreview converters delegate their field-by-field
// logic to internal/adapters/protoconv, the canonical implementation shared
// with brokerwire. sessionwire's policy is to collapse every failure -
// whether a narrowing range failure or a semantic validation failure - into
// one fixed sentinel per direction (protocol.ErrInvalidRemotePreviewRequest
// or protocol.ErrInvalidRemotePreview), except that remotePreviewToWire
// still distinguishes protoconv's own ErrOutOfRange (encoded as
// errProtoConvertRange) from a protocol validation failure (passed through
// unchanged), matching this file's pre-extraction behavior exactly.

import (
	"errors"

	"github.com/bnema/vev/internal/adapters/protoconv"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func remotePreviewRequestToWire(message protocol.RemotePreviewRequest) (*wire.RemotePreviewRequest, error) {
	out, err := protoconv.RemotePreviewRequestToWire(message)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func remotePreviewRequestFromWire(message *wire.RemotePreviewRequest) (protocol.RemotePreviewRequest, error) {
	if message == nil {
		return protocol.RemotePreviewRequest{}, protocol.ErrInvalidRemotePreviewRequest
	}
	request, err := protoconv.RemotePreviewRequestFromWire(message)
	if err != nil {
		return protocol.RemotePreviewRequest{}, protocol.ErrInvalidRemotePreviewRequest
	}
	return request, nil
}

func remotePreviewToWire(message protocol.RemotePreview) (*wire.RemotePreview, error) {
	out, err := protoconv.RemotePreviewToWire(message)
	if err != nil {
		if errors.Is(err, protoconv.ErrOutOfRange) {
			return nil, errProtoConvertRange
		}
		return nil, err
	}
	return out, nil
}

func remotePreviewFromWire(message *wire.RemotePreview) (protocol.RemotePreview, error) {
	if message == nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	preview, err := protoconv.RemotePreviewFromWire(message)
	if err != nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	return preview, nil
}

func pickerPreviewRequestToWire(message protocol.PickerPreviewRequest) (*wire.PickerPreviewRequest, error) {
	if err := protocol.ValidatePickerPreviewRequest(message); err != nil {
		return nil, err
	}
	return &wire.PickerPreviewRequest{
		Version:       uint32(message.Version),
		InteractionId: message.InteractionID,
		SourceId:      message.SourceID,
		Key:           message.Key,
		Width:         uint32(message.Width),
		Height:        uint32(message.Height),
	}, nil
}

func pickerPreviewRequestFromWire(message *wire.PickerPreviewRequest) (protocol.PickerPreviewRequest, error) {
	var request protocol.PickerPreviewRequest
	if message == nil {
		return request, protocol.ErrInvalidPickerPreviewRequest
	}
	version, err := mustUint16(message.GetVersion())
	if err != nil {
		return protocol.PickerPreviewRequest{}, protocol.ErrInvalidPickerPreviewRequest
	}
	width, err := mustUint16(message.GetWidth())
	if err != nil {
		return protocol.PickerPreviewRequest{}, protocol.ErrInvalidPickerPreviewRequest
	}
	height, err := mustUint16(message.GetHeight())
	if err != nil {
		return protocol.PickerPreviewRequest{}, protocol.ErrInvalidPickerPreviewRequest
	}
	request = protocol.PickerPreviewRequest{
		Version:       version,
		InteractionID: message.GetInteractionId(),
		SourceID:      message.GetSourceId(),
		Key:           message.GetKey(),
		Width:         width,
		Height:        height,
	}
	if err := protocol.ValidatePickerPreviewRequest(request); err != nil {
		return protocol.PickerPreviewRequest{}, err
	}
	return request, nil
}

func pickerPreviewToWire(message protocol.PickerPreview) (*wire.PickerPreview, error) {
	if err := protocol.ValidatePickerPreview(message); err != nil {
		return nil, err
	}
	cells, err := previewCellsToWire(message.Cells)
	if err != nil {
		return nil, err
	}
	return &wire.PickerPreview{
		Version:       uint32(message.Version),
		InteractionId: message.InteractionID,
		SourceId:      message.SourceID,
		Key:           message.Key,
		Status:        uint32(message.Status),
		Width:         uint32(message.Width),
		Height:        uint32(message.Height),
		Cells:         cells,
	}, nil
}

func pickerPreviewFromWire(message *wire.PickerPreview) (protocol.PickerPreview, error) {
	var preview protocol.PickerPreview
	if message == nil {
		return preview, protocol.ErrInvalidPickerPreview
	}
	version, err := mustUint16(message.GetVersion())
	if err != nil {
		return protocol.PickerPreview{}, protocol.ErrInvalidPickerPreview
	}
	width, err := mustUint16(message.GetWidth())
	if err != nil {
		return protocol.PickerPreview{}, protocol.ErrInvalidPickerPreview
	}
	height, err := mustUint16(message.GetHeight())
	if err != nil {
		return protocol.PickerPreview{}, protocol.ErrInvalidPickerPreview
	}
	status, err := enum8[protocol.PickerPreviewStatus](message.GetStatus())
	if err != nil {
		return protocol.PickerPreview{}, protocol.ErrInvalidPickerPreview
	}
	preview = protocol.PickerPreview{
		Version:       version,
		InteractionID: message.GetInteractionId(),
		SourceID:      message.GetSourceId(),
		Key:           message.GetKey(),
		Status:        status,
		Width:         width,
		Height:        height,
	}
	cells, err := previewCellsFromWire(message.GetCells())
	if err != nil {
		return protocol.PickerPreview{}, protocol.ErrInvalidPickerPreview
	}
	preview.Cells = cells
	if err := protocol.ValidatePickerPreview(preview); err != nil {
		return protocol.PickerPreview{}, err
	}
	return preview, nil
}
