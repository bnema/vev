package sessionwire

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func remotePreviewRequestToWire(message protocol.RemotePreviewRequest) (*wire.RemotePreviewRequest, error) {
	if err := protocol.ValidateRemotePreviewRequest(message); err != nil {
		return nil, err
	}
	width := uint32(message.Width)
	height := uint32(message.Height)
	version := uint32(message.Version)
	remote, err := remoteTargetToWire(&message.Target)
	if err != nil {
		return nil, err
	}
	return &wire.RemotePreviewRequest{Version: uint32(version), Target: remote, Width: width, Height: height}, nil
}

func remotePreviewRequestFromWire(message *wire.RemotePreviewRequest) (protocol.RemotePreviewRequest, error) {
	var request protocol.RemotePreviewRequest
	if message == nil {
		return request, protocol.ErrInvalidRemotePreviewRequest
	}
	version, err := mustUint16(message.GetVersion())
	if err != nil {
		return protocol.RemotePreviewRequest{}, protocol.ErrInvalidRemotePreviewRequest
	}
	request.Version = version
	remote, err := remoteTargetFromWire(message.GetTarget())
	if err != nil || remote == nil {
		return protocol.RemotePreviewRequest{}, protocol.ErrInvalidRemotePreviewRequest
	}
	request.Target = *remote
	width, err := mustUint16(message.GetWidth())
	if err != nil {
		return protocol.RemotePreviewRequest{}, protocol.ErrInvalidRemotePreviewRequest
	}
	height, err := mustUint16(message.GetHeight())
	if err != nil {
		return protocol.RemotePreviewRequest{}, protocol.ErrInvalidRemotePreviewRequest
	}
	request.Width = width
	request.Height = height
	if err := protocol.ValidateRemotePreviewRequest(request); err != nil {
		return protocol.RemotePreviewRequest{}, err
	}
	return request, nil
}

func remotePreviewToWire(message protocol.RemotePreview) (*wire.RemotePreview, error) {
	if err := protocol.ValidateRemotePreview(message); err != nil {
		return nil, err
	}
	lifecycle := lifecycleToWire(message.LifecycleID)
	cells, err := previewCellsToWire(message.Cells)
	if err != nil {
		return nil, err
	}
	return &wire.RemotePreview{
		Version:     uint32(message.Version),
		Status:      uint32(message.Status),
		LifecycleId: lifecycle,
		TabId:       string(message.TabID),
		Revision:    message.Revision,
		Width:       uint32(message.Width),
		Height:      uint32(message.Height),
		Cells:       cells,
	}, nil
}

func remotePreviewFromWire(message *wire.RemotePreview) (protocol.RemotePreview, error) {
	var preview protocol.RemotePreview
	if message == nil {
		return preview, protocol.ErrInvalidRemotePreview
	}
	version, err := mustUint16(message.GetVersion())
	if err != nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	preview.Version = version
	preview.Status, err = enum8[protocol.RemotePreviewStatus](message.GetStatus())
	if err != nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	lifecycle, err := lifecycleFromWire(message.GetLifecycleId())
	if err != nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	preview.LifecycleID = lifecycle
	preview.TabID = domain.TabStableID(message.GetTabId())
	preview.Revision = message.GetRevision()
	previewWidth, err := mustUint16(message.GetWidth())
	if err != nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	previewHeight, err := mustUint16(message.GetHeight())
	if err != nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	preview.Width = previewWidth
	preview.Height = previewHeight
	cells, err := previewCellsFromWire(message.GetCells())
	if err != nil {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	preview.Cells = cells
	if err := protocol.ValidateRemotePreview(preview); err != nil {
		return protocol.RemotePreview{}, err
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
