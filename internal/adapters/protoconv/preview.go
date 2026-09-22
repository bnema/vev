package protoconv

// RemotePreviewRequest/RemotePreview converters (shared by sessionwire and
// brokerwire).
//
// On failure these return either ErrOutOfRange (from a narrowing numeric or
// identity conversion) or one of protocol's own RemotePreview* validation
// sentinels, unwrapped. The two callers diverge on how they collapse these
// into their own local sentinel errors, so this package never wraps or
// renames them: sessionwire always folds every failure into a single fixed
// request/preview sentinel, while brokerwire keeps range failures distinct
// from semantic validation failures (and maps ErrRemotePreviewTooLarge to
// its own bounded-size sentinel). See the callers' wrapper functions for the
// exact mapping.

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// RemotePreviewRequestToWire validates before encoding: an invalid request
// is refused with protocol's own sentinel rather than emitted.
func RemotePreviewRequestToWire(message protocol.RemotePreviewRequest) (*wire.RemotePreviewRequest, error) {
	if err := protocol.ValidateRemotePreviewRequest(message); err != nil {
		return nil, err
	}
	return &wire.RemotePreviewRequest{
		Version: uint32(message.Version),
		Target:  RemoteTargetToWire(message.Target),
		Width:   uint32(message.Width),
		Height:  uint32(message.Height),
	}, nil
}

// RemotePreviewRequestFromWire requires a non-nil message and re-validates
// the fully decoded request, so decode refuses exactly what encode refuses.
func RemotePreviewRequestFromWire(message *wire.RemotePreviewRequest) (protocol.RemotePreviewRequest, error) {
	var request protocol.RemotePreviewRequest
	if message == nil {
		return request, ErrOutOfRange
	}
	version, err := Uint16[uint16](message.GetVersion())
	if err != nil {
		return protocol.RemotePreviewRequest{}, err
	}
	request.Version = version
	target, err := RemoteTargetFromWire(message.GetTarget())
	if err != nil {
		return protocol.RemotePreviewRequest{}, err
	}
	request.Target = target
	width, err := Uint16[uint16](message.GetWidth())
	if err != nil {
		return protocol.RemotePreviewRequest{}, err
	}
	request.Width = width
	height, err := Uint16[uint16](message.GetHeight())
	if err != nil {
		return protocol.RemotePreviewRequest{}, err
	}
	request.Height = height
	if err := protocol.ValidateRemotePreviewRequest(request); err != nil {
		return protocol.RemotePreviewRequest{}, err
	}
	return request, nil
}

// RemotePreviewToWire validates before encoding. A failure can be either
// protocol.ErrInvalidRemotePreview or protocol.ErrRemotePreviewTooLarge;
// callers distinguish the two.
func RemotePreviewToWire(message protocol.RemotePreview) (*wire.RemotePreview, error) {
	if err := protocol.ValidateRemotePreview(message); err != nil {
		return nil, err
	}
	cells, err := PreviewCellsToWire(message.Cells)
	if err != nil {
		return nil, err
	}
	return &wire.RemotePreview{
		Version:     uint32(message.Version),
		Status:      uint32(message.Status),
		LifecycleId: LifecycleToWire(message.LifecycleID),
		TabId:       string(message.TabID),
		Revision:    message.Revision,
		Width:       uint32(message.Width),
		Height:      uint32(message.Height),
		Cells:       cells,
	}, nil
}

// RemotePreviewFromWire requires a non-nil message and re-validates the
// fully decoded preview.
func RemotePreviewFromWire(message *wire.RemotePreview) (protocol.RemotePreview, error) {
	var preview protocol.RemotePreview
	if message == nil {
		return preview, ErrOutOfRange
	}
	version, err := Uint16[uint16](message.GetVersion())
	if err != nil {
		return protocol.RemotePreview{}, err
	}
	preview.Version = version
	status, err := Uint8[protocol.RemotePreviewStatus](message.GetStatus())
	if err != nil {
		return protocol.RemotePreview{}, err
	}
	preview.Status = status
	lifecycle, err := LifecycleFromWire(message.GetLifecycleId())
	if err != nil {
		return protocol.RemotePreview{}, err
	}
	preview.LifecycleID = lifecycle
	preview.TabID = domain.TabStableID(message.GetTabId())
	preview.Revision = message.GetRevision()
	width, err := Uint16[uint16](message.GetWidth())
	if err != nil {
		return protocol.RemotePreview{}, err
	}
	preview.Width = width
	height, err := Uint16[uint16](message.GetHeight())
	if err != nil {
		return protocol.RemotePreview{}, err
	}
	preview.Height = height
	cells, err := PreviewCellsFromWire(message.GetCells())
	if err != nil {
		return protocol.RemotePreview{}, err
	}
	preview.Cells = cells
	if err := protocol.ValidateRemotePreview(preview); err != nil {
		return protocol.RemotePreview{}, err
	}
	return preview, nil
}
