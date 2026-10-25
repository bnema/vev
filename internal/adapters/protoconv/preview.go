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
	"encoding/binary"

	renderer "github.com/bnema/vev-vt"
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
	styles := make([]*wire.CellStyle, 0)
	ids := make(map[renderer.Style]uint16)
	var runes, styleIDs []byte
	if len(message.Cells) > 0 {
		runes = make([]byte, len(message.Cells)*4)
		styleIDs = make([]byte, len(message.Cells)*2)
	}
	for i, cell := range message.Cells {
		id, ok := ids[cell.Style]
		if !ok {
			id = uint16(len(styles))
			ids[cell.Style] = id
			styles = append(styles, CellStyleToWire(cell.Style))
		}
		value := uint32(cell.Rune)
		if cell.Continuation {
			value = 0xFFFFFFFF
		}
		binary.LittleEndian.PutUint32(runes[i*4:], value)
		binary.LittleEndian.PutUint16(styleIDs[i*2:], id)
	}
	return &wire.RemotePreview{
		Version:     uint32(message.Version),
		Status:      uint32(message.Status),
		LifecycleId: LifecycleToWire(message.LifecycleID),
		TabId:       string(message.TabID),
		Revision:    message.Revision,
		Width:       uint32(message.Width),
		Height:      uint32(message.Height),
		Styles:      styles,
		Runes:       runes,
		StyleIds:    styleIDs,
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
	count := int(width) * int(height)
	if count > protocol.RemotePreviewMaxCells || len(message.GetStyles()) > protocol.RemotePreviewMaxStyles ||
		len(message.GetRunes()) != count*4 || len(message.GetStyleIds()) != count*2 ||
		(count > 0 && len(message.GetStyles()) == 0) ||
		(status != protocol.RemotePreviewOK && (len(message.GetStyles()) != 0 || count != 0)) {
		return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
	}
	styles := make([]renderer.Style, len(message.GetStyles()))
	for i, style := range message.GetStyles() {
		converted, err := CellStyleFromWire(style)
		if err != nil {
			return protocol.RemotePreview{}, err
		}
		styles[i] = converted
	}
	if count > 0 {
		preview.Cells = make([]renderer.Cell, count)
	}
	for i := range preview.Cells {
		id := int(binary.LittleEndian.Uint16(message.GetStyleIds()[i*2:]))
		if id >= len(styles) {
			return protocol.RemotePreview{}, protocol.ErrInvalidRemotePreview
		}
		value := binary.LittleEndian.Uint32(message.GetRunes()[i*4:])
		cell := renderer.Cell{Style: styles[id]}
		if value == 0xFFFFFFFF {
			cell.Continuation = true
		} else {
			if value > 0x10FFFF {
				return protocol.RemotePreview{}, ErrOutOfRange
			}
			cell.Rune = rune(value)
		}
		preview.Cells[i] = cell
	}
	if err := protocol.ValidateRemotePreview(preview); err != nil {
		return protocol.RemotePreview{}, err
	}
	return preview, nil
}
