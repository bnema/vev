package brokerwire

// Preview wire converters (broker preview slice 1).
//
// StartPreview/CancelPreview travel client-to-server; PreviewPublication
// travels server-to-client with exactly one of its preview/error result
// members set. Conversion mirrors the port preview types losslessly and
// validates every narrowing numeric cast before it happens, following the
// shared converter patterns in convert.go: wire uint32 taxonomy fields
// never truncate into a smaller semantic enum, identity lengths are exact,
// and semantic validation failures map to ErrInvalidMessage.
//
// The preview viewport (RemotePreviewRequest/RemotePreview) reuses the
// terminal wire shapes owned by the session conversation; the broker
// conversation carries them as nested messages under its own envelope
// tags. Repeated authority is scope/generation plus the preview target
// identity response; route/request dimensions are retained by client
// subscription state and re-checked here through the ports preview
// contract.

import (
	"errors"
	"math"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func previewLifecycleToWire(id domain.SessionLifecycleID) *wire.LifecycleID {
	value := append([]byte(nil), id[:]...)
	return &wire.LifecycleID{Value: value}
}

func previewLifecycleFromWire(message *wire.LifecycleID) (domain.SessionLifecycleID, error) {
	var id domain.SessionLifecycleID
	if message == nil || len(message.GetValue()) != len(id) {
		return id, errConvertRange
	}
	copy(id[:], message.GetValue())
	return id, nil
}

func previewTabSelectorToWire(selector domain.TabSelector) *wire.TabSelector {
	return &wire.TabSelector{
		Kind:          uint32(selector.Kind),
		StableId:      string(selector.StableID),
		Ordinal:       uint32(selector.Ordinal),
		RawName:       selector.RawName,
		ExpectedCount: uint32(selector.ExpectedCount),
	}
}

func previewTabSelectorFromWire(message *wire.TabSelector) (domain.TabSelector, error) {
	var selector domain.TabSelector
	if message == nil {
		return selector, nil
	}
	kind, err := brokerEnum8[domain.TabSelectorKind](message.GetKind())
	if err != nil {
		return domain.TabSelector{}, err
	}
	selector.Kind = kind
	selector.StableID = domain.TabStableID(message.GetStableId())
	ordinal, err := brokerEnum16[uint16](message.GetOrdinal())
	if err != nil {
		return domain.TabSelector{}, err
	}
	selector.Ordinal = ordinal
	selector.RawName = message.GetRawName()
	expected, err := brokerEnum16[uint16](message.GetExpectedCount())
	if err != nil {
		return domain.TabSelector{}, err
	}
	selector.ExpectedCount = expected
	return selector, nil
}

func previewRemoteTargetToWire(target domain.RemoteSessionTarget) (*wire.RemoteTarget, error) {
	lifecycle := previewLifecycleToWire(target.LifecycleID)
	return &wire.RemoteTarget{
		Endpoint:      target.Endpoint,
		DisplayOrigin: target.DisplayOrigin,
		LifecycleId:   lifecycle,
		SessionName:   target.SessionName,
		LiveTabId:     string(target.LiveTabID),
		StoppedTab:    previewTabSelectorToWire(target.StoppedTab),
		Stopped:       target.Stopped,
	}, nil
}

func previewRemoteTargetFromWire(message *wire.RemoteTarget) (domain.RemoteSessionTarget, error) {
	var target domain.RemoteSessionTarget
	if message == nil {
		return target, errConvertRange
	}
	lifecycle, err := previewLifecycleFromWire(message.GetLifecycleId())
	if err != nil {
		return domain.RemoteSessionTarget{}, err
	}
	stopped, err := previewTabSelectorFromWire(message.GetStoppedTab())
	if err != nil {
		return domain.RemoteSessionTarget{}, err
	}
	return domain.RemoteSessionTarget{
		Endpoint:      message.GetEndpoint(),
		DisplayOrigin: message.GetDisplayOrigin(),
		LifecycleID:   lifecycle,
		SessionName:   message.GetSessionName(),
		LiveTabID:     domain.TabStableID(message.GetLiveTabId()),
		StoppedTab:    stopped,
		Stopped:       message.GetStopped(),
	}, nil
}

func previewRGBToWire(color renderer.RGB) *wire.RGB {
	return &wire.RGB{R: uint32(color.R), G: uint32(color.G), B: uint32(color.B)}
}

func previewRGBFromWire(message *wire.RGB) (renderer.RGB, error) {
	var color renderer.RGB
	if message == nil {
		return color, nil
	}
	r, err := brokerEnum8[uint8](message.GetR())
	if err != nil {
		return renderer.RGB{}, err
	}
	g, err := brokerEnum8[uint8](message.GetG())
	if err != nil {
		return renderer.RGB{}, err
	}
	b, err := brokerEnum8[uint8](message.GetB())
	if err != nil {
		return renderer.RGB{}, err
	}
	return renderer.RGB{R: r, G: g, B: b}, nil
}

func previewCellStyleToWire(style renderer.Style) *wire.CellStyle {
	return &wire.CellStyle{
		Bold:                 style.Bold,
		Italic:               style.Italic,
		Inverse:              style.Inverse,
		Attrs:                uint32(style.Attrs),
		Foreground:           int32(style.Foreground),
		Background:           int32(style.Background),
		HasForegroundRgb:     style.HasForegroundRGB,
		HasBackgroundRgb:     style.HasBackgroundRGB,
		HasUnderlineColor:    style.HasUnderlineColor,
		HasUnderlineColorRgb: style.HasUnderlineColorRGB,
		UnderlineStyle:       uint32(style.UnderlineStyle),
		UnderlineColor:       int32(style.UnderlineColor),
		ForegroundRgb:        previewRGBToWire(style.ForegroundRGB),
		BackgroundRgb:        previewRGBToWire(style.BackgroundRGB),
		UnderlineColorRgb:    previewRGBToWire(style.UnderlineColorRGB),
	}
}

func previewCellStyleFromWire(message *wire.CellStyle) (renderer.Style, error) {
	var style renderer.Style
	if message == nil {
		return style, nil
	}
	foreground := message.GetForeground()
	if foreground < math.MinInt16 || foreground > math.MaxInt16 {
		return renderer.Style{}, errConvertRange
	}
	background := message.GetBackground()
	if background < math.MinInt16 || background > math.MaxInt16 {
		return renderer.Style{}, errConvertRange
	}
	underline := message.GetUnderlineColor()
	if underline < math.MinInt16 || underline > math.MaxInt16 {
		return renderer.Style{}, errConvertRange
	}
	underlineStyle, err := brokerEnum8[uint8](message.GetUnderlineStyle())
	if err != nil {
		return renderer.Style{}, err
	}
	attrs := message.GetAttrs()
	if attrs > math.MaxUint16 {
		return renderer.Style{}, errConvertRange
	}
	foregroundRGB, err := previewRGBFromWire(message.GetForegroundRgb())
	if err != nil {
		return renderer.Style{}, err
	}
	backgroundRGB, err := previewRGBFromWire(message.GetBackgroundRgb())
	if err != nil {
		return renderer.Style{}, err
	}
	underlineRGB, err := previewRGBFromWire(message.GetUnderlineColorRgb())
	if err != nil {
		return renderer.Style{}, err
	}
	return renderer.Style{
		Bold:                 message.GetBold(),
		Italic:               message.GetItalic(),
		Inverse:              message.GetInverse(),
		Attrs:                renderer.StyleAttrs(attrs),
		Foreground:           int(foreground),
		Background:           int(background),
		HasForegroundRGB:     message.GetHasForegroundRgb(),
		HasBackgroundRGB:     message.GetHasBackgroundRgb(),
		HasUnderlineColor:    message.GetHasUnderlineColor(),
		HasUnderlineColorRGB: message.GetHasUnderlineColorRgb(),
		UnderlineStyle:       renderer.UnderlineStyle(underlineStyle),
		UnderlineColor:       int(underline),
		ForegroundRGB:        foregroundRGB,
		BackgroundRGB:        backgroundRGB,
		UnderlineColorRGB:    underlineRGB,
	}, nil
}

func previewCellToWire(cell renderer.Cell) (*wire.PreviewCell, error) {
	if cell.Rune < 0 || cell.Rune > 0x10FFFF {
		return nil, errConvertRange
	}
	return &wire.PreviewCell{RuneValue: uint32(cell.Rune), Continuation: cell.Continuation, Style: previewCellStyleToWire(cell.Style)}, nil
}

func previewCellFromWire(message *wire.PreviewCell) (renderer.Cell, error) {
	var cell renderer.Cell
	if message == nil {
		return cell, errConvertRange
	}
	style, err := previewCellStyleFromWire(message.GetStyle())
	if err != nil {
		return renderer.Cell{}, err
	}
	runeValue := message.GetRuneValue()
	if runeValue > 0x10FFFF {
		return renderer.Cell{}, errConvertRange
	}
	return renderer.Cell{Rune: rune(runeValue), Continuation: message.GetContinuation(), Style: style}, nil
}

func previewCellsToWire(cells []renderer.Cell) ([]*wire.PreviewCell, error) {
	if len(cells) == 0 {
		return nil, nil
	}
	out := make([]*wire.PreviewCell, 0, len(cells))
	for _, cell := range cells {
		converted, err := previewCellToWire(cell)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func previewCellsFromWire(cells []*wire.PreviewCell) ([]renderer.Cell, error) {
	if len(cells) == 0 {
		return nil, nil
	}
	out := make([]renderer.Cell, 0, len(cells))
	for _, cell := range cells {
		converted, err := previewCellFromWire(cell)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func remotePreviewRequestToWire(message protocol.RemotePreviewRequest) (*wire.RemotePreviewRequest, error) {
	if err := protocol.ValidateRemotePreviewRequest(message); err != nil {
		return nil, ErrInvalidMessage
	}
	target, err := previewRemoteTargetToWire(message.Target)
	if err != nil {
		return nil, err
	}
	return &wire.RemotePreviewRequest{
		Version: uint32(message.Version),
		Target:  target,
		Width:   uint32(message.Width),
		Height:  uint32(message.Height),
	}, nil
}

func remotePreviewRequestFromWire(message *wire.RemotePreviewRequest) (protocol.RemotePreviewRequest, error) {
	var request protocol.RemotePreviewRequest
	if message == nil {
		return request, ErrInvalidMessage
	}
	version, err := brokerEnum16[uint16](message.GetVersion())
	if err != nil {
		return protocol.RemotePreviewRequest{}, errConvertRange
	}
	target, err := previewRemoteTargetFromWire(message.GetTarget())
	if err != nil {
		return protocol.RemotePreviewRequest{}, err
	}
	width, err := brokerEnum16[uint16](message.GetWidth())
	if err != nil {
		return protocol.RemotePreviewRequest{}, errConvertRange
	}
	height, err := brokerEnum16[uint16](message.GetHeight())
	if err != nil {
		return protocol.RemotePreviewRequest{}, errConvertRange
	}
	request = protocol.RemotePreviewRequest{Version: version, Target: target, Width: width, Height: height}
	if err := protocol.ValidateRemotePreviewRequest(request); err != nil {
		return protocol.RemotePreviewRequest{}, ErrInvalidMessage
	}
	return request, nil
}

func remotePreviewToWire(message protocol.RemotePreview) (*wire.RemotePreview, error) {
	if err := protocol.ValidateRemotePreview(message); err != nil {
		if errors.Is(err, protocol.ErrRemotePreviewTooLarge) {
			return nil, ErrTooLarge
		}
		return nil, ErrInvalidMessage
	}
	cells, err := previewCellsToWire(message.Cells)
	if err != nil {
		return nil, err
	}
	return &wire.RemotePreview{
		Version:     uint32(message.Version),
		Status:      uint32(message.Status),
		LifecycleId: previewLifecycleToWire(message.LifecycleID),
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
		return preview, ErrInvalidMessage
	}
	version, err := brokerEnum16[uint16](message.GetVersion())
	if err != nil {
		return protocol.RemotePreview{}, errConvertRange
	}
	status, err := brokerEnum8[protocol.RemotePreviewStatus](message.GetStatus())
	if err != nil {
		return protocol.RemotePreview{}, errConvertRange
	}
	lifecycle, err := previewLifecycleFromWire(message.GetLifecycleId())
	if err != nil {
		return protocol.RemotePreview{}, err
	}
	width, err := brokerEnum16[uint16](message.GetWidth())
	if err != nil {
		return protocol.RemotePreview{}, errConvertRange
	}
	height, err := brokerEnum16[uint16](message.GetHeight())
	if err != nil {
		return protocol.RemotePreview{}, errConvertRange
	}
	cells, err := previewCellsFromWire(message.GetCells())
	if err != nil {
		return protocol.RemotePreview{}, err
	}
	preview = protocol.RemotePreview{
		Version:     version,
		Status:      status,
		LifecycleID: lifecycle,
		TabID:       domain.TabStableID(message.GetTabId()),
		Revision:    message.GetRevision(),
		Width:       width,
		Height:      height,
		Cells:       cells,
	}
	if err := protocol.ValidateRemotePreview(preview); err != nil {
		if errors.Is(err, protocol.ErrRemotePreviewTooLarge) {
			return protocol.RemotePreview{}, ErrTooLarge
		}
		return protocol.RemotePreview{}, ErrInvalidMessage
	}
	return preview, nil
}

// previewRouteRequest mirrors one StartPreview route as the ports stream
// request the preview contract gates, so encode and decode refuse exactly
// what ports.BrokerPreviewRequest.Validate refuses.
func previewRouteRequest(m StartPreview) ports.BrokerPreviewRequest {
	return ports.BrokerPreviewRequest{
		Epoch:      m.Epoch,
		Connection: m.Connection,
		Generation: m.Generation,
		Route: ports.BrokerOpenStreamRequest{
			Epoch: m.Route.Epoch, Purpose: m.Route.Purpose,
			Admission: m.Route.Admission, Name: m.Route.Name, Local: m.Route.Local,
			Connection: m.Route.Connection, Stream: m.Route.Stream,
			Endpoint: m.Route.Endpoint, Registration: m.Route.Registration,
			Target: m.Route.Target, Env: m.Route.Env, Policy: m.Route.Policy,
			StartMode: m.Route.StartMode,
		},
		Preview: m.Preview,
	}
}

func startPreviewToWire(m StartPreview) (*wire.StartPreview, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Generation.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	// Authority repeats on the route: a route bound to another scope or a
	// non-observation purpose is refused before it travels.
	if m.Route.Epoch != m.Epoch || m.Route.Connection != m.Connection {
		return nil, ErrInvalidMessage
	}
	if m.Route.Purpose != ports.BrokerStreamObservation {
		return nil, ErrInvalidMessage
	}
	route, err := openStreamToWire(m.Route)
	if err != nil {
		return nil, err
	}
	preview, err := remotePreviewRequestToWire(m.Preview)
	if err != nil {
		return nil, err
	}
	if err := previewRouteRequest(m).Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	return &wire.StartPreview{
		Scope:      scopeToWire(m.Epoch, m.Connection),
		Generation: uint64(m.Generation),
		Route:      route,
		Preview:    preview,
	}, nil
}

func startPreviewFromWire(message *wire.StartPreview) (StartPreview, error) {
	var out StartPreview
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return StartPreview{}, ErrInvalidMessage
	}
	generation := ports.BrokerPreviewGeneration(message.GetGeneration())
	if err := generation.Validate(); err != nil {
		return StartPreview{}, ErrInvalidMessage
	}
	route, err := openStreamFromWire(message.GetRoute())
	if err != nil {
		return StartPreview{}, err
	}
	preview, err := remotePreviewRequestFromWire(message.GetPreview())
	if err != nil {
		return StartPreview{}, err
	}
	candidate := StartPreview{Epoch: epoch, Connection: connection, Generation: generation, Route: route, Preview: preview}
	if err := previewRouteRequest(candidate).Validate(); err != nil {
		return StartPreview{}, ErrInvalidMessage
	}
	return candidate, nil
}

func cancelPreviewToWire(m CancelPreview) (*wire.CancelPreview, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Generation.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	return &wire.CancelPreview{
		Scope:      scopeToWire(m.Epoch, m.Connection),
		Generation: uint64(m.Generation),
	}, nil
}

func cancelPreviewFromWire(message *wire.CancelPreview) (CancelPreview, error) {
	var out CancelPreview
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return CancelPreview{}, ErrInvalidMessage
	}
	generation := ports.BrokerPreviewGeneration(message.GetGeneration())
	if err := generation.Validate(); err != nil {
		return CancelPreview{}, ErrInvalidMessage
	}
	return CancelPreview{Epoch: epoch, Connection: connection, Generation: generation}, nil
}

func previewPublicationToWire(m PreviewPublication) (*wire.PreviewPublication, error) {
	if m.Epoch == 0 {
		return nil, ErrInvalidMessage
	}
	if err := m.Connection.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	if err := m.Generation.Validate(); err != nil {
		return nil, ErrInvalidMessage
	}
	out := &wire.PreviewPublication{
		Scope:      scopeToWire(m.Epoch, m.Connection),
		Generation: uint64(m.Generation),
	}
	// Exactly one of preview/error travels: a failed publication carries
	// only its typed error, a successful one only its preview.
	if m.HasError {
		if err := m.Error.validate(); err != nil {
			return nil, err
		}
		if previewCarriesViewport(m.Preview) {
			return nil, ErrInvalidMessage
		}
		out.Result = &wire.PreviewPublication_Error{Error: errorDetailToWire(m.Error)}
		return out, nil
	}
	if m.Error != (ErrorDetail{}) {
		return nil, ErrInvalidMessage
	}
	preview, err := remotePreviewToWire(m.Preview)
	if err != nil {
		return nil, err
	}
	out.Result = &wire.PreviewPublication_Preview{Preview: preview}
	return out, nil
}

// previewCarriesViewport reports whether a preview value carries any
// viewport authority. A failed publication must carry none.
func previewCarriesViewport(preview protocol.RemotePreview) bool {
	return preview.Version != 0 || preview.Status != 0 ||
		preview.LifecycleID != (domain.SessionLifecycleID{}) || preview.TabID != "" ||
		preview.Revision != 0 || preview.Width != 0 || preview.Height != 0 ||
		len(preview.Cells) != 0
}

func previewPublicationFromWire(message *wire.PreviewPublication) (PreviewPublication, error) {
	var out PreviewPublication
	if message == nil {
		return out, ErrInvalidMessage
	}
	epoch, connection, err := scopeFromWire(message.GetScope())
	if err != nil {
		return PreviewPublication{}, ErrInvalidMessage
	}
	generation := ports.BrokerPreviewGeneration(message.GetGeneration())
	if err := generation.Validate(); err != nil {
		return PreviewPublication{}, ErrInvalidMessage
	}
	result := PreviewPublication{Epoch: epoch, Connection: connection, Generation: generation}
	switch payload := message.GetResult().(type) {
	case *wire.PreviewPublication_Preview:
		if payload.Preview == nil {
			return PreviewPublication{}, ErrInvalidMessage
		}
		preview, err := remotePreviewFromWire(payload.Preview)
		if err != nil {
			return PreviewPublication{}, err
		}
		result.Preview = preview
	case *wire.PreviewPublication_Error:
		if payload.Error == nil {
			return PreviewPublication{}, ErrInvalidMessage
		}
		detail, err := errorDetailFromWire(payload.Error)
		if err != nil {
			return PreviewPublication{}, ErrInvalidMessage
		}
		result.Error = detail
		result.HasError = true
	default:
		// An absent result is malformed: exactly one member is required.
		return PreviewPublication{}, ErrInvalidMessage
	}
	return result, nil
}
