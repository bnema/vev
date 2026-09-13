// Protobuf semantic converters (P2.2, wired at the P3.3 cutover).
//
// This file owns every semantic protocol value <-> generated wire envelope
// conversion used by client.go/server.go dispatch. Converters preserve
// domain-owned identifiers/geometry field-by-field and validate every
// narrowing numeric conversion before casting. Use cases always see
// uncompressed terminal data.
package sessionwire

import (
	"errors"
	"math"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

var errProtoConvertRange = errors.New("sessionwire: wire value out of semantic range")

func mustUint16(v uint32) (uint16, error) {
	if v > math.MaxUint16 {
		return 0, errProtoConvertRange
	}
	return uint16(v), nil
}

func mustUint8(v uint32) (uint8, error) {
	if v > math.MaxUint8 {
		return 0, errProtoConvertRange
	}
	return uint8(v), nil
}

// enum8 narrows one wire uint32 into an 8-bit semantic enum. Values whose
// high bits would otherwise truncate into a valid enum range are refused
// before any cast, so hostile wire input cannot alias a legitimate value.
func enum8[T ~uint8](value uint32) (T, error) {
	if value > math.MaxUint8 {
		return 0, errProtoConvertRange
	}
	return T(value), nil
}

func mustTabIDString(value string) domain.TabStableID { return domain.TabStableID(value) }

func lifecycleToWire(id domain.SessionLifecycleID) *wire.LifecycleID {
	value := append([]byte(nil), id[:]...)
	return &wire.LifecycleID{Value: value}
}

func lifecycleFromWire(message *wire.LifecycleID) (domain.SessionLifecycleID, error) {
	var id domain.SessionLifecycleID
	if message == nil || len(message.GetValue()) != len(id) {
		return id, errProtoConvertRange
	}
	copy(id[:], message.GetValue())
	return id, nil
}

func tabSelectorToWire(selector domain.TabSelector) *wire.TabSelector {
	return &wire.TabSelector{
		Kind:          uint32(selector.Kind),
		StableId:      string(selector.StableID),
		Ordinal:       uint32(selector.Ordinal),
		RawName:       selector.RawName,
		ExpectedCount: uint32(selector.ExpectedCount),
	}
}

func tabSelectorFromWire(message *wire.TabSelector) (domain.TabSelector, error) {
	var selector domain.TabSelector
	if message == nil {
		return selector, nil
	}
	selector.Kind = domain.TabSelectorKind(message.GetKind())
	selector.StableID = domain.TabStableID(message.GetStableId())
	ordinal, err := mustUint16(message.GetOrdinal())
	if err != nil {
		return domain.TabSelector{}, err
	}
	selector.Ordinal = ordinal
	selector.RawName = message.GetRawName()
	expected, err := mustUint16(message.GetExpectedCount())
	if err != nil {
		return domain.TabSelector{}, err
	}
	selector.ExpectedCount = expected
	return selector, nil
}

func remoteTargetToWire(target *domain.RemoteSessionTarget) (*wire.RemoteTarget, error) {
	if target == nil {
		return nil, nil
	}
	lifecycle := lifecycleToWire(target.LifecycleID)
	return &wire.RemoteTarget{
		Endpoint:      target.Endpoint,
		DisplayOrigin: target.DisplayOrigin,
		LifecycleId:   lifecycle,
		SessionName:   target.SessionName,
		LiveTabId:     string(target.LiveTabID),
		StoppedTab:    tabSelectorToWire(target.StoppedTab),
		Stopped:       target.Stopped,
	}, nil
}

func remoteTargetFromWire(message *wire.RemoteTarget) (*domain.RemoteSessionTarget, error) {
	if message == nil {
		return nil, nil
	}
	target := &domain.RemoteSessionTarget{
		Endpoint:      message.GetEndpoint(),
		DisplayOrigin: message.GetDisplayOrigin(),
		SessionName:   message.GetSessionName(),
		LiveTabID:     domain.TabStableID(message.GetLiveTabId()),
		Stopped:       message.GetStopped(),
	}
	lifecycle, err := lifecycleFromWire(message.GetLifecycleId())
	if err != nil {
		return nil, err
	}
	target.LifecycleID = lifecycle
	stopped, err := tabSelectorFromWire(message.GetStoppedTab())
	if err != nil {
		return nil, err
	}
	target.StoppedTab = stopped
	return target, nil
}

func exactTargetToWire(target *protocol.ExactSessionTarget) *wire.ExactTarget {
	if target == nil {
		return nil
	}
	return &wire.ExactTarget{LifecycleId: lifecycleToWire(target.LifecycleID), SessionName: target.SessionName}
}

func exactTargetFromWire(message *wire.ExactTarget) (*protocol.ExactSessionTarget, error) {
	if message == nil {
		return nil, nil
	}
	lifecycle, err := lifecycleFromWire(message.GetLifecycleId())
	if err != nil {
		return nil, err
	}
	return &protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: message.GetSessionName()}, nil
}

func rgbToWire(color renderer.RGB) *wire.RGB {
	return &wire.RGB{R: uint32(color.R), G: uint32(color.G), B: uint32(color.B)}
}

func rgbFromWire(message *wire.RGB) (renderer.RGB, error) {
	var color renderer.RGB
	if message == nil {
		return color, nil
	}
	r, err := mustUint8(message.GetR())
	if err != nil {
		return renderer.RGB{}, err
	}
	g, err := mustUint8(message.GetG())
	if err != nil {
		return renderer.RGB{}, err
	}
	b, err := mustUint8(message.GetB())
	if err != nil {
		return renderer.RGB{}, err
	}
	return renderer.RGB{R: r, G: g, B: b}, nil
}

func cellStyleToWire(style renderer.Style) *wire.CellStyle {
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
		ForegroundRgb:        rgbToWire(style.ForegroundRGB),
		BackgroundRgb:        rgbToWire(style.BackgroundRGB),
		UnderlineColorRgb:    rgbToWire(style.UnderlineColorRGB),
	}
}

func cellStyleFromWire(message *wire.CellStyle) (renderer.Style, error) {
	var style renderer.Style
	if message == nil {
		return style, nil
	}
	foreground := message.GetForeground()
	if foreground < math.MinInt16 || foreground > math.MaxInt16 {
		return renderer.Style{}, errProtoConvertRange
	}
	background := message.GetBackground()
	if background < math.MinInt16 || background > math.MaxInt16 {
		return renderer.Style{}, errProtoConvertRange
	}
	underline := message.GetUnderlineColor()
	if underline < math.MinInt16 || underline > math.MaxInt16 {
		return renderer.Style{}, errProtoConvertRange
	}
	attrs := message.GetAttrs()
	if attrs > math.MaxUint16 {
		return renderer.Style{}, errProtoConvertRange
	}
	foregroundRGB, err := rgbFromWire(message.GetForegroundRgb())
	if err != nil {
		return renderer.Style{}, err
	}
	backgroundRGB, err := rgbFromWire(message.GetBackgroundRgb())
	if err != nil {
		return renderer.Style{}, err
	}
	underlineRGB, err := rgbFromWire(message.GetUnderlineColorRgb())
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
		UnderlineStyle:       renderer.UnderlineStyle(message.GetUnderlineStyle()),
		UnderlineColor:       int(underline),
		ForegroundRGB:        foregroundRGB,
		BackgroundRGB:        backgroundRGB,
		UnderlineColorRGB:    underlineRGB,
	}, nil
}

func previewCellToWire(cell renderer.Cell) (*wire.PreviewCell, error) {
	if cell.Rune < 0 || cell.Rune > 0x10FFFF {
		return nil, errProtoConvertRange
	}
	return &wire.PreviewCell{RuneValue: uint32(cell.Rune), Continuation: cell.Continuation, Style: cellStyleToWire(cell.Style)}, nil
}

func previewCellFromWire(message *wire.PreviewCell) (renderer.Cell, error) {
	var cell renderer.Cell
	if message == nil {
		return cell, errProtoConvertRange
	}
	style, err := cellStyleFromWire(message.GetStyle())
	if err != nil {
		return renderer.Cell{}, err
	}
	runeValue := message.GetRuneValue()
	if runeValue > 0x10FFFF {
		return renderer.Cell{}, errProtoConvertRange
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

func registrationToWire(registration domain.RemoteRegistration) *wire.RemoteRegistration {
	incarnation := append([]byte(nil), registration.Incarnation[:]...)
	return &wire.RemoteRegistration{Endpoint: registration.Endpoint, Incarnation: incarnation, Generation: uint64(registration.Generation)}
}

func registrationFromWire(message *wire.RemoteRegistration) (domain.RemoteRegistration, error) {
	var registration domain.RemoteRegistration
	if message == nil {
		return registration, nil
	}
	registration.Endpoint = message.GetEndpoint()
	if len(message.GetIncarnation()) != len(registration.Incarnation) {
		return domain.RemoteRegistration{}, errProtoConvertRange
	}
	copy(registration.Incarnation[:], message.GetIncarnation())
	registration.Generation = domain.RemoteGeneration(message.GetGeneration())
	return registration, nil
}
