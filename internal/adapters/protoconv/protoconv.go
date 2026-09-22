// Package protoconv holds the single canonical implementation of the
// semantic protocol value <-> generated wire envelope converters shared by
// internal/adapters/sessionwire and internal/adapters/brokerwire.
//
// Every narrowing numeric conversion is validated before the cast: values
// whose high bits would otherwise alias a legitimate value are refused with
// ErrOutOfRange before any truncation, so hostile wire input cannot alias a
// valid identifier, enum, or geometry value. A nil wire message that carries
// no optional-vs-absent distinction for its caller is refused the same way,
// except where a field is documented below as tolerant of a nil submessage.
//
// protoconv returns exactly two error shapes: ErrOutOfRange for every
// numeric/identity range or required-nil failure, and (for the
// RemotePreviewRequest/RemotePreview converters) the protocol package's own
// validation sentinels, returned unwrapped. Callers in sessionwire and
// brokerwire map these to their own local sentinel errors, since the two
// packages diverge on how strictly they distinguish range failures from
// structural/semantic ones.
package protoconv

import (
	"errors"
	"math"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/wire"
)

// ErrOutOfRange is returned by every converter in this package when a wire
// value cannot losslessly represent its semantic counterpart, or a wire
// submessage required for a semantic value is nil.
var ErrOutOfRange = errors.New("protoconv: wire value out of semantic range")

// Uint8 narrows one wire uint32 into an 8-bit semantic value or enum. Values
// whose high bits would otherwise truncate into a valid range are refused
// before any cast.
func Uint8[T ~uint8](v uint32) (T, error) {
	if v > math.MaxUint8 {
		return 0, ErrOutOfRange
	}
	return T(v), nil
}

// Uint16 narrows one wire uint32 into a 16-bit semantic value or enum.
func Uint16[T ~uint16](v uint32) (T, error) {
	if v > math.MaxUint16 {
		return 0, ErrOutOfRange
	}
	return T(v), nil
}

// LifecycleToWire never fails: a semantic lifecycle ID is always a fixed
// byte array.
func LifecycleToWire(id domain.SessionLifecycleID) *wire.LifecycleID {
	value := append([]byte(nil), id[:]...)
	return &wire.LifecycleID{Value: value}
}

// LifecycleFromWire requires an exact-length value: a nil message or a
// wrong-length byte slice is refused rather than silently zero-padded or
// truncated.
func LifecycleFromWire(message *wire.LifecycleID) (domain.SessionLifecycleID, error) {
	var id domain.SessionLifecycleID
	if message == nil || len(message.GetValue()) != len(id) {
		return id, ErrOutOfRange
	}
	copy(id[:], message.GetValue())
	return id, nil
}

// TabSelectorToWire never fails.
func TabSelectorToWire(selector domain.TabSelector) *wire.TabSelector {
	return &wire.TabSelector{
		Kind:          uint32(selector.Kind),
		StableId:      string(selector.StableID),
		Ordinal:       uint32(selector.Ordinal),
		RawName:       selector.RawName,
		ExpectedCount: uint32(selector.ExpectedCount),
	}
}

// TabSelectorFromWire tolerates a nil message: an absent selector decodes
// as the zero selector rather than an error, matching the optional stopped
// tab it composes into a RemoteTarget.
func TabSelectorFromWire(message *wire.TabSelector) (domain.TabSelector, error) {
	var selector domain.TabSelector
	if message == nil {
		return selector, nil
	}
	kind, err := Uint8[domain.TabSelectorKind](message.GetKind())
	if err != nil {
		return domain.TabSelector{}, err
	}
	selector.Kind = kind
	selector.StableID = domain.TabStableID(message.GetStableId())
	ordinal, err := Uint16[uint16](message.GetOrdinal())
	if err != nil {
		return domain.TabSelector{}, err
	}
	selector.Ordinal = ordinal
	selector.RawName = message.GetRawName()
	expected, err := Uint16[uint16](message.GetExpectedCount())
	if err != nil {
		return domain.TabSelector{}, err
	}
	selector.ExpectedCount = expected
	return selector, nil
}

// RemoteTargetToWire never fails.
func RemoteTargetToWire(target domain.RemoteSessionTarget) *wire.RemoteTarget {
	return &wire.RemoteTarget{
		Endpoint:      target.Endpoint,
		DisplayOrigin: target.DisplayOrigin,
		LifecycleId:   LifecycleToWire(target.LifecycleID),
		SessionName:   target.SessionName,
		LiveTabId:     string(target.LiveTabID),
		StoppedTab:    TabSelectorToWire(target.StoppedTab),
		Stopped:       target.Stopped,
	}
}

// RemoteTargetFromWire requires a non-nil message: unlike TabSelectorFromWire,
// a remote target is never optional at this layer, so a nil submessage is a
// range failure. Callers that carry an optional target (a pointer field)
// check nil themselves before calling in.
func RemoteTargetFromWire(message *wire.RemoteTarget) (domain.RemoteSessionTarget, error) {
	var target domain.RemoteSessionTarget
	if message == nil {
		return target, ErrOutOfRange
	}
	lifecycle, err := LifecycleFromWire(message.GetLifecycleId())
	if err != nil {
		return domain.RemoteSessionTarget{}, err
	}
	stopped, err := TabSelectorFromWire(message.GetStoppedTab())
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

// RGBToWire never fails.
func RGBToWire(color renderer.RGB) *wire.RGB {
	return &wire.RGB{R: uint32(color.R), G: uint32(color.G), B: uint32(color.B)}
}

// RGBFromWire tolerates a nil message: an absent color decodes as the zero
// RGB, matching an unset foreground/background/underline color.
func RGBFromWire(message *wire.RGB) (renderer.RGB, error) {
	var color renderer.RGB
	if message == nil {
		return color, nil
	}
	r, err := Uint8[uint8](message.GetR())
	if err != nil {
		return renderer.RGB{}, err
	}
	g, err := Uint8[uint8](message.GetG())
	if err != nil {
		return renderer.RGB{}, err
	}
	b, err := Uint8[uint8](message.GetB())
	if err != nil {
		return renderer.RGB{}, err
	}
	return renderer.RGB{R: r, G: g, B: b}, nil
}

// CellStyleToWire never fails.
func CellStyleToWire(style renderer.Style) *wire.CellStyle {
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
		ForegroundRgb:        RGBToWire(style.ForegroundRGB),
		BackgroundRgb:        RGBToWire(style.BackgroundRGB),
		UnderlineColorRgb:    RGBToWire(style.UnderlineColorRGB),
	}
}

// CellStyleFromWire tolerates a nil message: an absent style decodes as the
// zero style.
func CellStyleFromWire(message *wire.CellStyle) (renderer.Style, error) {
	var style renderer.Style
	if message == nil {
		return style, nil
	}
	foreground := message.GetForeground()
	if foreground < math.MinInt16 || foreground > math.MaxInt16 {
		return renderer.Style{}, ErrOutOfRange
	}
	background := message.GetBackground()
	if background < math.MinInt16 || background > math.MaxInt16 {
		return renderer.Style{}, ErrOutOfRange
	}
	underline := message.GetUnderlineColor()
	if underline < math.MinInt16 || underline > math.MaxInt16 {
		return renderer.Style{}, ErrOutOfRange
	}
	underlineStyle, err := Uint8[uint8](message.GetUnderlineStyle())
	if err != nil {
		return renderer.Style{}, err
	}
	attrs := message.GetAttrs()
	if attrs > math.MaxUint16 {
		return renderer.Style{}, ErrOutOfRange
	}
	foregroundRGB, err := RGBFromWire(message.GetForegroundRgb())
	if err != nil {
		return renderer.Style{}, err
	}
	backgroundRGB, err := RGBFromWire(message.GetBackgroundRgb())
	if err != nil {
		return renderer.Style{}, err
	}
	underlineRGB, err := RGBFromWire(message.GetUnderlineColorRgb())
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

// PreviewCellToWire refuses a rune outside the valid Unicode code point
// range before it can be emitted.
func PreviewCellToWire(cell renderer.Cell) (*wire.PreviewCell, error) {
	if cell.Rune < 0 || cell.Rune > 0x10FFFF {
		return nil, ErrOutOfRange
	}
	return &wire.PreviewCell{RuneValue: uint32(cell.Rune), Continuation: cell.Continuation, Style: CellStyleToWire(cell.Style)}, nil
}

// PreviewCellFromWire requires a non-nil message: unlike CellStyleFromWire
// and RGBFromWire, a single grid cell is never optional.
func PreviewCellFromWire(message *wire.PreviewCell) (renderer.Cell, error) {
	var cell renderer.Cell
	if message == nil {
		return cell, ErrOutOfRange
	}
	style, err := CellStyleFromWire(message.GetStyle())
	if err != nil {
		return renderer.Cell{}, err
	}
	runeValue := message.GetRuneValue()
	if runeValue > 0x10FFFF {
		return renderer.Cell{}, ErrOutOfRange
	}
	return renderer.Cell{Rune: rune(runeValue), Continuation: message.GetContinuation(), Style: style}, nil
}

// PreviewCellsToWire preserves nil for an empty/absent slice rather than
// allocating an empty one.
func PreviewCellsToWire(cells []renderer.Cell) ([]*wire.PreviewCell, error) {
	if len(cells) == 0 {
		return nil, nil
	}
	out := make([]*wire.PreviewCell, 0, len(cells))
	for _, cell := range cells {
		converted, err := PreviewCellToWire(cell)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

// PreviewCellsFromWire preserves nil for an empty/absent slice.
func PreviewCellsFromWire(cells []*wire.PreviewCell) ([]renderer.Cell, error) {
	if len(cells) == 0 {
		return nil, nil
	}
	out := make([]renderer.Cell, 0, len(cells))
	for _, cell := range cells {
		converted, err := PreviewCellFromWire(cell)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}
