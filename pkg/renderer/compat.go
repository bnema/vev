package renderer

import (
	"github.com/bnema/vev/pkg/ansi"
	"github.com/bnema/vev/pkg/vtcore"
)

type Capabilities = ansi.Capabilities
type Cell = vtcore.Cell
type Damage = vtcore.Damage
type DamageKind = vtcore.DamageKind
type DeltaCandidate = ansi.DeltaCandidate
type DeltaPlan = ansi.DeltaPlan
type Frame = vtcore.Frame
type PreparedDraw = ansi.PreparedDraw
type RGB = vtcore.RGB
type Renderer = ansi.Renderer
type Scroll = ansi.Scroll
type Span = ansi.Span
type Style = vtcore.Style
type StyleAttrs = vtcore.StyleAttrs
type UnderlineStyle = vtcore.UnderlineStyle

const (
	AttrDim           = vtcore.AttrDim
	AttrUnderline     = vtcore.AttrUnderline
	AttrBlink         = vtcore.AttrBlink
	AttrStrikethrough = vtcore.AttrStrikethrough

	DamageText       = vtcore.DamageText
	DamageClear      = vtcore.DamageClear
	DamageScrollUp   = vtcore.DamageScrollUp
	DamageFullRedraw = vtcore.DamageFullRedraw

	UnderlineNone   = vtcore.UnderlineNone
	UnderlineSingle = vtcore.UnderlineSingle
	UnderlineDouble = vtcore.UnderlineDouble
	UnderlineCurly  = vtcore.UnderlineCurly
	UnderlineDotted = vtcore.UnderlineDotted
	UnderlineDashed = vtcore.UnderlineDashed

	SyncStartCSI = ansi.SyncStartCSI
	SyncEndCSI   = ansi.SyncEndCSI
)

var (
	BlankCell    = vtcore.BlankCell
	DefaultStyle = vtcore.DefaultStyle
	FullRedraw   = vtcore.FullRedraw
	NewFrame     = vtcore.NewFrame
)

func New(caps Capabilities) *Renderer {
	return ansi.New(caps)
}

func PlanDelta(frame Frame, damage []Damage, committed Frame, reset bool) (DeltaCandidate, error) {
	return ansi.PlanDelta(frame, damage, committed, reset)
}

func RuneWidth(r rune) int { return vtcore.RuneWidth(r) }

func WrapSynchronized(content []byte, enabled bool) []byte {
	return ansi.WrapSynchronized(content, enabled)
}
