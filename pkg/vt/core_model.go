package vt

import "github.com/bnema/vev/pkg/vtcore"

// The public model is re-exported from the frontend-neutral core package while
// the extraction is staged. The aliases keep Screen, History, and consumers on
// one cell/style/frame representation; vtcore remains the owning package.
type Cell = vtcore.Cell
type RGB = vtcore.RGB
type StyleAttrs = vtcore.StyleAttrs
type UnderlineStyle = vtcore.UnderlineStyle
type Style = vtcore.Style
type Frame = vtcore.Frame

const (
	AttrDim           = vtcore.AttrDim
	AttrUnderline     = vtcore.AttrUnderline
	AttrBlink         = vtcore.AttrBlink
	AttrStrikethrough = vtcore.AttrStrikethrough

	UnderlineNone   = vtcore.UnderlineNone
	UnderlineSingle = vtcore.UnderlineSingle
	UnderlineDouble = vtcore.UnderlineDouble
	UnderlineCurly  = vtcore.UnderlineCurly
	UnderlineDotted = vtcore.UnderlineDotted
	UnderlineDashed = vtcore.UnderlineDashed
)

var (
	BlankCell    = vtcore.BlankCell
	DefaultStyle = vtcore.DefaultStyle
	FullRedraw   = vtcore.FullRedraw
	NewFrame     = vtcore.NewFrame
)

func RuneWidth(r rune) int { return vtcore.RuneWidth(r) }
