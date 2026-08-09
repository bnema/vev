package ansi

import "github.com/bnema/vev/pkg/vtcore"

type RGB = vtcore.RGB
type StyleAttrs = vtcore.StyleAttrs
type UnderlineStyle = vtcore.UnderlineStyle
type Style = vtcore.Style

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

var DefaultStyle = vtcore.DefaultStyle
