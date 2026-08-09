package vtcore

type RGB struct {
	R uint8
	G uint8
	B uint8
}

type StyleAttrs uint16

const (
	AttrDim StyleAttrs = 1 << iota
	AttrUnderline
	AttrBlink
	AttrStrikethrough
)

type UnderlineStyle uint8

const (
	UnderlineNone UnderlineStyle = iota
	UnderlineSingle
	UnderlineDouble
	UnderlineCurly
	UnderlineDotted
	UnderlineDashed
)

type Style struct {
	Bold       bool
	Italic     bool
	Inverse    bool
	Attrs      StyleAttrs
	Foreground int // -1 means unset; ignored when HasForegroundRGB is true
	Background int // -1 means unset; ignored when HasBackgroundRGB is true

	HasForegroundRGB bool
	ForegroundRGB    RGB
	HasBackgroundRGB bool
	BackgroundRGB    RGB

	UnderlineStyle       UnderlineStyle
	HasUnderlineColor    bool
	UnderlineColor       int // ignored when HasUnderlineColorRGB is true
	HasUnderlineColorRGB bool
	UnderlineColorRGB    RGB
}

func DefaultStyle() Style { return Style{Foreground: -1, Background: -1} }

func (s Style) Equal(other Style) bool {
	if s.Bold != other.Bold || s.Italic != other.Italic || s.Inverse != other.Inverse || s.Attrs != other.Attrs || s.UnderlineStyle != other.UnderlineStyle {
		return false
	}
	if s.HasForegroundRGB != other.HasForegroundRGB || s.HasBackgroundRGB != other.HasBackgroundRGB || s.HasUnderlineColorRGB != other.HasUnderlineColorRGB {
		return false
	}
	if s.HasForegroundRGB {
		if s.ForegroundRGB != other.ForegroundRGB {
			return false
		}
	} else if s.Foreground != other.Foreground {
		return false
	}
	if s.HasBackgroundRGB {
		if s.BackgroundRGB != other.BackgroundRGB {
			return false
		}
	} else if s.Background != other.Background {
		return false
	}
	if s.HasUnderlineColorRGB {
		return s.UnderlineColorRGB == other.UnderlineColorRGB
	}
	if s.HasUnderlineColor != other.HasUnderlineColor {
		return false
	}
	if s.HasUnderlineColor {
		return s.UnderlineColor == other.UnderlineColor
	}
	return true
}
