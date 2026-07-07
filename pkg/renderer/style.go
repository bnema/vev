package renderer

type RGB struct {
	R uint8
	G uint8
	B uint8
}

type Style struct {
	Bold       bool
	Italic     bool
	Inverse    bool
	Foreground int // -1 means unset; ignored when HasForegroundRGB is true
	Background int // -1 means unset; ignored when HasBackgroundRGB is true

	HasForegroundRGB bool
	ForegroundRGB    RGB
	HasBackgroundRGB bool
	BackgroundRGB    RGB
}

func DefaultStyle() Style { return Style{Foreground: -1, Background: -1} }

func (s Style) Equal(other Style) bool {
	if s.Bold != other.Bold || s.Italic != other.Italic || s.Inverse != other.Inverse {
		return false
	}
	if s.HasForegroundRGB != other.HasForegroundRGB || s.HasBackgroundRGB != other.HasBackgroundRGB {
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
		return s.BackgroundRGB == other.BackgroundRGB
	}
	return s.Background == other.Background
}
