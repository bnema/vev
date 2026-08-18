package daemon

import renderer "github.com/bnema/vev-vt"

// adaptFrameColors preserves the composed frame for capable attachments and
// converts direct colors to the xterm 256-color cube for constrained ones.
func adaptFrameColors(frame renderer.Frame, trueColor bool) renderer.Frame {
	if trueColor {
		return frame
	}
	adapted := frame.Clone()
	for i := range adapted.Cells {
		style := &adapted.Cells[i].Style
		if style.HasForegroundRGB {
			style.Foreground = xterm256Color(style.ForegroundRGB)
			style.HasForegroundRGB = false
		}
		if style.HasBackgroundRGB {
			style.Background = xterm256Color(style.BackgroundRGB)
			style.HasBackgroundRGB = false
		}
		if style.HasUnderlineColorRGB {
			style.UnderlineColor = xterm256Color(style.UnderlineColorRGB)
			style.HasUnderlineColorRGB = false
			style.HasUnderlineColor = true
		}
	}
	return adapted
}

var (
	xterm256ColorLevels = [...]uint8{0, 95, 135, 175, 215, 255}
	xterm256GrayLevels  = [...]uint8{8, 18, 28, 38, 48, 58, 68, 78, 88, 98, 108, 118, 128, 138, 148, 158, 168, 178, 188, 198, 208, 218, 228, 238}
)

func xterm256Color(rgb renderer.RGB) int {
	bestIndex := 16
	bestDistance := 3*255*255 + 1

	for red, redLevel := range xterm256ColorLevels {
		for green, greenLevel := range xterm256ColorLevels {
			for blue, blueLevel := range xterm256ColorLevels {
				candidate := renderer.RGB{R: redLevel, G: greenLevel, B: blueLevel}
				distance := rgbDistance(rgb, candidate)
				if distance < bestDistance {
					bestIndex = 16 + 36*red + 6*green + blue
					bestDistance = distance
				}
			}
		}
	}

	for i, level := range xterm256GrayLevels {
		candidate := renderer.RGB{R: level, G: level, B: level}
		distance := rgbDistance(rgb, candidate)
		if distance < bestDistance {
			bestIndex = 232 + i
			bestDistance = distance
		}
	}

	return bestIndex
}

func rgbDistance(a, b renderer.RGB) int {
	red := int(a.R) - int(b.R)
	green := int(a.G) - int(b.G)
	blue := int(a.B) - int(b.B)
	return red*red + green*green + blue*blue
}
