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

func xterm256Color(rgb renderer.RGB) int {
	red := (int(rgb.R)*5 + 127) / 255
	green := (int(rgb.G)*5 + 127) / 255
	blue := (int(rgb.B)*5 + 127) / 255
	return 16 + 36*red + 6*green + blue
}
