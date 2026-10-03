// Package colorprofile maps terminal color capabilities to the vev-vt renderer
// color profile. It is the single mode-to-profile policy shared by the client
// and the daemon.
package colorprofile

import (
	ansi "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain/terminalcap"
)

// Profile returns the renderer profile for c. A terminal without RGB support
// gets a reduced palette, so RGB surfaces are quantized instead of dropped.
// Unknown modes fall back to the conservative 256-color profile.
func Profile(c terminalcap.ColorCapabilities) ansi.ColorProfile {
	switch c.Mode {
	case terminalcap.TrueColor:
		return ansi.ColorProfileTrueColor
	case terminalcap.ANSI16:
		return ansi.ColorProfileANSI16
	case terminalcap.Monochrome:
		return ansi.ColorProfileMonochrome
	default:
		return ansi.ColorProfileANSI256
	}
}
