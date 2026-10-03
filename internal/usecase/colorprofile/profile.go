// Package colorprofile maps terminal color capabilities to the vev-vt renderer
// color profile. It is the single mode-to-profile policy shared by the client
// and the daemon.
package colorprofile

import (
	ansi "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain/terminalcap"
)

// Profile returns the renderer profile for c. A terminal without RGB support
// gets indexed colors, so RGB surfaces are quantized instead of dropped.
func Profile(c terminalcap.ColorCapabilities) ansi.ColorProfile {
	if c.RGB() {
		return ansi.ColorProfileTrueColor
	}
	return ansi.ColorProfileANSI256
}
