package client

import (
	"bytes"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
)

var transitionSpinnerFrames = [...]rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}

// transitionSpinner returns the connecting spinner for glyphs: the braille
// spinner with Nerd Font glyphs, the set's own spinner otherwise.
func transitionSpinner(glyphs ui.GlyphSet) []rune {
	if glyphs.Mode == domain.GlyphsUnicode || glyphs.Mode == domain.GlyphsASCII {
		return glyphs.Spinner
	}
	return transitionSpinnerFrames[:]
}

// RenderTransitionNotice draws one connecting frame for the terminal owner
// with the spinner of the client's glyph set.
func RenderTransitionNotice(size domain.Size, frame int, message string, glyphs ui.GlyphSet) []byte {
	var out bytes.Buffer
	frames := transitionSpinner(glyphs)
	if _, err := drawClientToast(&out, size, fmt.Sprintf("%c %s", frames[frame%len(frames)], message), domain.AnchorCenter, domain.NoticeInfo); err != nil {
		return nil
	}
	return out.Bytes()
}
