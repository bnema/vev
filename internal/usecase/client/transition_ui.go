package client

import (
	"bytes"
	"fmt"

	"github.com/bnema/vev/internal/domain"
)

var transitionSpinnerFrames = [...]rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}

// RenderTransitionNotice draws one connecting frame for the terminal owner.
func RenderTransitionNotice(size domain.Size, frame int, message string) []byte {
	var out bytes.Buffer
	if _, err := drawClientToast(&out, size, fmt.Sprintf("%c %s", transitionSpinnerFrames[frame%len(transitionSpinnerFrames)], message), domain.AnchorCenter); err != nil {
		return nil
	}
	return out.Bytes()
}
