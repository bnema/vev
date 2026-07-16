//go:build darwin

package clipboard

import (
	"context"
	"os/exec"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const osaReadTimeout = time.Second

// OSA implements ports.ClipboardReader using the macOS pasteboard through
// osascript. It holds no state and is safe for concurrent use.
type OSA struct{}

var _ ports.ClipboardReader = (*OSA)(nil)

// New returns the native clipboard reader for this operating system.
func New() ports.ClipboardReader { return &OSA{} }

// ReadImage reads the preferred pasteboard image representation. osascript
// reports unavailable pasteboard flavors as command failures; those and all
// malformed outputs are intentionally indistinguishable to callers from an
// empty clipboard.
func (*OSA) ReadImage(ctx context.Context) (string, []byte, error) {
	for _, format := range []struct {
		class string
		mime  string
	}{
		{class: "PNGf", mime: "image/png"},
		{class: "JPEG", mime: "image/jpeg"},
	} {
		commandCtx, cancel := context.WithTimeout(ctx, osaReadTimeout)
		out, err := exec.CommandContext(commandCtx, "osascript", "-e", "the clipboard as «class "+format.class+"»").Output()
		cancel()
		if err != nil {
			continue
		}

		data, err := parseOSAData(string(out), format.class)
		if err == nil {
			return format.mime, data, nil
		}
	}
	return "", nil, ports.ErrNoClipboardImage
}
