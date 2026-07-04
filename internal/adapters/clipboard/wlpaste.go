// Package clipboard implements ports.ClipboardReader over Wayland's
// wl-clipboard CLI (wl-paste). It is the client-side seam that lets a remote
// attach forward a locally clipped image to the daemon (see
// docs/superpowers/specs/2026-07-04-clipboard-image-transfer-design.md); an
// xclip or OSC-52 backend could implement the same interface later without
// touching the wire protocol.
package clipboard

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// mimePreference orders candidate image MIME types the same way pi (the
// reference TUI agent whose local Ctrl+V behavior this replicates) does:
// png first, then the other common web/image formats.
var mimePreference = []string{"image/png", "image/jpeg", "image/webp", "image/gif"}

// listTypesTimeout bounds the "what's on the clipboard" probe so a hung or
// missing wl-paste never stalls the stdin pump for long.
const listTypesTimeout = 1 * time.Second

// defaultReadTimeout bounds the actual clipboard data read. Without this, a
// wl-paste that hangs mid-read (as opposed to during the --list-types probe,
// which listTypesTimeout already covers) would stall the client's stdin pump
// indefinitely, since ReadImage is called with context.Background() from the
// interceptor. Generous for a read of up to the 10 MiB cap.
const defaultReadTimeout = 10 * time.Second

// WlPaste implements ports.ClipboardReader by shelling out to wl-paste. It
// holds no state and is safe for concurrent use; every call runs a fresh
// subprocess.
type WlPaste struct {
	// execCommand constructs the command to run; overridable in tests so
	// command construction can be verified without requiring wl-paste (or
	// even Wayland) to be installed.
	execCommand func(ctx context.Context, name string, args ...string) *exec.Cmd
	// readTimeout bounds the data-read command; zero means defaultReadTimeout
	// (a zero-value WlPaste{execCommand: ...}, as tests construct directly,
	// still gets a sane default). Tests needing a short deadline set it
	// explicitly.
	readTimeout time.Duration
}

// New returns a WlPaste clipboard reader that shells out to the real
// wl-paste binary.
func New() *WlPaste {
	return &WlPaste{execCommand: exec.CommandContext, readTimeout: defaultReadTimeout}
}

func (w *WlPaste) readTimeoutOrDefault() time.Duration {
	if w.readTimeout <= 0 {
		return defaultReadTimeout
	}
	return w.readTimeout
}

// ReadImage implements ports.ClipboardReader. It lists the clipboard's
// offered MIME types, picks the best supported image type (if any), and
// reads it. Any failure (wl-paste missing, no image on the clipboard, a
// read error or timeout) is reported as ports.ErrNoClipboardImage so callers
// uniformly fall back to forwarding the keystroke that triggered the read.
func (w *WlPaste) ReadImage(ctx context.Context) (string, []byte, error) {
	listCtx, cancel := context.WithTimeout(ctx, listTypesTimeout)
	defer cancel()

	listCmd := w.execCommand(listCtx, "wl-paste", "--list-types")
	out, err := listCmd.Output()
	if err != nil {
		return "", nil, ports.ErrNoClipboardImage
	}

	mime := pickImageMime(string(out))
	if mime == "" {
		return "", nil, ports.ErrNoClipboardImage
	}

	readCtx, readCancel := context.WithTimeout(ctx, w.readTimeoutOrDefault())
	defer readCancel()

	readCmd := w.execCommand(readCtx, "wl-paste", "--type", mime, "--no-newline")
	var buf bytes.Buffer
	readCmd.Stdout = &buf
	if err := readCmd.Run(); err != nil {
		return "", nil, ports.ErrNoClipboardImage
	}
	return mime, buf.Bytes(), nil
}

// pickImageMime scans wl-paste --list-types output (one MIME type per line)
// and returns the first match in mimePreference order, or "" if the
// clipboard offers no supported image type.
func pickImageMime(listTypesOutput string) string {
	offered := make(map[string]bool)
	for _, line := range strings.Split(listTypesOutput, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			offered[line] = true
		}
	}
	for _, mime := range mimePreference {
		if offered[mime] {
			return mime
		}
	}
	return ""
}
