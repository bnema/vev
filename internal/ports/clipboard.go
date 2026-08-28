package ports

import (
	"context"
	"errors"
)

// ErrNoClipboardImage is returned by ClipboardReader.ReadImage when the
// clipboard holds no image (or an unsupported type). Callers fall back to
// forwarding whatever keystroke triggered the read.
var ErrNoClipboardImage = errors.New("ports: no image on clipboard")

// ClipboardReader reads an image off the client-side system clipboard for
// forwarding to a remote session's focused pane. Implementations live in
// internal/adapters; use cases only see this interface.
type ClipboardReader interface {
	// ReadImage returns the clipboard's image bytes and MIME type. It returns
	// ErrNoClipboardImage when the clipboard holds no image (or an
	// unsupported type).
	ReadImage(ctx context.Context) (mime string, data []byte, err error)
}
