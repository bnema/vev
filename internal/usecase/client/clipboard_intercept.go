package client

import (
	"bytes"
	"context"
	"errors"
	"log/slog"

	"github.com/bnema/vev/internal/ports"
)

// ctrlV is the byte a Ctrl+V keypress sends on the wire.
const ctrlV = 0x16

// maxClipboardImagePush caps the image the client will send in one
// MsgImagePush, mirroring the daemon's independent cap. A 16 MiB frame limit
// means one frame always suffices under this cap.
const maxClipboardImagePush = 10 << 20 // 10 MiB

// clipboardIntercept sits between the terminal's theme scanner and the paste
// coalescer on a remote attach. It splits pass-through stdin bytes on Ctrl+V
// (0x16): each occurrence triggers a clipboard image read, sent as one
// MsgImagePush frame instead of the keystroke. Everything else — ordinary
// bytes, and any 0x16 that arrives while the coalescer is mid-paste (pasted
// text may legitimately contain it) — is forwarded to next unchanged, so it
// still gets the coalescer's marker-splitting treatment.
type clipboardIntercept struct {
	coalescer *pasteCoalescer
	reader    ports.ClipboardReader
	log       *slog.Logger
	sendImage func(mime string, data []byte)
	next      func([]byte)
}

// Scan implements the same func([]byte) shape as pasteCoalescer.Scan, so it
// can be used interchangeably as the scanner sink.
//
// It must never scan for Ctrl+V past the point where a bracketed paste
// starts (or might start): pasted text can legitimately contain 0x16, and
// the paste's opening marker can itself be split across two reads. So:
//
//  1. If the coalescer is already Buffering (mid-paste, or holding a
//     trailing strict prefix of the opening marker from a previous call),
//     the whole chunk is handed to next untouched.
//  2. Otherwise, this chunk is scanned only up to the first byte that either
//     starts the opening marker in full or could be the start of a marker
//     split across this chunk's end (pasteMarkerBoundary); everything from
//     that point onward is handed to next as one piece, unscanned, letting
//     the coalescer's own state decide what happens to it (and to any
//     bytes trailing an in-chunk paste — see the package-level note below).
func (c *clipboardIntercept) Scan(data []byte) {
	if c.coalescer.Buffering() {
		c.next(data)
		return
	}

	boundary := pasteMarkerBoundary(data)
	segment := data[:boundary]
	for len(segment) > 0 {
		idx := bytes.IndexByte(segment, ctrlV)
		if idx < 0 {
			c.next(segment)
			break
		}
		if idx > 0 {
			c.next(segment[:idx])
		}
		segment = segment[idx+1:]
		c.handleCtrlV()
	}

	// Bytes at/after boundary are (the possible start of) a bracketed
	// paste: handed off whole, so any Ctrl+V trailing a paste that closes
	// within this same chunk is not intercepted either. In practice a real
	// keypress never arrives concatenated with paste bytes in one read, so
	// this is a deliberate, narrow trade-off in favor of never breaking
	// paste content.
	if tail := data[boundary:]; len(tail) > 0 {
		c.next(tail)
	}
}

// pasteMarkerBoundary returns the index in data at or after which bytes must
// not be scanned for Ctrl+V because they start (or, at the very end of data,
// might be a split-across-reads prefix of) the bracketed-paste opening
// marker. It returns len(data) when neither is present.
func pasteMarkerBoundary(data []byte) int {
	if idx := bytes.Index(data, pasteOpenMarker); idx >= 0 {
		return idx
	}
	if p := trailingOpenPrefixLen(data); p > 0 {
		return len(data) - p
	}
	return len(data)
}

func (c *clipboardIntercept) handleCtrlV() {
	mime, data, err := c.reader.ReadImage(context.Background())
	if err != nil {
		if !errors.Is(err, ports.ErrNoClipboardImage) {
			c.log.Warn("clipboard image read failed", "err", err)
		}
		c.next([]byte{ctrlV})
		return
	}
	if len(data) > maxClipboardImagePush {
		c.log.Warn("clipboard image too large to send, forwarding Ctrl+V instead", "size", len(data), "cap", maxClipboardImagePush)
		c.next([]byte{ctrlV})
		return
	}
	c.sendImage(mime, data)
}
