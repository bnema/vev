package client

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// ciFakeReader is a directly-controllable ports.ClipboardReader for
// white-box interceptor tests (this file lives in package client, alongside
// paste_coalescer_test.go, so it can drive pasteCoalescer.Scan directly).
type ciFakeReader struct {
	mime string
	data []byte
	err  error
	n    int
}

func (r *ciFakeReader) ReadImage(context.Context) (string, []byte, error) {
	r.n++
	return r.mime, r.data, r.err
}

func newTestClipboardIntercept(reader ports.ClipboardReader) (*clipboardIntercept, *pasteCoalescer, *pcFakeClock, *pcCollector, *[]protocol.ImagePush) {
	clk := &pcFakeClock{}
	col := &pcCollector{}
	pc := newPasteCoalescer(clk, col.emit)
	var images []protocol.ImagePush
	ci := &clipboardIntercept{
		coalescer: pc,
		reader:    reader,
		log:       slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelError + 1})),
		sendImage: func(mime string, data []byte) {
			images = append(images, protocol.ImagePush{Mime: mime, Data: data})
		},
		next: pc.Scan,
	}
	return ci, pc, clk, col, &images
}

func TestClipboardInterceptCtrlVWithImageSendsNoPassthrough(t *testing.T) {
	reader := &ciFakeReader{mime: "image/png", data: []byte("PNGDATA")}
	ci, pc, _, col, images := newTestClipboardIntercept(reader)
	defer pc.Close()

	ci.Scan([]byte("a"))
	ci.Scan([]byte{ctrlV})
	ci.Scan([]byte("b"))

	require.Equal(t, 1, reader.n, "clipboard must be read exactly once per Ctrl+V")
	require.Equal(t, []protocol.ImagePush{{Mime: "image/png", Data: []byte("PNGDATA")}}, *images)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, col.snapshot(), "0x16 itself must never reach the coalescer/pane")
}

func TestClipboardInterceptCtrlVNoImageForwardsByte(t *testing.T) {
	reader := &ciFakeReader{err: ports.ErrNoClipboardImage}
	ci, pc, _, col, images := newTestClipboardIntercept(reader)
	defer pc.Close()

	ci.Scan([]byte("a\x16b"))

	require.Empty(t, *images)
	require.Equal(t, [][]byte{[]byte("a"), {ctrlV}, []byte("b")}, col.snapshot())
}

func TestClipboardInterceptCtrlVOtherErrorForwardsByte(t *testing.T) {
	reader := &ciFakeReader{err: errors.New("boom")}
	ci, pc, _, col, images := newTestClipboardIntercept(reader)
	defer pc.Close()

	ci.Scan([]byte{ctrlV})

	require.Empty(t, *images)
	require.Equal(t, [][]byte{{ctrlV}}, col.snapshot())
}

func TestClipboardInterceptFailureNotifiesDaemonAndForwardsCtrlV(t *testing.T) {
	tests := []struct {
		name   string
		reader ports.ClipboardReader
		want   uint8
	}{
		{name: "image read failure", reader: &ciFakeReader{err: errors.New("boom")}, want: ports.ClientNoticeClipboardFallback},
		{name: "oversized image", reader: &ciFakeReader{mime: "image/png", data: make([]byte, maxClipboardImagePush+1)}, want: ports.ClientNoticeClipboardTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ci, pc, _, col, images := newTestClipboardIntercept(tt.reader)
			defer pc.Close()
			var actions []uint8
			ci.sendNotice = func(action uint8) { actions = append(actions, action) }

			ci.Scan([]byte{ctrlV})

			require.Equal(t, []uint8{tt.want}, actions)
			require.Empty(t, *images)
			require.Equal(t, [][]byte{{ctrlV}}, col.snapshot())
		})
	}
}

func TestClipboardInterceptOversizedImageForwardsByteInsteadOfSwallowing(t *testing.T) {
	huge := make([]byte, maxClipboardImagePush+1)
	reader := &ciFakeReader{mime: "image/png", data: huge}
	ci, pc, _, col, images := newTestClipboardIntercept(reader)
	defer pc.Close()

	ci.Scan([]byte{ctrlV})

	require.Empty(t, *images, "an oversized image must not be sent")
	require.Equal(t, [][]byte{{ctrlV}}, col.snapshot(), "an oversized image must fall back to forwarding Ctrl+V, not drop it")
}

func TestClipboardInterceptMultipleCtrlVInOneChunk(t *testing.T) {
	reader := &ciFakeReader{mime: "image/png", data: []byte("X")}
	ci, pc, _, col, images := newTestClipboardIntercept(reader)
	defer pc.Close()

	ci.Scan([]byte("a\x16b\x16c"))

	require.Equal(t, 2, reader.n)
	require.Len(t, *images, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, col.snapshot())
}

func TestClipboardInterceptCtrlVInsideBracketedPasteSameChunkNotIntercepted(t *testing.T) {
	reader := &ciFakeReader{mime: "image/png", data: []byte("X")}
	ci, pc, _, col, images := newTestClipboardIntercept(reader)
	defer pc.Close()

	paste := []byte("\x1b[200~pa\x16ste\x1b[201~")
	ci.Scan(paste)

	require.Zero(t, reader.n, "clipboard must never be consulted for a Ctrl+V inside a paste")
	require.Empty(t, *images)
	require.Equal(t, [][]byte{paste}, col.snapshot(), "the paste must reach the coalescer whole, byte-identical")
}

func TestClipboardInterceptCtrlVInsideBracketedPasteAcrossChunksNotIntercepted(t *testing.T) {
	reader := &ciFakeReader{mime: "image/png", data: []byte("X")}
	ci, pc, _, col, images := newTestClipboardIntercept(reader)
	defer pc.Close()

	paste := []byte("\x1b[200~pa\x16ste\x1b[201~")
	for s := 1; s < len(paste); s++ {
		t.Run("", func(t *testing.T) {
			reader.n = 0
			*images = nil
			col.emits = nil
			ci.Scan(paste[:s])
			ci.Scan(paste[s:])
			require.Zero(t, reader.n, "split at %d: clipboard must never be consulted for a Ctrl+V inside a paste", s)
			require.Empty(t, *images)
			require.Equal(t, [][]byte{paste}, col.snapshot(), "split at %d: the paste must reach the coalescer whole", s)
		})
	}
}

func TestClipboardInterceptCtrlVAfterLoneEscapePendingNotIntercepted(t *testing.T) {
	// A trailing lone ESC (a strict prefix of the open marker) is held by
	// the coalescer as "pending" without yet flipping Buffering() via the
	// old (buffering-only) definition; Buffering() must also cover pending
	// so a Ctrl+V arriving before the flush timer fires is never
	// misinterpreted mid-marker.
	reader := &ciFakeReader{mime: "image/png", data: []byte("X")}
	ci, pc, _, col, images := newTestClipboardIntercept(reader)
	defer pc.Close()

	ci.Scan([]byte{0x1b}) // held as pending, awaiting more marker bytes
	require.True(t, pc.Buffering(), "a pending trailing marker-prefix must count as Buffering")

	ci.Scan([]byte{ctrlV})

	require.Zero(t, reader.n, "clipboard must not be consulted while a marker prefix is pending")
	require.Empty(t, *images)
	// The ESC-then-Ctrl+V recombine as ordinary bytes handed whole to the
	// coalescer, which does not find a marker in "\x1bfamiliar" and emits
	// it as plain text once no further marker bytes follow.
	require.Equal(t, [][]byte{{0x1b, ctrlV}}, col.snapshot())
}
