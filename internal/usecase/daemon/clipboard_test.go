package daemon

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

func TestHandleImagePushWritesFileWithExactBytesAndMode0600(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	d.tempDir = t.TempDir()

	data := []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x01, 0x02, 0x03}
	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: data})

	injected := <-writes
	path := string(injected)
	require.FileExists(t, path)
	require.Equal(t, ".png", filepath.Ext(path))
	require.True(t, filepath.IsAbs(path))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, got)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestHandleImagePushInjectsPathWithoutBracketedPasteWrapping(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	d.tempDir = t.TempDir()
	pane := sess.tabs[0].focusedPane()
	require.False(t, pane.screen.BracketedPasteMode())

	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: []byte("x")})

	injected := <-writes
	require.False(t, hasBracketedPasteMarkers(injected), "path must not be wrapped when the pane is not in bracketed-paste mode")
	require.FileExists(t, string(injected))
}

func TestHandleImagePushInjectsPathWithBracketedPasteWrapping(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	d.tempDir = t.TempDir()
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("\x1b[?2004h"))
	require.True(t, pane.screen.BracketedPasteMode())

	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: []byte("x")})

	injected := <-writes
	require.True(t, len(injected) > len(clipPasteOpenMarker)+len(clipPasteCloseMarker))
	require.Equal(t, clipPasteOpenMarker, injected[:len(clipPasteOpenMarker)])
	require.Equal(t, clipPasteCloseMarker, injected[len(injected)-len(clipPasteCloseMarker):])
	path := string(injected[len(clipPasteOpenMarker) : len(injected)-len(clipPasteCloseMarker)])
	require.FileExists(t, path)
}

func hasBracketedPasteMarkers(b []byte) bool {
	return len(b) >= len(clipPasteOpenMarker) &&
		string(b[:len(clipPasteOpenMarker)]) == string(clipPasteOpenMarker)
}

func TestHandleImagePushRejectsOversizedPayloadWithoutWritingOrInjecting(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	dir := t.TempDir()
	d.tempDir = dir

	huge := make([]byte, maxImagePushSize+1)
	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: huge})

	select {
	case w := <-writes:
		t.Fatalf("oversized push must not be injected into the pane, got %q", w)
	default:
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "oversized push must not write a temp file")
}

func TestHandleImagePushIgnoresEmptyPayload(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	dir := t.TempDir()
	d.tempDir = dir

	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: nil})

	select {
	case w := <-writes:
		t.Fatalf("empty push must not be injected, got %q", w)
	default:
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestClipboardExtMapsKnownMimeTypesAndFallsBackToBin(t *testing.T) {
	cases := map[string]string{
		"image/png":                "png",
		"image/jpeg":               "jpg",
		"image/webp":               "webp",
		"image/gif":                "gif",
		"image/bmp":                "bin",
		"":                         "bin",
		"application/octet-stream": "bin",
	}
	for mime, want := range cases {
		require.Equal(t, want, clipboardExt(mime), "mime %q", mime)
	}
}

func TestKillSessionRemovesClipboardTempFiles(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	d.tempDir = t.TempDir()
	// killSession requires the session to be registered in the daemon's
	// registry (newManualSessionWithPTYs already does this via d.sessions).

	d.handleImagePush(sess, ports.ImagePush{Mime: "image/png", Data: []byte("x")})
	injected := <-writes
	path := string(injected)
	require.FileExists(t, path)

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	require.NoFileExists(t, path)
}
