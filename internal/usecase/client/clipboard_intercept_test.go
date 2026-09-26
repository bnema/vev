package client

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestClipboardInterceptCtrlV(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		hasImage   bool
		reads      bool
		wantNext   []string
		wantImages int
	}{
		{name: "legacy with image", in: "a\x16b", hasImage: true, wantNext: []string{"a", "b"}, wantImages: 1},
		{name: "kitty with image", in: "a\x1b[118;5ub", hasImage: true, wantNext: []string{"a", "b"}, wantImages: 1},
		{name: "legacy without image forwards byte", in: "\x16", reads: true, wantNext: []string{"\x16"}},
		{name: "kitty without image forwards sequence", in: "\x1b[118;5u", reads: true, wantNext: []string{"\x1b[118;5u"}},
		{name: "kitty cyrillic layout base key", in: "\x1b[1084::118;5u", hasImage: true, wantImages: 1},
		{name: "kitty with caps lock", in: "\x1b[118;69u", hasImage: true, wantImages: 1},
		{name: "kitty release ignored", in: "\x1b[118;5:3u", wantNext: []string{"\x1b[118;5:3u"}},
		{name: "other kitty key untouched", in: "\x1b[118;7u", wantNext: []string{"\x1b[118;7u"}},
		{name: "inside paste untouched", in: "\x1b[200~\x1b[118;5u\x1b[201~", wantNext: []string{"\x1b[200~\x1b[118;5u\x1b[201~"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := portsmocks.NewMockClipboardReader(t)
			if tt.hasImage {
				reader.EXPECT().ReadImage(mock.Anything).Return("image/png", []byte{1}, nil).Times(tt.wantImages)
			} else if tt.reads {
				reader.EXPECT().ReadImage(mock.Anything).Return("", nil, ports.ErrNoClipboardImage).Once()
			}
			var next []string
			images := 0
			c := &clipboardIntercept{
				coalescer: newPasteCoalescer(nil, func([]byte) {}),
				reader:    reader,
				log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
				sendImage: func(string, []byte) { images++ },
				next:      func(b []byte) { next = append(next, string(b)) },
			}
			c.Scan([]byte(tt.in))
			require.Equal(t, tt.wantNext, next)
			require.Equal(t, tt.wantImages, images)
		})
	}
}
