package brokeripc

import (
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/protoconv"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Exercise the actual stream byte pipe used beneath the broker's preview
// session codec: the session envelope exceeds the opaque chunk ceiling.
func TestPackedPreviewAcrossStreamChunks(t *testing.T) {
	for _, size := range [][2]int{{145, 50}, {256, 128}} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			p := protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, LifecycleID: previewTarget().LifecycleID, TabID: previewTarget().LiveTabID, Revision: 1, Width: uint16(size[0]), Height: uint16(size[1]), Cells: make([]renderer.Cell, size[0]*size[1])}
			for i := range p.Cells {
				s := renderer.DefaultStyle()
				s.HasForegroundRGB = true
				s.ForegroundRGB = renderer.RGB{R: uint8(i), G: uint8(i >> 8), B: 255}
				p.Cells[i] = renderer.Cell{Rune: 'x', Style: s}
			}
			protocol.CapRemotePreviewStyles(p.Cells)
			w, err := protoconv.RemotePreviewToWire(p)
			require.NoError(t, err)
			raw, err := proto.Marshal(&wire.ServerEnvelope{Payload: &wire.ServerEnvelope_RemotePreview{RemotePreview: w}})
			require.NoError(t, err)
			require.Greater(t, len(raw), int(brokerwire.MaxStreamChunkBytes))
			receiver := newStreamPipe(int(brokerwire.MaxStreamChunkBytes), func([]byte) error { return nil }, func(uint64) error { return nil })
			chunks := 0
			sender := newStreamPipe(int(brokerwire.MaxStreamChunkBytes), func(data []byte) error {
				chunks++
				if len(data) > int(brokerwire.MaxStreamChunkBytes) {
					return fmt.Errorf("oversized chunk")
				}
				return receiver.deliver(data)
			}, func(uint64) error { return nil })
			defer sender.closeWith(nil)
			defer receiver.closeWith(nil)
			framed := make([]byte, 4+len(raw))
			binary.BigEndian.PutUint32(framed, uint32(len(raw)))
			copy(framed[4:], raw)
			_, err = sender.Write(framed)
			require.NoError(t, err)
			require.Greater(t, chunks, 1)
			var header [4]byte
			_, err = io.ReadFull(receiver, header[:])
			require.NoError(t, err)
			body := make([]byte, int(binary.BigEndian.Uint32(header[:])))
			_, err = io.ReadFull(receiver, body)
			require.NoError(t, err)
			got, err := sessionwire.DecodeServerEnvelope(body)
			require.NoError(t, err)
			require.Equal(t, p, got)
		})
	}
}
