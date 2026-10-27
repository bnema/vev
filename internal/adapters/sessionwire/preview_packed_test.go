package sessionwire

import (
	"fmt"
	"testing"

	renderer "bnema.dev/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestPackedPreviewEnvelope(t *testing.T) {
	for _, size := range [][2]int{{145, 50}, {256, 128}} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			p := protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, LifecycleID: domain.SessionLifecycleID{1}, TabID: "tab-1", Revision: 1, Width: uint16(size[0]), Height: uint16(size[1]), Cells: make([]renderer.Cell, size[0]*size[1])}
			for i := range p.Cells {
				s := renderer.DefaultStyle()
				s.Bold = true
				s.Italic = true
				s.Inverse = true
				s.Attrs = renderer.AttrDim | renderer.AttrUnderline | renderer.AttrBlink | renderer.AttrStrikethrough
				s.Foreground = 32767
				s.Background = 32767
				s.UnderlineColor = 32767
				s.UnderlineStyle = renderer.UnderlineDashed
				s.HasForegroundRGB = true
				s.HasBackgroundRGB = true
				s.HasUnderlineColor = true
				s.HasUnderlineColorRGB = true
				s.ForegroundRGB = renderer.RGB{R: 255, G: 255, B: 255}
				s.BackgroundRGB = s.ForegroundRGB
				s.UnderlineColorRGB = renderer.RGB{R: 255, G: uint8((i%1024)/256 + 128), B: uint8(i % 256)}
				p.Cells[i] = renderer.Cell{Rune: 'x', Style: s}
			}
			env, err := encodeProtoServer(p)
			require.NoError(t, err)
			raw, err := proto.Marshal(env)
			require.NoError(t, err)
			var count func(protoreflect.Message) int
			count = func(m protoreflect.Message) int {
				n := 0
				m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
					if f.IsList() {
						l := v.List()
						n += l.Len()
						if f.Message() != nil {
							for i := 0; i < l.Len(); i++ {
								n += count(l.Get(i).Message())
							}
						}
					} else {
						n++
						if f.Message() != nil {
							n += count(v.Message())
						}
					}
					return true
				})
				return n
			}
			t.Logf("encoded bytes=%d scanner fields=%d", len(raw), count(env.ProtoReflect()))
			got, err := DecodeServerEnvelope(raw)
			require.NoError(t, err)
			require.Equal(t, p, got)
			for _, tc := range []struct {
				name string
				raw  []byte
			}{{"truncated prefix", raw[:len(raw)-1]}, {"trailing garbage", append(append([]byte(nil), raw...), 0xff)}} {
				t.Run(tc.name, func(t *testing.T) { _, err := DecodeServerEnvelope(tc.raw); require.Error(t, err) })
			}
		})
	}
}
