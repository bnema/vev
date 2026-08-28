package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestSharedPTYGeometryCharacterization(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "rejected claim restores most recent remaining attachment",
			run: func(t *testing.T) {
				sess := &session{}
				first := &attachedClient{}
				first.setGeometry(domain.Geometry{
					Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480,
				})
				second := &attachedClient{}
				second.setGeometry(domain.Geometry{
					Size: domain.Size{Cols: 100, Rows: 30}, PixelWidth: 1000, PixelHeight: 600,
				})
				require.True(t, sess.registerAttachment(first))
				require.True(t, sess.registerAttachment(second))

				claim, claimed := sess.geometry.claimSize(sess, second, domain.Size{Cols: 120, Rows: 40})
				require.True(t, claimed)
				sess.geometry.release(sess, claim)

				source, ok := sess.geometry.sourceSnapshot(sess)
				require.True(t, ok)
				require.Same(t, first, source.attachment)
				require.Equal(t, first.geometrySnapshot(), source.geometry)
			},
		},
		{
			name: "losing shared claim retains attachment local geometry",
			run: func(t *testing.T) {
				pty, releasePTY := newBlockingPTY(t)
				t.Cleanup(releasePTY)
				d := newTestDaemon(t, newFactory(t, pty), stubClock{})
				firstTransport, _ := newCapturingTransport(t)
				sess, first, err := d.route(protocol.Hello{
					Version: protocol.Version, Intent: protocol.IntentNew, Name: "work",
					Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{1},
				}, firstTransport)
				require.NoError(t, err)
				secondTransport, _ := newCapturingTransport(t)
				_, second, err := d.route(protocol.Hello{
					Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work",
					Size: domain.Size{Cols: 100, Rows: 40}, ClientID: [16]byte{2},
				}, secondTransport)
				require.NoError(t, err)

				rc := sess.renderCoordinator()
				lease := rc.attachmentLease(second)
				require.True(t, rc.markAttachmentReady(lease))
				token := sess.captureAttachmentCapability(second, secondTransport)
				token.lease = lease
				second.installTestAttachmentCapability(token)
				effect, admitted := second.beginAttachmentEffect(token)
				require.True(t, admitted)
				defer effect.End()

				requested := domain.Geometry{
					Size: domain.Size{Cols: 120, Rows: 50}, PixelWidth: 1200, PixelHeight: 1000,
				}
				var superseded bool
				d.beforeSessionResizePublication = func() {
					if superseded {
						return
					}
					superseded = true
					_, claimed := sess.geometry.claimAttachment(sess, first)
					require.True(t, claimed)
				}
				d.resizeAttachmentGeometryForLease(effect, requested)

				require.Equal(t, requested, second.geometrySnapshot(), "a losing shared claim must retain its Attachment-local window")
				source, ok := sess.geometry.sourceSnapshot(sess)
				require.True(t, ok)
				require.Same(t, first, source.attachment)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
