package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func attachRemoteGeometryClient(t *testing.T, view *remoteView, size domain.Size) (*attachedClient, chan ports.Frame) {
	t.Helper()
	transport, sends := newCapturingTransport(t)
	ac := &attachedClient{tr: transport, output: newOutputStateStream(), size: size}
	ac.initOverlays()
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	return ac, sends
}

func resizeRemoteGeometryAttachment(t *testing.T, d *Daemon, view *remoteView, ac *attachedClient, size domain.Size, remoteSends chan ports.Frame) domain.Size {
	t.Helper()
	token := attachmentOwnerToken(view, ac, ac.transport())
	require.True(t, token.current())
	require.True(t, d.resizeAttachmentForLease(token, size))

	frame := awaitFrame(t, remoteSends, ports.MsgResize)
	resize, err := ports.UnmarshalResize(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, contentSize(size), resize.Size)
	return resize.Size
}

func remoteViewScreenSize(t *testing.T, view *remoteView) domain.Size {
	t.Helper()
	view.mu.Lock()
	defer view.mu.Unlock()
	return domain.Size{Cols: view.screen.Frame.Width, Rows: view.screen.Frame.Height}
}

func TestRemoteViewContentGeometryFollowsLatestValidAttachmentClaim(t *testing.T) {
	d, view, _, remoteTransport := newRemoteMetadataLinkFixture(t)
	t.Cleanup(func() { require.NoError(t, remoteTransport.Close()) })
	first, _ := attachRemoteGeometryClient(t, view, domain.Size{Cols: 80, Rows: 24})
	second, _ := attachRemoteGeometryClient(t, view, domain.Size{Cols: 120, Rows: 40})

	// A smaller, later claim is authoritative even when a larger peer remains
	// attached; shared remote content follows claim order, not max dimensions.
	resizeRemoteGeometryAttachment(t, d, view, first, domain.Size{Cols: 120, Rows: 40}, remoteTransport.sent)
	require.Equal(t, domain.Size{Cols: 120, Rows: 38}, remoteViewScreenSize(t, view))
	resizeRemoteGeometryAttachment(t, d, view, second, domain.Size{Cols: 90, Rows: 30}, remoteTransport.sent)
	require.Equal(t, domain.Size{Cols: 90, Rows: 28}, remoteViewScreenSize(t, view))

	// Invalid claims do not displace the most recent valid geometry.
	token := attachmentOwnerToken(view, first, first.transport())
	require.False(t, d.resizeAttachmentForLease(token, domain.Size{Cols: 0, Rows: 0}))
	require.Equal(t, domain.Size{Cols: 90, Rows: 28}, remoteViewScreenSize(t, view))
}

func TestNonAuthoritativeRemoteAttachmentRenderCannotResizeOrMutateSharedVT(t *testing.T) {
	d, view, _, remoteTransport := newRemoteMetadataLinkFixture(t)
	t.Cleanup(func() { require.NoError(t, remoteTransport.Close()) })
	nonAuthoritative, _ := attachRemoteGeometryClient(t, view, domain.Size{Cols: 60, Rows: 20})
	authoritative, _ := attachRemoteGeometryClient(t, view, domain.Size{Cols: 110, Rows: 35})
	resizeRemoteGeometryAttachment(t, d, view, authoritative, domain.Size{Cols: 120, Rows: 40}, remoteTransport.sent)

	view.mu.Lock()
	view.screen.Write([]byte("shared remote state"))
	before := screenLineText(view.screen, 0)
	view.mu.Unlock()

	state, ok := d.captureRemoteViewRenderState(view, nonAuthoritative, renderCaptureRequest{})
	require.True(t, ok)
	require.Len(t, state.panes, 1)
	require.Equal(t, domain.Size{Cols: 120, Rows: 38}, domain.Size{Cols: state.panes[0].frame.Width, Rows: state.panes[0].frame.Height})

	// The composed snapshot is detached from the persistent remote VT too.
	state.panes[0].frame.Set(0, 0, renderer.Cell{Rune: 'X'})
	view.mu.Lock()
	after := screenLineText(view.screen, 0)
	size := domain.Size{Cols: view.screen.Frame.Width, Rows: view.screen.Frame.Height}
	view.mu.Unlock()
	require.Equal(t, before, after)
	require.Equal(t, domain.Size{Cols: 120, Rows: 38}, size)
}

func TestRemoteResizeForwardingUsesAuthoritativeContentSize(t *testing.T) {
	d, view, _, remoteTransport := newRemoteMetadataLinkFixture(t)
	t.Cleanup(func() { require.NoError(t, remoteTransport.Close()) })
	authoritative, _ := attachRemoteGeometryClient(t, view, domain.Size{Cols: 80, Rows: 24})
	peer, _ := attachRemoteGeometryClient(t, view, domain.Size{Cols: 160, Rows: 50})

	// The authoritative attachment's window is smaller than its peer's. The
	// forwarded remote resize must describe the authoritative content region,
	// exactly once past the local chrome boundary.
	want := domain.Size{Cols: 100, Rows: 28}
	got := resizeRemoteGeometryAttachment(t, d, view, authoritative, domain.Size{Cols: 100, Rows: 30}, remoteTransport.sent)
	require.Equal(t, want, got)
	require.Equal(t, want, remoteViewScreenSize(t, view))

	// Rendering the larger peer is attachment-local and cannot replace the
	// authoritative content size used by the remote link.
	state, ok := d.captureRemoteViewRenderState(view, peer, renderCaptureRequest{})
	require.True(t, ok)
	require.Equal(t, want, domain.Size{Cols: state.panes[0].frame.Width, Rows: state.panes[0].frame.Height})
	require.Equal(t, want, remoteViewScreenSize(t, view))
}
