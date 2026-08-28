package daemon

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestSessionPaneGeometryUsesLatestAttachmentClaimAndTruncates(t *testing.T) {
	sess := &session{}
	first := &attachedClient{}
	first.setGeometry(domain.Geometry{
		Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480,
	})
	second := &attachedClient{}
	second.setGeometry(domain.Geometry{
		Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 803, PixelHeight: 487,
	})

	require.True(t, sess.registerAttachment(first))
	require.True(t, sess.registerAttachment(second))
	require.Equal(t, domain.Geometry{
		Size: domain.Size{Cols: 39, Rows: 23}, PixelWidth: 391, PixelHeight: 466,
	}, sess.geometry.paneGeometry(domain.Size{Cols: 39, Rows: 23}))

	require.True(t, sess.unregisterAttachment(second))
	require.Equal(t, domain.Geometry{
		Size: domain.Size{Cols: 39, Rows: 23}, PixelWidth: 390, PixelHeight: 460,
	}, sess.geometry.paneGeometry(domain.Size{Cols: 39, Rows: 23}))
}

func TestPreparedLayoutPropagatesGeometryToPTYAndScreenQueries(t *testing.T) {
	geometry := domain.Geometry{
		Size: domain.Size{Cols: 39, Rows: 23}, PixelWidth: 391, PixelHeight: 466,
	}
	pty := &recordingGeometryPTY{}
	pane := &pane{pty: pty, screen: vt.NewScreen(1, 1), rect: domain.Rect{Width: 1, Height: 1}}
	plan := preparedTabLayout{members: []resizeMember{{pane: pane, rect: domain.Rect{Width: 39, Height: 23}, geometry: geometry}}}
	daemon := &Daemon{log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	geometryModule := &sharedPTYGeometry{}
	geometryModule.applyPreparedTabMembers(daemon, &plan)
	require.True(t, commitPreparedTabLayoutLocked(&plan))
	require.Equal(t, geometry, pty.geometry)
	require.Equal(t, vt.Geometry{
		Cols: 39, Rows: 23, PixelWidth: 391, PixelHeight: 466,
	}, pane.screen.Geometry())

	var responses string
	pane.screen.OnResponse = func(response []byte) { responses += string(response) }
	pane.screen.Write([]byte("\x1b[14t\x1b[16t"))
	require.Equal(t, "\x1b[4;466;391t\x1b[6;20;10t", responses)
}

type recordingGeometryPTY struct {
	geometry domain.Geometry
}

func (p *recordingGeometryPTY) Read([]byte) (int, error)    { return 0, io.EOF }
func (p *recordingGeometryPTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *recordingGeometryPTY) Close() error                { return nil }
func (p *recordingGeometryPTY) Resize(geometry domain.Geometry) error {
	p.geometry = geometry
	return nil
}
func (p *recordingGeometryPTY) Pid() int                     { return 1 }
func (p *recordingGeometryPTY) ForegroundPgid() (int, error) { return 1, nil }

func TestSizeOnlyGeometryClaimsPreservePixelsAndFallback(t *testing.T) {
	sess := &session{}
	first := &attachedClient{}
	first.setGeometry(domain.Geometry{
		Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480,
	})
	second := &attachedClient{}
	second.setGeometry(domain.Geometry{
		Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 900, PixelHeight: 540,
	})
	require.True(t, sess.registerAttachment(first))
	require.True(t, sess.registerAttachment(second))

	_, claimed := sess.geometry.claimSize(sess, second, domain.Size{Cols: 80, Rows: 24})
	require.True(t, claimed)
	require.Equal(t, domain.Geometry{
		Size: domain.Size{Cols: 39, Rows: 23}, PixelWidth: 438, PixelHeight: 517,
	}, sess.geometry.paneGeometry(domain.Size{Cols: 39, Rows: 23}))

	require.True(t, sess.unregisterAttachment(second))
	require.Equal(t, domain.Geometry{
		Size: domain.Size{Cols: 39, Rows: 23}, PixelWidth: 390, PixelHeight: 460,
	}, sess.geometry.paneGeometry(domain.Size{Cols: 39, Rows: 23}))

	partial := &attachedClient{}
	partial.setGeometry(domain.Geometry{
		Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 901,
	})
	require.True(t, sess.registerAttachment(partial))
	require.Equal(t, domain.Geometry{Size: domain.Size{Cols: 39, Rows: 23}}, sess.geometry.paneGeometry(domain.Size{Cols: 39, Rows: 23}), "partial pixels must remain cell-only")
	require.True(t, sess.unregisterAttachment(partial))
	require.Equal(t, domain.Geometry{
		Size: domain.Size{Cols: 39, Rows: 23}, PixelWidth: 390, PixelHeight: 460,
	}, sess.geometry.paneGeometry(domain.Size{Cols: 39, Rows: 23}), "fallback must restore the remaining complete pair")
}

func TestSizeOnlyLayoutAndFloatingRetainClaimingPixelGeometry(t *testing.T) {
	full := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480}
	ac := &attachedClient{}
	ac.setGeometry(full)
	sess := &session{sessionCore: sessionCore{id: "geometry"}}
	require.True(t, sess.registerAttachment(ac))

	layoutPTY := &recordingGeometryPTY{}
	tb := newTab(layoutPTY, domain.Size{Cols: 80, Rows: 22})
	sess.tabs = []*tab{tb}
	d := newTestDaemon(t, nil, stubClock{})
	require.True(t, sess.geometry.applyTabLayout(d, sess, tb))
	require.Equal(t, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 22}, PixelWidth: 800, PixelHeight: 440}, layoutPTY.geometry)

	floatingPTY := &recordingGeometryPTY{}
	cfg := domain.FloatingConfig{Width: 50, Height: 50}
	d.ApplyConfig(domain.Config{Floating: cfg})
	floatingGeometry := calculateContentFloatingGeometry(tb.size, cfg)
	floating := newPane("floating", floatingPTY, rectSize(floatingGeometry.Inner))
	floating.popupGeometry = floatingGeometry
	tb.floating = floatingSlot{state: floatingVisible, pane: floating, generation: 1}
	failed, ok := sess.geometry.applyVisibleFloatingLayout(d, sess, tb, nil)
	require.True(t, ok)
	require.Empty(t, failed)
	ptyRect := floatingGeometry.ptyRect()
	require.Equal(t, domain.Geometry{
		Size:       domain.Size{Cols: ptyRect.Width, Rows: ptyRect.Height},
		PixelWidth: 800 * ptyRect.Width / 80, PixelHeight: 480 * ptyRect.Height / 24,
	}, floatingPTY.geometry)
}

type recordingPTYFactory struct {
	pty      ports.PTY
	geometry domain.Geometry
}

func (f *recordingPTYFactory) Open(_ context.Context, _ string, _ []string, _ []string, _ string, geometry domain.Geometry) (ports.PTY, error) {
	f.geometry = geometry
	return f.pty, nil
}

type blockingRecordingGeometryPTY struct {
	mu       sync.Mutex
	geometry domain.Geometry
	done     chan struct{}
	once     sync.Once
}

func (p *blockingRecordingGeometryPTY) Read([]byte) (int, error) {
	<-p.done
	return 0, io.EOF
}
func (p *blockingRecordingGeometryPTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *blockingRecordingGeometryPTY) Close() error {
	p.once.Do(func() { close(p.done) })
	return nil
}
func (p *blockingRecordingGeometryPTY) Resize(geometry domain.Geometry) error {
	p.mu.Lock()
	p.geometry = geometry
	p.mu.Unlock()
	return nil
}
func (p *blockingRecordingGeometryPTY) geometrySnapshot() domain.Geometry {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.geometry
}
func (*blockingRecordingGeometryPTY) Pid() int                     { return 1 }
func (*blockingRecordingGeometryPTY) ForegroundPgid() (int, error) { return 1, nil }

func TestInitialSessionPTYReceivesClaimingPixelGeometry(t *testing.T) {
	pty := &recordingGeometryPTY{}
	factory := &recordingPTYFactory{pty: pty}
	d := newTestDaemon(t, factory, stubClock{})
	geometry := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480}

	d.mu.Lock()
	sess, err := d.createSessionLockedWithMode("pixels", true, "/tmp", geometry, nil)
	d.mu.Unlock()
	require.NoError(t, err)
	want := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 22}, PixelWidth: 800, PixelHeight: 440}
	require.Equal(t, want, factory.geometry)
	require.Equal(t, want, sess.tabs[0].focusedPane().geometry)
	require.Equal(t, vt.Geometry{Cols: 80, Rows: 22, PixelWidth: 800, PixelHeight: 440}, sess.tabs[0].focusedPane().screen.Geometry())
}

func TestResumeReappliesClaimingPixelGeometryBeforeReturn(t *testing.T) {
	pty := &blockingRecordingGeometryPTY{done: make(chan struct{})}
	t.Cleanup(func() { _ = pty.Close() })
	d := newTestDaemon(t, &recordingPTYFactory{pty: pty}, stubClock{})
	firstHello := helloResumeCapable(protocol.IntentNew, "pixels", 0)
	firstHello.PixelWidth, firstHello.PixelHeight = 800, 480
	firstTransport := &closeTrackingTransport{}
	sess, ac, err := d.route(firstHello, firstTransport)
	require.NoError(t, err)
	d.clientGone(sess, ac, firstTransport, false)

	resumeHello := helloResumeCapable(protocol.IntentResume, "pixels", ac.resumeToken)
	resumeHello.PixelWidth, resumeHello.PixelHeight = 1000, 600
	resumed, same, ok, err := d.resumeParked(resumeHello, &closeTrackingTransport{}, resumeHello.Size)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumed)
	require.Same(t, ac, same)
	require.Equal(t, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 22}, PixelWidth: 1000, PixelHeight: 550}, pty.geometrySnapshot())
}
