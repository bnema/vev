package daemon

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/mock"
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
	pty, ptyGeometry := newRecordingGeometryPTY(t, false)
	pane := &pane{pty: pty, screen: vt.NewScreen(1, 1), rect: domain.Rect{Width: 1, Height: 1}}
	plan := preparedTabLayout{members: []resizeMember{{pane: pane, rect: domain.Rect{Width: 39, Height: 23}, geometry: geometry}}}
	daemon := &Daemon{log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	geometryModule := &sharedPTYGeometry{}
	geometryModule.applyPreparedTabMembers(daemon, &plan)
	require.True(t, commitPreparedTabLayoutLocked(&plan))
	require.Equal(t, geometry, ptyGeometry.snapshot())
	require.Equal(t, vt.Geometry{
		Cols: 39, Rows: 23, PixelWidth: 391, PixelHeight: 466,
	}, pane.screen.Geometry())

	var responses string
	pane.screen.OnResponse = func(response []byte) { responses += string(response) }
	pane.screen.Write([]byte("\x1b[14t\x1b[16t"))
	require.Equal(t, "\x1b[4;466;391t\x1b[6;20;10t", responses)
}

// geometryRecorder holds the last geometry a recording MockPTY was resized to.
type geometryRecorder struct {
	mu       sync.Mutex
	geometry domain.Geometry
	done     chan struct{}
	once     sync.Once
}

func (r *geometryRecorder) set(g domain.Geometry) {
	r.mu.Lock()
	r.geometry = g
	r.mu.Unlock()
}

func (r *geometryRecorder) snapshot() domain.Geometry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.geometry
}

func (r *geometryRecorder) close() { r.once.Do(func() { close(r.done) }) }

// newRecordingGeometryPTY returns a MockPTY that records Resize geometry. When
// blocking, Read parks until Close; otherwise Read reports io.EOF at once.
func newRecordingGeometryPTY(t testing.TB, blocking bool) (*portsmocks.MockPTY, *geometryRecorder) {
	t.Helper()
	rec := &geometryRecorder{done: make(chan struct{})}
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Read(mock.Anything).RunAndReturn(func([]byte) (int, error) {
		if blocking {
			<-rec.done
		}
		return 0, io.EOF
	}).Maybe()
	p.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) { return len(b), nil }).Maybe()
	p.EXPECT().Close().RunAndReturn(func() error { rec.close(); return nil }).Maybe()
	p.EXPECT().Resize(mock.Anything).RunAndReturn(func(g domain.Geometry) error { rec.set(g); return nil }).Maybe()
	p.EXPECT().Pid().Return(1).Maybe()
	p.EXPECT().ForegroundPgid().Return(1, nil).Maybe()
	return p, rec
}

// newRecordingPTYFactory returns a MockPTYFactory that hands out pty and records
// the geometry of the last Open.
func newRecordingPTYFactory(t testing.TB, pty ports.PTY) (*portsmocks.MockPTYFactory, *geometryRecorder) {
	t.Helper()
	rec := &geometryRecorder{done: make(chan struct{})}
	f := portsmocks.NewMockPTYFactory(t)
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ []string, _ []string, _ string, g domain.Geometry) (ports.PTY, error) {
			rec.set(g)
			return pty, nil
		}).Maybe()
	return f, rec
}

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

	layoutPTY, layoutGeometry := newRecordingGeometryPTY(t, false)
	tb := newTab(layoutPTY, domain.Size{Cols: 80, Rows: 22})
	sess.tabs = []*tab{tb}
	d := newTestDaemon(t, nil, newStubClock(t))
	require.True(t, sess.geometry.applyTabLayout(d, sess, tb))
	require.Equal(t, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 22}, PixelWidth: 800, PixelHeight: 440}, layoutGeometry.snapshot())

	floatingPTY, floatingGeometryRec := newRecordingGeometryPTY(t, false)
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
	}, floatingGeometryRec.snapshot())
}

func TestInitialSessionPTYReceivesClaimingPixelGeometry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, _ := newRecordingGeometryPTY(t, false)
		factory, openGeometry := newRecordingPTYFactory(t, pty)
		d := newTestDaemon(t, factory, newStubClock(t))
		geometry := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480}

		d.mu.Lock()
		sess, err := d.createSessionLockedWithMode("pixels", true, "/tmp", geometry, nil)
		d.mu.Unlock()
		require.NoError(t, err)
		want := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 22}, PixelWidth: 800, PixelHeight: 440}
		require.Equal(t, want, openGeometry.snapshot())
		require.Equal(t, want, sess.tabs[0].focusedPane().geometry)
		require.Equal(t, vt.Geometry{Cols: 80, Rows: 22, PixelWidth: 800, PixelHeight: 440}, sess.tabs[0].focusedPane().screen.Geometry())
	})
}

func TestResumeReappliesClaimingPixelGeometryBeforeReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, ptyGeometry := newRecordingGeometryPTY(t, true)
		t.Cleanup(func() { _ = pty.Close() })
		factory, _ := newRecordingPTYFactory(t, pty)
		d := newTestDaemon(t, factory, newStubClock(t))
		firstHello := helloResumeCapable(protocol.IntentNew, "pixels", 0)
		firstHello.PixelWidth, firstHello.PixelHeight = 800, 480
		firstTransport := newQuietConnection(t)
		sess, ac, err := d.route(firstHello, firstTransport)
		require.NoError(t, err)
		d.clientGone(sess, ac, firstTransport, false)

		resumeHello := helloResumeCapable(protocol.IntentResume, "pixels", ac.resumeToken)
		resumeHello.PixelWidth, resumeHello.PixelHeight = 1000, 600
		resumed, same, ok, err := d.resumeParked(resumeHello, newQuietConnection(t), resumeHello.Size)
		require.NoError(t, err)
		require.True(t, ok)
		require.Same(t, sess, resumed)
		require.Same(t, ac, same)
		require.Equal(t, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 22}, PixelWidth: 1000, PixelHeight: 550}, ptyGeometry.snapshot())
	})
}
