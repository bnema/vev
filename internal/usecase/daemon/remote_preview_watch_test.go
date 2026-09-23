package daemon

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

// --- fake clock: distinguishes the watch's own interval/revalidate timers
// from the boundedControlSend timeout timer, so a test can drive the watch
// loop deterministically without ever needing the send-bound timer to fire.

type previewTimer struct {
	mock     *portsmocks.MockTimer
	ch       chan time.Time
	duration time.Duration
}

func (pt *previewTimer) fire() { pt.ch <- time.Time{} }

type previewClockHarness struct {
	clock   *portsmocks.MockClock
	pending chan *previewTimer
}

func newPreviewClockHarness(t *testing.T) *previewClockHarness {
	t.Helper()
	h := &previewClockHarness{clock: portsmocks.NewMockClock(t), pending: make(chan *previewTimer, 64)}
	h.clock.EXPECT().Now().Return(time.Time{}).Maybe()
	h.clock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(d time.Duration) ports.Timer {
		pt := &previewTimer{mock: portsmocks.NewMockTimer(t), ch: make(chan time.Time, 1), duration: d}
		pt.mock.EXPECT().C().Maybe().Return((<-chan time.Time)(pt.ch))
		pt.mock.EXPECT().Stop().Maybe().Return(true)
		// boundedControlSend races this timer against an already-completing
		// SendServer call on every frame send; the test never needs to fire it.
		if d != detachNotifyTimeout {
			h.pending <- pt
		}
		return pt.mock
	}).Maybe()
	return h
}

func (h *previewClockHarness) awaitTimer(t *testing.T, want time.Duration) *previewTimer {
	t.Helper()
	select {
	case pt := <-h.pending:
		require.Equal(t, want, pt.duration)
		return pt
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for a %s watch timer", want)
		return nil
	}
}

// --- fake connection: ReceiveClient must block until the test simulates
// either a client message/EOF or a server-side Close. A generated
// portsmocks.MockServerConnection still backs SendServer/Close so frame
// assertions decode typed protocol.ServerMessage values directly, but the
// blocking-until-signaled ReceiveClient is hand-rolled: expressing it through
// mock.Call plumbing would need the same channel underneath anyway.

type clientRecvResult struct {
	msg protocol.ClientMessage
	err error
}

type previewConnHarness struct {
	tr        *portsmocks.MockServerConnection
	frames    chan protocol.RemotePreview
	recv      chan clientRecvResult
	closed    chan struct{}
	closeOnce sync.Once
}

func newPreviewConnHarness(t *testing.T) *previewConnHarness {
	t.Helper()
	h := &previewConnHarness{
		tr:     portsmocks.NewMockServerConnection(t),
		frames: make(chan protocol.RemotePreview, 16),
		recv:   make(chan clientRecvResult, 1),
		closed: make(chan struct{}),
	}
	h.tr.EXPECT().ReceiveClient().RunAndReturn(func() (protocol.ClientMessage, error) {
		select {
		case r := <-h.recv:
			return r.msg, r.err
		case <-h.closed:
			return nil, io.EOF
		}
	}).Maybe()
	h.tr.EXPECT().SendServer(mock.Anything).RunAndReturn(func(message protocol.ServerMessage) error {
		preview, ok := message.(protocol.RemotePreview)
		if !ok {
			return fmt.Errorf("unexpected server message %T", message)
		}
		h.frames <- preview
		return nil
	}).Maybe()
	h.tr.EXPECT().Close().RunAndReturn(func() error {
		h.closeOnce.Do(func() { close(h.closed) })
		return nil
	}).Maybe()
	return h
}

// closeClient simulates the client itself sending a message or hanging up;
// this is distinct from the server-side Close the daemon issues on teardown.
func (h *previewConnHarness) closeClient(err error) {
	select {
	case h.recv <- clientRecvResult{err: err}:
	default:
	}
}

func (h *previewConnHarness) nextFrame(t *testing.T) protocol.RemotePreview {
	t.Helper()
	select {
	case f := <-h.frames:
		return f
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a preview frame")
		return protocol.RemotePreview{}
	}
}

func (h *previewConnHarness) requireNoFrame(t *testing.T) {
	t.Helper()
	select {
	case f := <-h.frames:
		t.Fatalf("unexpected preview frame sent: %+v", f)
	case <-time.After(75 * time.Millisecond):
	}
}

func (h *previewConnHarness) awaitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-h.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("transport was not closed")
	}
}

// --- daemon/session fixture -------------------------------------------------

// newPreviewSession builds a headless named session with one quiet-PTY tab per
// id, registers it in d.sessions the same way createSessionLocked would, and
// wires each tab/pane context like the package's other manual fixtures.
func newPreviewSession(t *testing.T, d *Daemon, tabIDs ...string) *session {
	t.Helper()
	lifecycle := newTestLifecycle(t)
	sctx, scancel := context.WithCancel(d.serveCtx)
	tabs := make([]*tab, 0, len(tabIDs))
	for i, id := range tabIDs {
		wctx, wcancel := context.WithCancel(sctx)
		tb := newTabWithStableID(id, fmt.Sprintf("pane-%d", i), newQuietPTY(), domain.Size{Cols: 80, Rows: 24})
		tb.ctx, tb.cancel = wctx, wcancel
		for _, p := range tb.panes {
			p.ctx, p.cancel = wctx, wcancel
		}
		tabs = append(tabs, tb)
	}
	sess := &session{
		sessionCore: sessionCore{id: "preview", name: "work", incarnation: lifecycle, attachments: map[*attachedClient]struct{}{}},
		ctx:         sctx,
		cancel:      scancel,
		tabs:        tabs,
	}
	for _, tb := range tabs {
		publishTiledPaneOwners(sess, tb)
	}
	d.mu.Lock()
	d.sessions[sess.id] = sess
	d.mu.Unlock()
	t.Cleanup(scancel)
	return sess
}

// writePreviewContent writes directly to the pane's VT screen under pane.mu,
// matching the lock captureRemotePreview takes around its own Snapshot call
// so a concurrently running watch never races the write under -race.
func writePreviewContent(t *testing.T, p *pane, seq string) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.screen.Write([]byte(seq))
}

// previewFrameText flattens a frame's cells to a plain string for substring
// assertions; continuation cells (the right half of a wide rune) are skipped.
func previewFrameText(preview protocol.RemotePreview) string {
	var b strings.Builder
	for _, cell := range preview.Cells {
		if cell.Continuation {
			continue
		}
		if cell.Rune == 0 {
			b.WriteByte(' ')
		} else {
			b.WriteRune(cell.Rune)
		}
	}
	return b.String()
}

func buildPreviewWatch(sess *session, tabID string, interval time.Duration) protocol.RemotePreviewWatch {
	return protocol.RemotePreviewWatch{
		Request: protocol.RemotePreviewRequest{
			Version: protocol.RemotePreviewSchemaVersion,
			Target: domain.RemoteSessionTarget{
				Endpoint: "local", DisplayOrigin: "local",
				LifecycleID: sess.incarnation, SessionName: sess.name, LiveTabID: domain.TabStableID(tabID),
			},
			Width: 20, Height: 5,
		},
		MinInterval: interval,
	}
}

// --- watch harness -----------------------------------------------------------

type previewWatchHarness struct {
	t    *testing.T
	d    *Daemon
	sess *session
	tabs []*tab
	clk  *previewClockHarness
	conn *previewConnHarness
	done chan struct{}
}

func newPreviewWatchHarness(t *testing.T, tabIDs ...string) *previewWatchHarness {
	t.Helper()
	clk := newPreviewClockHarness(t)
	d := newTestDaemonWithCleanup(t, nil, clk.clock, true)
	sess := newPreviewSession(t, d, tabIDs...)
	conn := newPreviewConnHarness(t)
	return &previewWatchHarness{t: t, d: d, sess: sess, tabs: sess.tabs, clk: clk, conn: conn}
}

func (h *previewWatchHarness) start(watch protocol.RemotePreviewWatch) {
	h.t.Helper()
	h.done = make(chan struct{})
	go func() {
		h.d.serveRemotePreviewWatch(h.conn.tr, watch)
		close(h.done)
	}()
}

func (h *previewWatchHarness) awaitDone(timeout time.Duration) {
	h.t.Helper()
	select {
	case <-h.done:
	case <-time.After(timeout):
		h.t.Fatal("serveRemotePreviewWatch did not return")
	}
}

// stop ends a still-running watch through the daemon hard context, exactly
// as daemon shutdown would, and joins the goroutine.
func (h *previewWatchHarness) stop() {
	h.d.hardCancel()
	h.awaitDone(2 * time.Second)
}

// --- tests -------------------------------------------------------------------

func TestServeRemotePreviewWatchSendsFirstFrameImmediately(t *testing.T) {
	h := newPreviewWatchHarness(t, "tab-0")
	writePreviewContent(t, h.tabs[0].focusedPane(), "\x1b[H\x1b[2Jv1")
	h.start(buildPreviewWatch(h.sess, "tab-0", 32*time.Millisecond))

	frame := h.conn.nextFrame(t)
	require.Equal(t, protocol.RemotePreviewOK, frame.Status)
	require.Contains(t, previewFrameText(frame), "v1")

	h.stop()
}

func TestServeRemotePreviewWatchCoalescesBurstToOneFramePerInterval(t *testing.T) {
	h := newPreviewWatchHarness(t, "tab-0")
	pane := h.tabs[0].focusedPane()
	writePreviewContent(t, pane, "\x1b[H\x1b[2Jv1")
	interval := 32 * time.Millisecond
	h.start(buildPreviewWatch(h.sess, "tab-0", interval))
	first := h.conn.nextFrame(t)
	require.Contains(t, previewFrameText(first), "v1")

	// A burst of writes inside one MinInterval window: only the state at the
	// next poll boundary should ever reach the client.
	writePreviewContent(t, pane, "\x1b[H\x1b[2Jv2")
	writePreviewContent(t, pane, "\x1b[H\x1b[2Jv3")
	writePreviewContent(t, pane, "\x1b[H\x1b[2Jv4")

	h.clk.awaitTimer(t, interval).fire()
	h.clk.awaitTimer(t, remotePreviewRevalidate).fire()

	second := h.conn.nextFrame(t)
	require.Contains(t, previewFrameText(second), "v4")
	h.conn.requireNoFrame(t)

	h.stop()
}

func TestServeRemotePreviewWatchSkipsResendWhenUnchanged(t *testing.T) {
	h := newPreviewWatchHarness(t, "tab-0")
	pane := h.tabs[0].focusedPane()
	writePreviewContent(t, pane, "\x1b[H\x1b[2Jsame")
	interval := 32 * time.Millisecond
	h.start(buildPreviewWatch(h.sess, "tab-0", interval))
	h.conn.nextFrame(t)

	h.clk.awaitTimer(t, interval).fire()
	h.clk.awaitTimer(t, remotePreviewRevalidate).fire()
	// The next interval timer is armed only after the loop re-captured and
	// compared this pass, so seeing it proves the unchanged content was
	// already skipped rather than merely not-yet-checked.
	h.clk.awaitTimer(t, interval)
	h.conn.requireNoFrame(t)

	h.stop()
}

func TestServeRemotePreviewWatchIgnoresBackgroundTabOutput(t *testing.T) {
	h := newPreviewWatchHarness(t, "tab-0", "tab-1")
	watched := h.tabs[0].focusedPane()
	background := h.tabs[1].focusedPane()
	writePreviewContent(t, watched, "\x1b[H\x1b[2Jwatched")
	interval := 32 * time.Millisecond
	h.start(buildPreviewWatch(h.sess, "tab-0", interval))
	first := h.conn.nextFrame(t)
	require.Contains(t, previewFrameText(first), "watched")

	writePreviewContent(t, background, "\x1b[H\x1b[2Jbg-output")
	h.clk.awaitTimer(t, interval).fire()
	h.clk.awaitTimer(t, remotePreviewRevalidate).fire()
	h.clk.awaitTimer(t, interval) // proves the pass already re-captured tab-0
	h.conn.requireNoFrame(t)

	h.stop()
}

func TestServeRemotePreviewWatchCapturesSelectedTab(t *testing.T) {
	h := newPreviewWatchHarness(t, "tab-0", "tab-1")
	writePreviewContent(t, h.tabs[0].focusedPane(), "\x1b[H\x1b[2Jactive")
	writePreviewContent(t, h.tabs[1].focusedPane(), "\x1b[H\x1b[2Jselected")
	h.start(buildPreviewWatch(h.sess, "tab-1", 32*time.Millisecond))

	frame := h.conn.nextFrame(t)
	require.Equal(t, protocol.RemotePreviewOK, frame.Status)
	require.Equal(t, domain.TabStableID("tab-1"), frame.TabID)
	require.Contains(t, previewFrameText(frame), "selected")
	require.NotContains(t, previewFrameText(frame), "active")

	h.stop()
}

func TestServeRemotePreviewWatchSendsNoSuchTargetOnceWhenSessionKilled(t *testing.T) {
	h := newPreviewWatchHarness(t, "tab-0")
	writePreviewContent(t, h.tabs[0].focusedPane(), "\x1b[H\x1b[2Jv1")
	interval := 32 * time.Millisecond
	h.start(buildPreviewWatch(h.sess, "tab-0", interval))
	h.conn.nextFrame(t)

	rc := attachmentRenderCoordinator(h.sess)
	require.NotNil(t, rc)
	require.True(t, rc.hasPreviewSubscribers())

	// Simulate a kill through the same exact-identity lookup
	// remotePreviewSession uses, without driving the full kill workflow.
	h.d.mu.Lock()
	delete(h.d.sessions, h.sess.id)
	h.d.mu.Unlock()

	h.clk.awaitTimer(t, interval).fire()
	h.clk.awaitTimer(t, remotePreviewRevalidate).fire()

	frame := h.conn.nextFrame(t)
	require.Equal(t, protocol.RemotePreviewNoSuchTarget, frame.Status)
	h.conn.requireNoFrame(t)
	h.awaitDone(2 * time.Second)
	require.False(t, rc.hasPreviewSubscribers())
}

func TestServeRemotePreviewWatchEndsOnClientClose(t *testing.T) {
	h := newPreviewWatchHarness(t, "tab-0")
	writePreviewContent(t, h.tabs[0].focusedPane(), "\x1b[H\x1b[2Jv1")
	h.start(buildPreviewWatch(h.sess, "tab-0", 32*time.Millisecond))
	h.conn.nextFrame(t)

	rc := attachmentRenderCoordinator(h.sess)
	require.NotNil(t, rc)
	require.True(t, rc.hasPreviewSubscribers())

	h.conn.closeClient(io.EOF)
	h.awaitDone(2 * time.Second)
	h.conn.requireNoFrame(t)
	require.False(t, rc.hasPreviewSubscribers())
}

func TestServeRemotePreviewWatchRejectsInvalidWatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.RemotePreviewWatch)
	}{
		{name: "interval below bound", mutate: func(w *protocol.RemotePreviewWatch) {
			w.MinInterval = protocol.RemotePreviewWatchMinInterval - time.Millisecond
		}},
		{name: "invalid request width", mutate: func(w *protocol.RemotePreviewWatch) {
			w.Request.Width = 0
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newPreviewWatchHarness(t, "tab-0")
			watch := buildPreviewWatch(h.sess, "tab-0", 32*time.Millisecond)
			tt.mutate(&watch)

			h.start(watch)
			frame := h.conn.nextFrame(t)
			require.Equal(t, protocol.RemotePreviewMalformed, frame.Status)
			h.conn.awaitClosed(t)
			h.awaitDone(2 * time.Second)
		})
	}
}

func TestServeRemotePreviewWatchEndsOnHardContextCancel(t *testing.T) {
	h := newPreviewWatchHarness(t, "tab-0")
	writePreviewContent(t, h.tabs[0].focusedPane(), "\x1b[H\x1b[2Jv1")
	h.start(buildPreviewWatch(h.sess, "tab-0", 32*time.Millisecond))
	h.conn.nextFrame(t)

	h.d.hardCancel()
	h.awaitDone(2 * time.Second)
	h.conn.requireNoFrame(t)
	h.conn.awaitClosed(t)
}
