package daemon

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/keys"
)

// --- test doubles -----------------------------------------------------------

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by
// every test that is not specifically exercising the debounce.
type stubClock struct{}

func (stubClock) Now() time.Time                     { return time.Time{} }
func (stubClock) NewTimer(time.Duration) ports.Timer { return stubTimer{} }

type stubTimer struct{}

func (stubTimer) C() <-chan time.Time      { return nil }
func (stubTimer) Reset(time.Duration) bool { return false }
func (stubTimer) Stop() bool               { return true }

type signalClock struct {
	called chan struct{}
	timers chan *signalTimer
	once   sync.Once
}

func (c *signalClock) Now() time.Time { return time.Time{} }
func (c *signalClock) NewTimer(d time.Duration) ports.Timer {
	if c.called != nil {
		c.once.Do(func() { close(c.called) })
	}
	if c.timers == nil {
		return stubTimer{}
	}
	t := &signalTimer{ch: make(chan time.Time, 1), duration: d}
	c.timers <- t
	return t
}

type signalTimer struct {
	ch       chan time.Time
	duration time.Duration
}

func (t *signalTimer) C() <-chan time.Time      { return t.ch }
func (t *signalTimer) Reset(time.Duration) bool { return false }
func (t *signalTimer) Stop() bool               { return true }

func newTestDaemon(t testing.TB, ptys ports.PTYFactory, clk ports.Clock) *Daemon {
	t.Helper()
	return newTestDaemonWithCleanup(t, ptys, clk, true)
}

type terminalEnv struct {
	TrueColor bool
}

func createSessionForTest(d *Daemon, name string, ephemeral bool, cwd string, sz domain.Size, _ terminalEnv, env []string, restoredTabNames ...[]string) (*session, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.createSessionLocked(name, ephemeral, cwd, domain.Geometry{Size: sz}, env, restoredTabNames...)
}

func newTestDaemonWithCleanup(t testing.TB, ptys ports.PTYFactory, clk ports.Clock, registerCleanup bool) *Daemon {
	t.Helper()
	d := New(ptys, clk, slog.New(slog.NewTextHandler(io.Discard, nil)))
	d.serveCtx, d.serveCancel = context.WithCancel(context.Background())
	d.hardCtx, d.hardCancel = context.WithCancel(context.Background())
	if registerCleanup {
		t.Cleanup(d.serveCancel)
		t.Cleanup(d.hardCancel)
	}
	return d
}

// newBlockingPTY returns a MockPTY whose Read blocks until release is called,
// then reports io.EOF (the "child exited" signal).
func newBlockingPTY(t *testing.T) (*portsmocks.MockPTY, func()) {
	t.Helper()
	return newBlockingPTYWithWrites(t, nil)
}

func newBlockingPTYWithWrites(t *testing.T, writes chan<- []byte) (*portsmocks.MockPTY, func()) {
	t.Helper()
	p := portsmocks.NewMockPTY(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		<-release
		return 0, io.EOF
	}).Maybe()
	p.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		if writes != nil {
			cp := append([]byte(nil), b...)
			writes <- cp
		}
		return len(b), nil
	}).Maybe()
	p.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	// Close unblocks a parked Read, exactly as the real PTY does (its Close
	// closes the master fd, failing the in-flight read).
	p.EXPECT().Close().RunAndReturn(func() error { unblock(); return nil }).Maybe()
	p.EXPECT().Pid().Return(4242).Maybe()
	return p, unblock
}

// newBlockingPTYWithFailingWrite returns a MockPTY whose Write always fails
// with writeErr, simulating a wedged pty (or a dead child) that cannot
// accept forwarded keystrokes.
func newBlockingPTYWithFailingWrite(t *testing.T, writeErr error) (*portsmocks.MockPTY, func()) {
	t.Helper()
	p := portsmocks.NewMockPTY(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		<-release
		return 0, io.EOF
	}).Maybe()
	p.EXPECT().Write(mock.Anything).Return(0, writeErr).Maybe()
	p.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	p.EXPECT().Close().RunAndReturn(func() error { unblock(); return nil }).Maybe()
	p.EXPECT().Pid().Return(4242).Maybe()
	return p, unblock
}

// quietPTY is an independently closable PTY for background floating prewarms.
// It intentionally has no testify expectations: tests using the factories below
// are asserting their regular panes, not incidental asynchronous prewarms.
type quietPTY struct{ done chan struct{} }

func newQuietPTY() *quietPTY                  { return &quietPTY{done: make(chan struct{})} }
func (p *quietPTY) Read([]byte) (int, error)  { <-p.done; return 0, io.EOF }
func (*quietPTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *quietPTY) Close() error {
	select {
	case <-p.done:
	default:
		close(p.done)
	}
	return nil
}
func (*quietPTY) Resize(domain.Geometry) error { return nil }
func (*quietPTY) Pid() int                     { return 0 }
func (*quietPTY) ForegroundPgid() (int, error) { return 0, nil }

// blockingOpenFactory keeps activation tests deterministically in Warming
// without starting pane reader/scheduler goroutines.
type blockingOpenFactory struct {
	release chan struct{}
	once    sync.Once
}

func newBlockingOpenFactory(t *testing.T, d *Daemon) *blockingOpenFactory {
	t.Helper()
	f := &blockingOpenFactory{release: make(chan struct{})}
	t.Cleanup(func() {
		f.once.Do(func() { close(f.release) })
		d.sessWg.Wait()
	})
	return f
}

func (f *blockingOpenFactory) Open(_ context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
	<-f.release
	return nil, io.ErrClosedPipe
}

// newFactory keeps the supplied fixture for normal panes. Floating prewarms
// have a distinct (smaller) geometry and receive independent quiet PTYs, so
// they cannot consume test output or add post-test mock calls.
func newFactory(t *testing.T, p ports.PTY) *portsmocks.MockPTYFactory {
	t.Helper()
	f := portsmocks.NewMockPTYFactory(t)
	var normal domain.Size
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ []string, _ []string, _ string, geometry domain.Geometry) (ports.PTY, error) {
			if !normal.Valid() {
				normal = geometry.Size
			}
			if geometry.Size == normal {
				return p, nil
			}
			return newQuietPTY(), nil
		},
	).Maybe()
	return f
}

func newFactorySeq(t *testing.T, ptys ...ports.PTY) *portsmocks.MockPTYFactory {
	t.Helper()
	f := portsmocks.NewMockPTYFactory(t)
	var mu sync.Mutex
	var normal domain.Size
	next := 0
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ []string, _ []string, _ string, geometry domain.Geometry) (ports.PTY, error) {
			mu.Lock()
			defer mu.Unlock()
			if !normal.Valid() {
				normal = geometry.Size
			}
			if geometry.Size != normal {
				return newQuietPTY(), nil
			}
			if next >= len(ptys) {
				return newQuietPTY(), nil
			}
			p := ptys[next]
			next++
			return p, nil
		},
	).Maybe()
	return f
}

// newConn scripts a MockTransport: Recv yields first then more (in order),
// then blocks until the connection is released or Closed (returning io.EOF).
// Every Send is captured on the returned channel.
func newConn(t *testing.T, first ports.Frame, more ...ports.Frame) (*portsmocks.MockTransport, chan ports.Frame, func()) {
	t.Helper()
	tr := portsmocks.NewMockTransport(t)
	sends := make(chan ports.Frame, 64)
	recvCh := make(chan ports.Frame, 1+len(more))
	recvCh <- first
	for _, f := range more {
		recvCh <- f
	}
	done := make(chan struct{})
	var once sync.Once
	closeDone := func() { once.Do(func() { close(done) }) }

	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case f := <-recvCh:
			return f, nil
		case <-done:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		sends <- f
		return nil
	}).Maybe()
	tr.EXPECT().Close().RunAndReturn(func() error { closeDone(); return nil }).Maybe()
	return tr, sends, closeDone
}

func mustHello(intent uint8, name string, sz domain.Size) ports.Frame {
	return ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(ports.Hello{
		Version: ports.ProtocolVersion, Intent: intent, Name: name, Size: sz, TermEnv: "xterm-256color",
	})}
}

func frameInput(data []byte) ports.Frame {
	return ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: data})}
}

func screenLineText(s *vt.Screen, y int) string {
	out := make([]rune, s.Frame.Width)
	for x := range s.Frame.Width {
		out[x] = s.Frame.At(x, y).Rune
	}
	return string(out)
}

// awaitFrame waits for the next frame of type typ on ch, failing on timeout.
func awaitFrame(t *testing.T, ch chan ports.Frame, typ ports.MsgType) ports.Frame {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case f := <-ch:
			if f.Type == typ {
				return f
			}
		case <-deadline:
			t.Fatalf("timed out waiting for frame type %d", typ)
		}
	}
}

func firstSession(d *Daemon) *session {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.sessions {
		if s != nil {
			return s
		}
	}
	return nil
}

func sessionCount(d *Daemon) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.sessions)
}

func listSessions(t *testing.T, d *Daemon) ports.Sessions {
	t.Helper()
	tr, sends, _ := newConn(t, ports.Frame{Type: ports.MsgList, Payload: ports.MarshalList(ports.List{})})
	d.handleList(tr)
	f := awaitFrame(t, sends, ports.MsgSessions)
	sessions, err := ports.UnmarshalSessions(f.Payload)
	require.NoError(t, err)
	return sessions
}

// welcomeBlockingTransport holds the route-level handshake precisely between
// attachment publication and Welcome acceptance. It uses the generated
// transport mock while channels make both sides of the wire deterministic.
type welcomeBlockingTransport struct {
	tr             *portsmocks.MockTransport
	sends          chan ports.Frame
	welcomeEntered chan struct{}
	releaseWelcome chan struct{}
	recvDone       chan struct{}
	releaseOnce    sync.Once
	closeOnce      sync.Once
}

func newWelcomeBlockingTransport(t *testing.T) *welcomeBlockingTransport {
	t.Helper()
	b := &welcomeBlockingTransport{
		sends:          make(chan ports.Frame, 8),
		welcomeEntered: make(chan struct{}),
		releaseWelcome: make(chan struct{}),
		recvDone:       make(chan struct{}),
	}
	b.tr = portsmocks.NewMockTransport(t)
	b.tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		b.sends <- f
		if f.Type == ports.MsgWelcome {
			close(b.welcomeEntered)
			<-b.releaseWelcome
		}
		return nil
	}).Maybe()
	b.tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		<-b.recvDone
		return ports.Frame{}, io.EOF
	}).Maybe()
	b.tr.EXPECT().Close().Return(nil).Maybe()
	t.Cleanup(b.finish)
	return b
}

func (b *welcomeBlockingTransport) release() {
	b.releaseOnce.Do(func() { close(b.releaseWelcome) })
}

func (b *welcomeBlockingTransport) finish() {
	b.release()
	b.closeOnce.Do(func() { close(b.recvDone) })
}

func newCapturingTransport(t testing.TB) (*portsmocks.MockTransport, chan ports.Frame) {
	t.Helper()
	tr := portsmocks.NewMockTransport(t)
	sends := make(chan ports.Frame, 64)
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		sends <- f
		return nil
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	return tr, sends
}

func newManualSessionWithPTYs(t testing.TB, ptys ...ports.PTY) (*Daemon, *session, *attachedClient, chan ports.Frame) {
	t.Helper()
	return newManualSessionWithPTYsClock(t, stubClock{}, ptys...)
}

func newManualSessionWithPTYsClock(t testing.TB, clock ports.Clock, ptys ...ports.PTY) (*Daemon, *session, *attachedClient, chan ports.Frame) {
	t.Helper()
	return newManualSessionWithPTYsClockCleanup(t, clock, true, ptys...)
}

func newManualSessionWithPTYsCleanup(t testing.TB, registerCleanup bool, ptys ...ports.PTY) (*Daemon, *session, *attachedClient, chan ports.Frame) {
	t.Helper()
	return newManualSessionWithPTYsClockCleanup(t, stubClock{}, registerCleanup, ptys...)
}

func newManualSessionWithPTYsClockCleanup(t testing.TB, clock ports.Clock, registerCleanup bool, ptys ...ports.PTY) (*Daemon, *session, *attachedClient, chan ports.Frame) {
	t.Helper()
	d := newTestDaemonWithCleanup(t, nil, clock, registerCleanup)
	tr, sends := newCapturingTransport(t)
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	sctx, cancel := context.WithCancel(d.serveCtx)
	tabs := make([]*tab, 0, len(ptys))
	for _, p := range ptys {
		wctx, wcancel := context.WithCancel(sctx)
		tb := newTab(p, domain.Size{Cols: 80, Rows: 23})
		tb.ctx, tb.cancel = wctx, wcancel
		for _, pane := range tb.panes {
			pane.ctx, pane.cancel = wctx, wcancel
		}
		tabs = append(tabs, tb)
	}
	sess := &session{sessionCore: sessionCore{id: "manual", name: "work", attachments: map[*attachedClient]struct{}{ac: {}}}, ctx: sctx, cancel: cancel, tabs: tabs}
	for _, tb := range tabs {
		publishTiledPaneOwners(sess, tb)
	}
	ac.setSession(sess)
	ac.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac}, nil)
	d.sessions[sess.id] = sess
	if registerCleanup {
		t.Cleanup(cancel)
	}
	return d, sess, ac, sends
}

func newManualTabSession(t *testing.T, n int) (*Daemon, *session, *attachedClient, chan ports.Frame, []func()) {
	t.Helper()
	ptys := make([]ports.PTY, 0, n)
	releases := make([]func(), 0, n)
	for range n {
		p, release := newBlockingPTY(t)
		ptys = append(ptys, p)
		releases = append(releases, release)
	}
	d, sess, ac, sends := newManualSessionWithPTYs(t, ptys...)
	return d, sess, ac, sends, releases
}

func testAttachment(sess *session) *attachedClient {
	if sess == nil {
		return nil
	}
	attachments := sess.snapshotAttachments()
	if len(attachments) == 0 {
		return nil
	}
	return attachments[0]
}

func testAttachmentTabLocked(sess *session) *tab {
	if sess == nil {
		return nil
	}
	var ac *attachedClient
	for candidate := range sess.attachments {
		ac = candidate
		break
	}
	if ac == nil {
		if len(sess.tabs) == 0 {
			return nil
		}
		return sess.tabs[0]
	}
	view := ac.viewSnapshot()
	for _, tb := range sess.tabs {
		if domain.TabStableID(tb.stableID) == view.tabID {
			return tb
		}
	}
	return nil
}

func testAttachmentTab(sess *session) *tab {
	if ac := testAttachment(sess); ac != nil {
		return sess.tabForAttachment(ac)
	}
	return sess.firstTab()
}

func selectTestAttachmentTab(sess *session, idx int) bool {
	ac := testAttachment(sess)
	if ac == nil {
		return idx == 0 && testAttachmentTab(sess) != nil
	}
	return sess.switchAttachmentTab(ac, idx)
}

// selectTestAttachmentTabLocked is used only by fixtures that already hold
// sess.mu; production navigation always goes through selectAttachmentTab.
func selectTestAttachmentTabLocked(sess *session, idx int) bool {
	if sess == nil {
		return false
	}
	var ac *attachedClient
	for candidate := range sess.attachments {
		ac = candidate
		break
	}
	if ac == nil {
		return idx == 0 && len(sess.tabs) > 0
	}
	return sess.activateAttachmentViewLocked(ac, idx)
}

func testAttachmentTabIndexLocked(sess *session) int {
	if sess == nil {
		return 0
	}
	var ac *attachedClient
	for candidate := range sess.attachments {
		ac = candidate
		break
	}
	if ac == nil {
		return 0
	}
	view := ac.viewSnapshot()
	for i, tb := range sess.tabs {
		if domain.TabStableID(tb.stableID) == view.tabID {
			return i
		}
	}
	return 0
}

func testAttachmentTabIndex(sess *session) int {
	if sess == nil {
		return 0
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return testAttachmentTabIndexLocked(sess)
}

func testRow(text string) []renderer.Cell {
	cells := make([]renderer.Cell, 0, len(text))
	for _, r := range text {
		cells = append(cells, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	}
	return cells
}

// appendHistoryRow records a row whose logical extent covers it entirely and
// that ends its logical line — the shape of a line scrolled off a grid it never
// wrapped out of. Tests that care about wrapped extents build the bound instead.
func appendHistoryRow(tb testing.TB, history *vt.History, row []renderer.Cell) {
	tb.Helper()
	id := max(vt.RowID(1_000_000), history.NextRowID())
	require.NoError(tb, history.AppendWithID(row, vt.LineBound{End: len(row)}, id))
}

func TestAppendHistoryRowAllocatesUniqueIDsAfterEviction(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 2, ChunkRows: 1})
	ids := make(map[vt.RowID]struct{})
	for range 4 {
		appendHistoryRow(t, history, testRow("row"))
		view := history.View()
		id := view.RowID(view.Len() - 1)
		require.NotContains(t, ids, id)
		ids[id] = struct{}{}
	}
	require.Len(t, ids, 4)
}

func tabCount(sess *session) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.tabs)
}

// --- handshake --------------------------------------------------------------

func TestRouteRejectsInvalidTerminalSizeBeforeResumeMutation(t *testing.T) {
	tests := []struct {
		name string
		size domain.Size
	}{
		{name: "zero columns", size: domain.Size{Cols: 0, Rows: 24}},
		{name: "zero rows", size: domain.Size{Cols: 80, Rows: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty, release := newBlockingPTY(t)
			defer release()
			d := newTestDaemon(t, newFactory(t, pty), stubClock{})

			transport := &closeTrackingTransport{}
			sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), transport)
			require.NoError(t, err)
			token := ac.resumeToken
			d.clientGone(sess, ac, transport, false)

			d.mu.Lock()
			parked := d.parked[token]
			d.mu.Unlock()
			require.NotNil(t, parked)

			invalid := helloResumeCapable(ports.IntentResume, "work", token)
			invalid.Size = tt.size
			_, _, err = d.route(invalid, &closeTrackingTransport{})
			var protocolErr *protoErr
			require.ErrorAs(t, err, &protocolErr)
			require.Equal(t, ports.ErrInternal, protocolErr.code)

			d.mu.Lock()
			require.Same(t, parked, d.parked[token], "invalid resume must not consume the parked attachment")
			d.mu.Unlock()
			require.Same(t, transport, ac.transport(), "invalid resume must not bind a replacement transport")
		})
	}
}

func waitGroupDone(wg *sync.WaitGroup) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

func rowText(row []renderer.Cell) string {
	runes := make([]rune, len(row))
	for i, c := range row {
		if c.Continuation {
			runes[i] = ' '
			continue
		}
		runes[i] = c.Rune
	}
	return string(runes)
}

type notifyClock struct{}

func (notifyClock) Now() time.Time { return time.Now() }
func (notifyClock) NewTimer(d time.Duration) ports.Timer {
	if d == debounceInterval {
		return stubTimer{}
	}
	return realTimer{t: time.NewTimer(5 * time.Millisecond)}
}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
func (r realTimer) Stop() bool                 { return r.t.Stop() }

func TestHandleHelloDefersFreshOutputUntilWelcome(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	clock := newCoordinatorMockClock(t, 4)
	d := newTestDaemon(t, newFactory(t, pty), clock.clock)
	tr := newWelcomeBlockingTransport(t)
	done := make(chan struct{})
	go func() {
		d.handleHello(tr.tr, mustHello(ports.IntentNew, "welcome-gate", domain.Size{Cols: 80, Rows: 24}))
		close(done)
	}()

	awaitTestCompletion(t, tr.welcomeEntered, "timed out waiting for Welcome send")
	handshakeTimer := awaitHandshakeTimer(t, clock)
	welcome := awaitFrame(t, tr.sends, ports.MsgWelcome)
	require.Equal(t, ports.MsgWelcome, welcome.Type)
	sess := firstSession(d)
	sess.mu.Lock()
	ac := sess.snapshotAttachmentsLocked()[0]
	sess.mu.Unlock()
	require.NotNil(t, ac, "route must publish the attachment before Welcome returns")

	// Exercise the real producer fan-in after publication. The deadline worker
	// has completed before asserting the gate, so this is not a timing race.
	d.invalidateRender(sess, ac, true, "welcome-gate-test")
	// Attach-time theme setup may have armed an older bulk timer. The fresh
	// urgent producer replaces it, so advance the newest published deadline.
	timer := awaitLatestCoordinatorTimer(t, clock)
	rc := sess.renderCoordinator()
	rc.mu.Lock()
	workerDone := rc.normalLane.token.done
	rc.mu.Unlock()
	require.NotNil(t, workerDone)
	timer.ch <- time.Time{}
	awaitTestCompletion(t, workerDone, "coordinator deadline worker did not complete")
	requireNoCoordinatorOutputFrame(t, tr.sends)

	tr.release()
	output := awaitFrame(t, tr.sends, ports.MsgOutput)
	first, err := ports.UnmarshalOutput(output.Payload)
	require.NoError(t, err)
	require.Zero(t, first.Base, "the first post-Welcome frame is full")
	tr.finish()
	awaitTestCompletion(t, done, "timed out waiting for Welcome handler completion")
	awaitTestCompletion(t, handshakeTimer.stopped, "handshake timer was not canceled after publication")
	requireNoCoordinatorOutputFrame(t, tr.sends)
}

// TestServeReturnsDespiteWedgedClientOnShutdown: killSession's teardown must
// not be gated behind the Detached notice. The client transport wedges every
// MsgDetached send until the transport is closed (mirroring a full kernel
// send buffer: only Close fails the in-flight write). Serve must still tear

func mustOutputData(t *testing.T, sends chan ports.Frame) []byte {
	t.Helper()
	f := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(f.Payload)
	require.NoError(t, err)
	return out.Data
}
