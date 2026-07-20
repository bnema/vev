package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type fakeBarRunner struct {
	mu    sync.Mutex
	calls []barScriptContext
	outs  []string
	errs  []error
}

func (r *fakeBarRunner) run(_ context.Context, _ string, _ []string, ctx barScriptContext) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, ctx)
	i := len(r.calls) - 1
	if i < len(r.errs) && r.errs[i] != nil {
		return "", r.errs[i]
	}
	if i < len(r.outs) {
		return r.outs[i], nil
	}
	return "", nil
}

type blockingBarRunner struct {
	*fakeBarRunner
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingBarRunner(outs []string) *blockingBarRunner {
	return &blockingBarRunner{fakeBarRunner: &fakeBarRunner{outs: outs}, entered: make(chan struct{}), release: make(chan struct{})}
}

func (r *blockingBarRunner) run(ctx context.Context, command string, env []string, barCtx barScriptContext) (string, error) {
	r.once.Do(func() { close(r.entered) })
	select {
	case <-r.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return r.fakeBarRunner.run(ctx, command, env, barCtx)
}

func TestBarScriptRefreshIntervalClampingTickAndLastGoodOnFailure(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top1", "bot1", "top2", "bot2"}}
	d := newBarRefreshTestDaemon(r, 100*time.Millisecond)
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 120, Rows: 24}}

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), false))
	waitBarRefreshIdle(t, d)
	require.Equal(t, 2, len(r.calls))
	require.Equal(t, "top1", d.barStateFor(sess, "").topRight)
	require.False(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, int64(500*time.Millisecond)), false), "clamped interval should skip sub-second tick")

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(1, 0), false))
	waitBarRefreshIdle(t, d)
	require.Equal(t, 4, len(r.calls))
	require.Equal(t, "top2", d.barStateFor(sess, "").topRight)

	r.errs = []error{nil, nil, nil, nil, errors.New("boom"), errors.New("boom")}
	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(2, 0), false))
	waitBarRefreshIdle(t, d)
	state := d.barStateFor(sess, "")
	require.Equal(t, "top2", state.topRight)
	require.Equal(t, "bot2", state.bottomRight)
}

func TestBarScriptContextUsesActivePaneSessionAndClientCols(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top", "bottom"}}
	d := newBarRefreshTestDaemon(r, time.Second)
	sess := newBarRefreshTestSession()
	sess.name = "work"
	sess.cwd = "/repo"
	sess.client = &attachedClient{size: domain.Size{Cols: 132, Rows: 40}}
	sess.active = 1
	sess.tabs[1].size = domain.Size{Cols: 132, Rows: 38}
	active := sess.tabs[1].focusedPane()
	active.stableID = "pane-active"
	active.pty = newScriptPTY(nil)
	d.procCwd = func(pid int) (string, error) {
		require.Equal(t, 4242, pid)
		return "/pane-repo", nil
	}

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), true))
	waitBarRefreshIdle(t, d)
	require.Len(t, r.calls, 2)
	for _, call := range r.calls {
		require.Equal(t, "work", call.Session)
		require.Equal(t, "tab-active", call.Tab)
		require.Equal(t, "pane-active", call.Pane)
		require.Equal(t, "/pane-repo", call.PaneCWD)
		require.Equal(t, 132, call.Cols)
	}
	require.Equal(t, "top-right", r.calls[0].Anchor)
	require.Equal(t, "bottom-right", r.calls[1].Anchor)
}

func TestBarScriptRefreshSkipsWithoutAttachedClient(t *testing.T) {
	r := &fakeBarRunner{}
	d := newBarRefreshTestDaemon(r, time.Second)
	sess := newBarRefreshTestSession()

	require.False(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), true))
	require.Empty(t, r.calls)
}

func TestBarScriptRefreshSkipsRunnerWhenCommandsDisabled(t *testing.T) {
	r := &fakeBarRunner{}
	d := newBarRefreshTestDaemon(r, time.Second)
	d.barScripts.cfg.topRight = ""
	d.barScripts.cfg.bottomRight = ""
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}
	d.barScripts.outputs[sess.id] = barScriptOutputs{topRight: "old", bottomRight: "old"}

	require.False(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), true))
	require.Empty(t, r.calls)
	state := d.barStateFor(sess, "")
	require.Empty(t, state.topRight)
	require.Empty(t, state.bottomRight)
}

func TestDefaultBarConfigDoesNotInvokeRunner(t *testing.T) {
	r := &fakeBarRunner{}
	d := newBarRefreshTestDaemon(r, time.Second)
	d.barScripts.cfg = barConfigFromDomain(domain.Defaults().Bar)
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

	require.False(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), true))
	require.Empty(t, r.calls)
}

func TestBarScriptForcedRefreshRespectsMinimumInterval(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top1", "bottom1", "top2", "bottom2"}}
	d := newBarRefreshTestDaemon(r, 5*time.Second)
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(10, 0), true))
	waitBarRefreshIdle(t, d)
	require.False(t, d.refreshBarScriptsIfDue(sess, time.Unix(10, int64(500*time.Millisecond)), true))
	require.Len(t, r.calls, 2)
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.calls) == 4
	}, 1500*time.Millisecond, 10*time.Millisecond)
}

func TestBarScriptContextChangeDebouncesUntilMinimumInterval(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top1", "bottom1", "top2", "bottom2"}}
	d := newBarRefreshTestDaemon(r, 5*time.Second)
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Now(), false))
	waitBarRefreshIdle(t, d)
	sess.cwd = "/other"
	require.False(t, d.refreshBarScriptsIfDue(sess, time.Now().Add(100*time.Millisecond), false))
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.calls) == 4
	}, 1500*time.Millisecond, 10*time.Millisecond)
	require.Equal(t, "/other", r.calls[2].PaneCWD)
}

func TestBarScriptRefreshForcesWhenContextChanges(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top1", "bottom1", "top2", "bottom2"}}
	d := newBarRefreshTestDaemon(r, 5*time.Second)
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), false))
	waitBarRefreshIdle(t, d)
	sess.cwd = "/other"
	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(1, 0), false))
	waitBarRefreshIdle(t, d)
	require.Len(t, r.calls, 4)
	require.Equal(t, "/other", r.calls[2].PaneCWD)
}

func TestApplyConfigPreservesBarOutputsWhenBarConfigUnchanged(t *testing.T) {
	r := &fakeBarRunner{}
	d := newBarRefreshTestDaemon(r, time.Second)
	sess := newBarRefreshTestSession()
	d.barScripts.outputs[sess.id] = barScriptOutputs{topRight: "top-good", bottomRight: "bottom-good"}

	d.ApplyConfig(domain.Config{Bar: domain.BarConfig{TopRight: "top", BottomRight: "bottom", Interval: time.Second}})

	state := d.barStateFor(sess, "")
	require.Equal(t, "top-good", state.topRight)
	require.Equal(t, "bottom-good", state.bottomRight)
}

func TestRefreshBarScriptsAllSessionsForcesRun(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top1", "bottom1"}}
	d := newBarRefreshTestDaemon(r, 60*time.Second)
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}
	d.sessions = map[domain.SessionID]*session{sess.id: sess}

	// Verifies refreshBarScriptsAllSessions iterates every live session and
	// dispatches a run for each. This does not exercise force specifically:
	// lastRefresh is zero on a fresh session, so refreshBarScriptsIfDue skips
	// the interval check regardless of force, and a non-forcing call would
	// pass this test too.
	d.refreshBarScriptsAllSessions()
	waitBarRefreshIdle(t, d)

	require.Len(t, r.calls, 2, "should force a run for every live session")
	require.Equal(t, "top1", d.barStateFor(sess, "").topRight)
}

func TestApplyConfigBumpsVersionOnlyWhenBarConfigChanges(t *testing.T) {
	r := &fakeBarRunner{}
	d := newBarRefreshTestDaemon(r, time.Second)

	before := d.barScripts.version
	d.ApplyConfig(domain.Config{Bar: domain.BarConfig{TopRight: "top", BottomRight: "bottom", Interval: time.Second}})
	require.Equal(t, before, d.barScripts.version, "identical bar config must not invalidate")

	d.ApplyConfig(domain.Config{Bar: domain.BarConfig{TopRight: "changed", BottomRight: "bottom", Interval: time.Second}})
	require.Equal(t, before+1, d.barScripts.version, "changed bar config must invalidate")
}

func TestBarScriptFailureLogsAndRetainsLastGood(t *testing.T) {
	var logs bytes.Buffer
	r := &fakeBarRunner{outs: []string{"top-good", "bottom-good"}}
	d := newBarRefreshTestDaemon(r, time.Second)
	d.log = slog.New(slog.NewTextHandler(&logs, nil))
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(10, 0), true))
	waitBarRefreshIdle(t, d)
	r.errs = []error{nil, nil, errors.New("boom"), errors.New("bang")}
	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(11, 0), true))
	waitBarRefreshIdle(t, d)

	state := d.barStateFor(sess, "")
	require.Equal(t, "top-good", state.topRight)
	require.Equal(t, "bottom-good", state.bottomRight)
	require.True(t, strings.Contains(logs.String(), "bar script failed; keeping last good output"), logs.String())
	require.True(t, strings.Contains(logs.String(), "anchor=top-right"), logs.String())
}

func TestBarScriptRunDoesNotRestoreClearedSessionState(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top", "bottom"}}
	d := newBarRefreshTestDaemon(r, time.Second)
	sess := newBarRefreshTestSession()
	d.barScripts.mu.Lock()
	d.barScripts.running[sess.id] = true
	d.barScripts.mu.Unlock()

	d.clearBarScriptsForSession(sess.id)
	d.runBarScripts(sess, r, d.barScripts.cfg, barScriptContext{}, nil, 0)

	state := d.barStateFor(sess, "")
	require.Empty(t, state.topRight)
	require.Empty(t, state.bottomRight)
}

func TestBarScriptRunConsumesPendingAfterRunningClears(t *testing.T) {
	r := newBlockingBarRunner([]string{"top1", "bottom1", "top2", "bottom2"})
	d := newBarRefreshTestDaemon(r, time.Second)
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(10, 0), true))
	select {
	case <-r.entered:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first bar script run")
	}
	require.False(t, d.refreshBarScriptsIfDue(sess, time.Unix(11, 0), true))
	close(r.release)
	waitBarRefreshIdle(t, d)
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.calls) == 4
	}, time.Second, time.Millisecond)
}

// TestBarScriptFailureLogsOnceUntilSignatureChanges covers the full
// shouldLogBarFailure/clearBarFailure contract for a single anchor
// (top-right; bottom-right always succeeds in this test so it never
// contributes a log line):
//
//  1. an identical repeated failure does not log a second time,
//  2. a different failure for the same session+anchor logs again, and
//  3. a successful run clears the recorded signature, so a subsequent
//     identical failure logs again afterward.
//
// Each refresh consumes two fakeBarRunner call-count entries (top-right then
// bottom-right), so the stages are order-dependent and asserted sequentially
// rather than table-driven.
func TestBarScriptFailureLogsOnceUntilSignatureChanges(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&syncWriter{w: &buf, mu: &mu}, nil))

	notFound := &barScriptError{exitCode: 127, stderr: "sh: vev-bar-top-right: not found", err: errors.New("exit status 127")}
	permissionDenied := &barScriptError{exitCode: 126, stderr: "sh: vev-bar-top-right: permission denied", err: errors.New("exit status 126")}

	r := &fakeBarRunner{errs: []error{
		notFound, nil, // refresh 1: top fails
		notFound, nil, // refresh 2: top fails identically -> must not log again
		permissionDenied, nil, // refresh 3: top fails differently -> must log again
		nil, nil, // refresh 4: top succeeds -> clears recorded signature
		notFound, nil, // refresh 5: top fails with the original signature -> must log (cleared)
	}}
	d := newBarRefreshTestDaemon(r, time.Second)
	d.log = logger
	d.baseEnv = []string{"PATH=/usr/bin"}
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

	readBuf := func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), true))
	waitBarRefreshIdle(t, d)
	out := readBuf()
	require.Equal(t, 1, strings.Count(out, "anchor=top-right"), "first failure should log")
	require.Contains(t, out, "not found")
	require.Contains(t, out, "PATH=/usr/bin")

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(10, 0), true))
	waitBarRefreshIdle(t, d)
	out = readBuf()
	require.Equal(t, 1, strings.Count(out, "anchor=top-right"), "identical repeated failure should not log again")

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(20, 0), true))
	waitBarRefreshIdle(t, d)
	out = readBuf()
	require.Equal(t, 2, strings.Count(out, "anchor=top-right"), "a different failure for the same anchor should log again")
	require.Contains(t, out, "permission denied")

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(30, 0), true))
	waitBarRefreshIdle(t, d)
	out = readBuf()
	require.Equal(t, 2, strings.Count(out, "anchor=top-right"), "a successful run must not log and must clear the recorded signature")

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(40, 0), true))
	waitBarRefreshIdle(t, d)
	out = readBuf()
	require.Equal(t, 3, strings.Count(out, "anchor=top-right"),
		"an identical failure after a successful run should log again since the recorded signature was cleared")
}

type syncWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func TestBarScriptRunIgnoresStaleConfigVersion(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top", "bottom"}}
	d := newBarRefreshTestDaemon(r, time.Second)
	sess := newBarRefreshTestSession()
	d.barScripts.mu.Lock()
	d.barScripts.running[sess.id] = true
	d.barScripts.version = 2
	d.barScripts.mu.Unlock()

	d.runBarScripts(sess, r, d.barScripts.cfg, barScriptContext{}, nil, 1)

	state := d.barStateFor(sess, "")
	require.Empty(t, state.topRight)
	require.Empty(t, state.bottomRight)
}

func TestPokeSessionRenderWakesHeadlessPreviewCoordinator(t *testing.T) {
	d := newBarRefreshTestDaemon(nil, time.Second)
	sess := newBarRefreshTestSession()
	var invalidations int
	rc := newRenderCoordinator(renderCoordinatorOptions{onInvalidate: func(renderInvalidation) { invalidations++ }})
	sess.installRenderCoordinator(rc)

	d.pokeSessionRender(sess)

	require.Equal(t, 1, invalidations)
}

func TestBarScriptRefreshIsPerSession(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top-a", "bottom-a", "top-b", "bottom-b"}}
	d := newBarRefreshTestDaemon(r, time.Second)
	a := newBarRefreshTestSession()
	a.id = "a"
	a.name = "a"
	a.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}
	b := newBarRefreshTestSession()
	b.id = "b"
	b.name = "b"
	b.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

	require.True(t, d.refreshBarScriptsIfDue(a, time.Unix(0, 0), false))
	waitBarRefreshIdle(t, d)
	require.True(t, d.refreshBarScriptsIfDue(b, time.Unix(0, 0), false))
	waitBarRefreshIdle(t, d)

	stateA := d.barStateFor(a, "")
	stateB := d.barStateFor(b, "")
	require.Equal(t, "top-a", stateA.topRight)
	require.Equal(t, "bottom-a", stateA.bottomRight)
	require.Equal(t, "top-b", stateB.topRight)
	require.Equal(t, "bottom-b", stateB.bottomRight)
}

// pollerMockClock and pollerMockTimer wrap the generated portsmocks.MockClock
// and portsmocks.MockTimer (see internal/ports/mocks/clock_mock.go), following
// the coordinatorMockClock pattern in render_coordinator_test.go: the mocks
// supply the ports.Clock/ports.Timer implementations, while a small
// hand-owned struct retains the real channel and requested duration for each
// timer so the test goroutine can fire ticks and assert on re-arming.
type pollerMockClock struct {
	clock  *portsmocks.MockClock
	timers chan *pollerMockTimer
}

type pollerMockTimer struct {
	mock     *portsmocks.MockTimer
	ch       chan time.Time
	duration time.Duration
}

func newPollerMockClock(t *testing.T, capacity int) *pollerMockClock {
	t.Helper()
	clk := &pollerMockClock{
		clock:  portsmocks.NewMockClock(t),
		timers: make(chan *pollerMockTimer, capacity),
	}
	clk.clock.EXPECT().Now().Return(time.Unix(0, 0)).Maybe()
	clk.clock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(d time.Duration) ports.Timer {
		timer := &pollerMockTimer{
			mock:     portsmocks.NewMockTimer(t),
			duration: d,
			ch:       make(chan time.Time, 1),
		}
		// Stop is called 0, 1, or 2 times per timer depending on whether it is
		// replaced mid-loop or torn down by the poller's deferred Stop on
		// exit, so its expectation is unconstrained.
		timer.mock.EXPECT().C().Maybe().Return((<-chan time.Time)(timer.ch))
		timer.mock.EXPECT().Stop().Maybe().Return(true)
		clk.timers <- timer
		return timer.mock
	}).Maybe()
	return clk
}

// TestBarScriptPollerRearmsTicksReloadsAndExitsOnCancel drives barScriptPoller
// directly (not through Serve) with a mock clock, covering the three
// behaviors the Task 5 rewrite touched but left untested:
//
//  1. after a tick fires, the loop arms a NEW timer instead of blocking
//     forever (the regression that would silently stop bar refreshes),
//  2. a reload signal re-arms the timer at the newly configured interval
//     rather than the one the poller started with, and
//  3. cancelling the context makes the goroutine return instead of leaking.
func TestBarScriptPollerRearmsTicksReloadsAndExitsOnCancel(t *testing.T) {
	clk := newPollerMockClock(t, 8)
	d := newBarRefreshTestDaemon(&fakeBarRunner{}, 2*time.Second)
	d.clock = clk.clock
	d.barScripts.reload = make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.barScriptPoller(ctx)
		close(done)
	}()

	// Stage 1: the poller arms its first timer at the configured interval.
	var timer0 *pollerMockTimer
	select {
	case timer0 = <-clk.timers:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the poller's initial timer")
	}
	require.Equal(t, 2*time.Second, timer0.duration)

	// Stage 2: firing that timer processes the tick and re-arms a new one.
	timer0.ch <- time.Unix(0, 0)
	var timer1 *pollerMockTimer
	select {
	case timer1 = <-clk.timers:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the poller to re-arm after a tick")
	}
	require.Equal(t, 2*time.Second, timer1.duration, "tick re-arm should keep the same interval when config is unchanged")

	// Stage 3: changing the interval and signaling reload re-arms at the NEW
	// interval instead of waiting out the timer armed under the old one.
	d.barScripts.mu.Lock()
	d.barScripts.cfg.interval = 7 * time.Second
	d.barScripts.mu.Unlock()
	d.barScripts.reload <- struct{}{}
	var timer2 *pollerMockTimer
	select {
	case timer2 = <-clk.timers:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the poller to re-arm after reload")
	}
	require.Equal(t, 7*time.Second, timer2.duration, "reload should re-arm at the newly configured interval")

	// Stage 4: cancelling the context exits the goroutine instead of leaking it.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("barScriptPoller did not exit after context cancellation")
	}
}

func TestApplyConfigSignalsPollerReload(t *testing.T) {
	r := &fakeBarRunner{}
	d := newBarRefreshTestDaemon(r, 60*time.Second)
	d.barScripts.reload = make(chan struct{}, 1)

	cfg := domain.Defaults()
	cfg.Bar.TopRight = "t"
	cfg.Bar.BottomRight = "b"
	cfg.Bar.Interval = time.Second
	d.ApplyConfig(cfg)

	select {
	case <-d.barScripts.reload:
	default:
		t.Fatal("ApplyConfig should signal the poller to re-arm its timer")
	}
	require.Equal(t, time.Second, d.barScriptInterval())
}

func newBarRefreshTestDaemon(r barScriptExecutor, interval time.Duration) *Daemon {
	d := &Daemon{clock: barRefreshTestClock{}, barScripts: &barScriptState{
		cfg:         barScriptConfig{topRight: "top", bottomRight: "bottom", interval: effectiveBarInterval(interval)},
		runner:      r,
		outputs:     make(map[domain.SessionID]barScriptOutputs),
		lastRefresh: make(map[domain.SessionID]time.Time),
		lastContext: make(map[domain.SessionID]barScriptContext),
		running:     make(map[domain.SessionID]bool),
		pending:     make(map[domain.SessionID]bool),
	}}
	return d
}

type barRefreshTestClock struct{}

func (barRefreshTestClock) Now() time.Time { return time.Now() }
func (barRefreshTestClock) NewTimer(d time.Duration) ports.Timer {
	return realTimer{t: time.NewTimer(d)}
}

func newBarRefreshTestSession() *session {
	inactive := newTabWithStableID("tab-inactive", "pane-inactive", nil, domain.Size{Cols: 80, Rows: 22})
	active := newTabWithStableID("tab-active", "pane-active", nil, domain.Size{Cols: 80, Rows: 22})
	active.tree.Focus = layout.PaneID("pane-1")
	return &session{id: "s", name: "s", cwd: "/tmp", tabs: []*tab{inactive, active}, active: 0, ctx: context.Background()}
}

func waitBarRefreshIdle(t *testing.T, d *Daemon) {
	t.Helper()
	require.Eventually(t, func() bool {
		d.barScripts.mu.Lock()
		defer d.barScripts.mu.Unlock()
		return len(d.barScripts.running) == 0
	}, time.Second, time.Millisecond)
}

type envRecordingRunner struct {
	mu   sync.Mutex
	envs [][]string
}

func (r *envRecordingRunner) run(_ context.Context, _ string, env []string, _ barScriptContext) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.envs = append(r.envs, append([]string(nil), env...))
	return "out", nil
}

func (r *envRecordingRunner) captured() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]string(nil), r.envs...)
}

func TestBarScriptsUseSessionEnvNotDaemonBaseEnv(t *testing.T) {
	tests := []struct {
		name     string
		sessEnv  []string
		baseEnv  []string
		wantPath string
	}{
		{
			name:     "session env wins over stale daemon env",
			sessEnv:  []string{"PATH=/home/u/.local/bin:/usr/bin"},
			baseEnv:  []string{"PATH=/usr/bin"},
			wantPath: "PATH=/home/u/.local/bin:/usr/bin",
		},
		{
			name:     "falls back to daemon env when session env empty",
			sessEnv:  nil,
			baseEnv:  []string{"PATH=/usr/bin"},
			wantPath: "PATH=/usr/bin",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &envRecordingRunner{}
			d := newBarRefreshTestDaemon(r, time.Second)
			d.baseEnv = tc.baseEnv
			sess := newBarRefreshTestSession()
			sess.env = tc.sessEnv
			sess.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

			require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), true))
			waitBarRefreshIdle(t, d)

			envs := r.captured()
			require.Len(t, envs, 2, "one call per anchor")
			for _, env := range envs {
				require.Contains(t, env, tc.wantPath)
			}
		})
	}
}
