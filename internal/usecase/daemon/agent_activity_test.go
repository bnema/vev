package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/agentactivity"
)

const agentTestShellPid = 4242

// agentTestPTY is a MockPTY whose foreground pgid the test controls.
type agentTestPTY struct {
	mock  *portsmocks.MockPTY
	pgid  atomic.Int64
	calls atomic.Int64
}

func newAgentTestPTY(t *testing.T) *agentTestPTY {
	t.Helper()
	p := &agentTestPTY{mock: portsmocks.NewMockPTY(t)}
	p.pgid.Store(agentTestShellPid)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	p.mock.EXPECT().Read(mock.Anything).RunAndReturn(func([]byte) (int, error) {
		<-release
		return 0, context.Canceled
	}).Maybe()
	p.mock.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) { return len(b), nil }).Maybe()
	p.mock.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	p.mock.EXPECT().Close().Return(nil).Maybe()
	p.mock.EXPECT().Pid().Return(agentTestShellPid).Maybe()
	p.mock.EXPECT().ForegroundPgid().RunAndReturn(func() (int, error) {
		p.calls.Add(1)
		return int(p.pgid.Load()), nil
	}).Maybe()
	return p
}

type agentTestRig struct {
	d     *Daemon
	sess  *session
	ac    *attachedClient
	sends chan wire.Envelope
	ptys  []*agentTestPTY
	proc  *portsmocks.MockProcessInspector
	sched *agentActivityScheduler
	now   time.Time
}

func newAgentTestRig(t *testing.T, tabs int) *agentTestRig {
	t.Helper()
	ptys := make([]*agentTestPTY, tabs)
	raw := make([]ports.PTY, tabs)
	for i := range ptys {
		ptys[i] = newAgentTestPTY(t)
		raw[i] = ptys[i].mock
	}
	d, sess, ac, sends := newManualSessionWithPTYs(t, raw...)
	proc := portsmocks.NewMockProcessInspector(t)
	// Pane title refreshes on paint read comm; agent identification only
	// falls back to it when Argv and GroupArgv fail, which no test expects.
	proc.EXPECT().Comm(mock.Anything).Return("", errors.New("no comm")).Maybe()
	d.proc = proc
	r := &agentTestRig{d: d, sess: sess, ac: ac, sends: sends, ptys: ptys, proc: proc, sched: d.agents, now: time.Unix(1_000, 0)}
	// The first step scans every live pane once; start each test after it.
	r.sched.step(r.now)
	r.settle()
	for _, p := range ptys {
		p.calls.Store(0)
	}
	return r
}

func (r *agentTestRig) pane(i int) *pane { return r.sess.tabs[i].focusedPane() }

// seedAgentStatus publishes status for p in tb as the scheduler goroutine
// would. Tests call it only while that goroutine is not running.
func seedAgentStatus(d *Daemon, sess *session, tb *tab, p *pane, status agentPaneStatus) {
	s := d.agents
	tr := s.tabRecord(tb, sess)
	if status.done && status.doneGen == 0 {
		status.doneGen = s.gen + 1
	}
	if status.done && !status.hidden && status.shownGen == 0 {
		status.shownGen = status.doneGen
	}
	tr.panes[p] = status
	s.republish(tr)
}

// paneVisibilitySeq reads tb's pane visibility sequence as a paint captures
// it.
func paneVisibilitySeq(tb *tab) uint64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.paneVisibilitySeq
}

// advance moves the fake time and runs the scheduler once.
func (r *agentTestRig) advance(d time.Duration) {
	r.now = r.now.Add(d)
	r.sched.step(r.now)
}

// output writes rows at the bottom of pane i's screen and records the output.
func (r *agentTestRig) output(i int, rows ...string) {
	r.screen(i, rows...)
	p := r.pane(i)
	p.mu.Lock()
	p.refreshTerminalTitleLocked()
	p.mu.Unlock()
	r.sched.markDirty(p)
	// The scheduler goroutine drains as soon as it is woken.
	r.advance(0)
}

// settle lets the coalescing window pass.
func (r *agentTestRig) settle() { r.advance(agentScanMaxLatency) }

// runAgent makes argv the foreground group leader of pane i.
func (r *agentTestRig) runAgent(i int, pgid int, argv ...string) {
	r.ptys[i].pgid.Store(int64(pgid))
	r.proc.EXPECT().Argv(pgid).Return(argv, nil).Maybe()
}

// screen writes rows at the bottom of pane i's screen without recording
// output.
func (r *agentTestRig) screen(i int, rows ...string) {
	p := r.pane(i)
	p.mu.Lock()
	top := p.screen.Rows() - len(rows) + 1
	p.screen.Write([]byte(fmt.Sprintf("\x1b[2J\x1b[%d;1H", top) + strings.Join(rows, "\r\n")))
	p.mu.Unlock()
}

var (
	claudeWorkingRows = []string{"● Reading files", "", "✻ Thinking… (3s · esc to interrupt)"}
	claudeIdleRows    = []string{"● Done.", "", "❯ "}
	claudeBlockedRows = []string{"Bash command", "", " Do you want to proceed?", " ❯ 1. Yes", "   2. No"}
)

func TestAgentActivitySchedulerCoalesces(t *testing.T) {
	tests := []struct {
		name      string
		steps     int
		every     time.Duration
		wantScans int64
	}{
		{name: "burst within quiet window scans once", steps: 1, every: 0, wantScans: 1},
		// 360 ms of output every 30 ms: one scan per 120 ms latency bound,
		// then the trailing scan once output stops.
		{name: "continuous output scans at the latency bound", steps: 12, every: 30 * time.Millisecond, wantScans: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 1)
			p := r.pane(0)
			for range 50 {
				r.sched.markDirty(p)
			}
			r.advance(0)
			for range tt.steps {
				r.advance(tt.every)
				r.sched.markDirty(p)
				r.advance(0)
			}
			r.advance(agentScanMaxLatency)
			require.Equal(t, tt.wantScans, r.ptys[0].calls.Load())
		})
	}
}

func TestAgentActivitySchedulerNextDeadline(t *testing.T) {
	r := newAgentTestRig(t, 1)
	require.True(t, r.sched.nextDeadline().IsZero(), "an idle scheduler parks")
	r.sched.markDirty(r.pane(0))
	r.advance(0)
	require.Equal(t, r.now.Add(agentScanQuiet), r.sched.nextDeadline())
	r.settle()
	require.True(t, r.sched.nextDeadline().IsZero(), "a scanned shell pane leaves no deadline")
}

func TestAgentActivityShellPaneSkipsProc(t *testing.T) {
	r := newAgentTestRig(t, 1)
	for range 3 {
		r.output(0, "$ ls", "file")
		r.settle()
	}
	// The mock inspector has no expectations: any /proc read fails the test.
	require.Equal(t, int64(3), r.ptys[0].calls.Load())
	require.Equal(t, agentactivity.IndicatorNone, r.sess.tabs[0].agentIndicator())
}

func TestAgentActivityIdentifyCaching(t *testing.T) {
	tests := []struct {
		name     string
		gap      time.Duration
		wantArgv int
	}{
		{name: "same pgid is cached", gap: time.Second, wantArgv: 1},
		{name: "identified agent is re-verified", gap: agentReverifyInterval, wantArgv: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 1)
			r.runAgent(0, 777, "claude")
			r.output(0, claudeWorkingRows...)
			r.settle()
			r.advance(tt.gap)
			r.output(0, claudeWorkingRows...)
			r.settle()
			r.proc.AssertNumberOfCalls(t, "Argv", tt.wantArgv)
		})
	}
}

func TestAgentActivityNoneIsReverified(t *testing.T) {
	// A pgid that is no agent is re-read after 1 s, then with a doubling
	// interval starting at agentReverifyInterval.
	tests := []struct {
		name     string
		gaps     []time.Duration
		wantArgv int
	}{
		{name: "cached within the first retry", gaps: []time.Duration{500 * time.Millisecond}, wantArgv: 1},
		{name: "first retry after one second", gaps: []time.Duration{agentNoneReverifyFirst}, wantArgv: 2},
		{name: "second retry backs off", gaps: []time.Duration{agentNoneReverifyFirst, agentNoneReverifyFirst}, wantArgv: 2},
		{name: "second retry after the reverify interval", gaps: []time.Duration{agentNoneReverifyFirst, agentReverifyInterval}, wantArgv: 3},
		{name: "third retry doubles", gaps: []time.Duration{agentNoneReverifyFirst, agentReverifyInterval, agentReverifyInterval}, wantArgv: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 1)
			r.runAgent(0, 777, "vim")
			r.output(0, "~")
			r.settle()
			for _, gap := range tt.gaps {
				r.advance(gap - agentScanMaxLatency)
				r.output(0, "~")
				r.settle()
			}
			r.proc.AssertNumberOfCalls(t, "Argv", tt.wantArgv)
		})
	}
}

func TestAgentActivityExecIntoAgentIsDetected(t *testing.T) {
	r := newAgentTestRig(t, 1)
	argv := []string{"sh"}
	r.ptys[0].pgid.Store(777)
	r.proc.EXPECT().Argv(777).RunAndReturn(func(int) ([]string, error) { return argv, nil })
	r.output(0, "$")
	r.settle()
	// The same group leader execs the agent.
	argv = []string{"claude"}
	r.advance(agentNoneReverifyFirst)
	r.output(0, claudeWorkingRows...)
	r.settle()
	require.Equal(t, agentactivity.IndicatorWorking, r.sess.tabs[0].agentIndicator())
}

func TestAgentActivityIdentifySource(t *testing.T) {
	tests := []struct {
		name      string
		argv      []string
		argvErr   error
		wantGroup bool
	}{
		{name: "group leader argv", argv: []string{"claude"}},
		{name: "empty leader argv falls back to the group", wantGroup: true},
		{name: "leader gone falls back to the group", argvErr: errors.New("gone"), wantGroup: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 1)
			r.ptys[0].pgid.Store(777)
			r.proc.EXPECT().Argv(777).Return(tt.argv, tt.argvErr)
			if tt.wantGroup {
				r.proc.EXPECT().GroupArgv(777, agentTestShellPid).Return([]string{"claude"}, nil)
			}
			r.output(0, claudeWorkingRows...)
			r.settle()
			require.Equal(t, agentactivity.IndicatorWorking, r.sess.tabs[0].agentIndicator())
		})
	}
}

func TestAgentActivityIdentifyOnPgidChange(t *testing.T) {
	r := newAgentTestRig(t, 1)
	r.runAgent(0, 777, "vim")
	r.output(0, "~")
	r.settle()
	r.output(0, "~")
	r.settle()
	r.proc.AssertNumberOfCalls(t, "Argv", 1)
	r.runAgent(0, 778, "node", "/usr/lib/node_modules/@openai/codex/bin/codex.js")
	r.output(0, "~")
	r.settle()
	r.proc.AssertNumberOfCalls(t, "Argv", 2)
}

func TestAgentActivityTransitions(t *testing.T) {
	tests := []struct {
		name string
		rows [][]string
		gaps []time.Duration
		want agentactivity.Indicator
	}{
		{name: "working", rows: [][]string{claudeWorkingRows}, want: agentactivity.IndicatorWorking},
		{name: "blocked", rows: [][]string{claudeBlockedRows}, want: agentactivity.IndicatorBlocked},
		{name: "short turn ends idle", rows: [][]string{claudeWorkingRows, claudeIdleRows}, gaps: []time.Duration{0, time.Second}, want: agentactivity.IndicatorIdle},
		{name: "long turn ends done", rows: [][]string{claudeWorkingRows, claudeIdleRows}, gaps: []time.Duration{0, 3 * time.Second}, want: agentactivity.IndicatorDone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			selectTestAttachmentTab(r.sess, 0)
			r.runAgent(1, 777, "claude")
			for i, rows := range tt.rows {
				if tt.gaps != nil {
					r.advance(tt.gaps[i])
				}
				r.output(1, rows...)
				r.settle()
			}
			// Resolve a pending idle hold.
			r.advance(agentactivity.DefaultIdleHoldCap)
			require.Equal(t, tt.want, r.sess.tabs[1].agentIndicator())
		})
	}
}

func TestAgentActivityBlockedRaisesAttentionOnce(t *testing.T) {
	r := newAgentTestRig(t, 2)
	selectTestAttachmentTab(r.sess, 0)
	r.runAgent(1, 777, "claude")
	r.output(1, claudeBlockedRows...)
	r.settle()
	tb := r.sess.tabs[1]
	r.sess.mu.Lock()
	require.True(t, tb.attention)
	tb.attention = false
	r.sess.mu.Unlock()

	r.output(1, append(claudeBlockedRows, "")...)
	r.settle()
	r.sess.mu.Lock()
	require.False(t, tb.attention, "a persisting Blocked episode raises attention once")
	r.sess.mu.Unlock()
	require.Equal(t, agentactivity.IndicatorBlocked, tb.agentIndicator(), "the indicator persists after the bell is acknowledged")
}

func TestAgentActivityTabAggregation(t *testing.T) {
	tests := []struct {
		name  string
		panes []agentPaneStatus
		want  agentactivity.Indicator
	}{
		{name: "done beats working", panes: []agentPaneStatus{{state: agentactivity.Idle, done: true}, {state: agentactivity.Working}}, want: agentactivity.IndicatorDone},
		{name: "blocked beats done", panes: []agentPaneStatus{{state: agentactivity.Blocked}, {state: agentactivity.Idle, done: true}}, want: agentactivity.IndicatorBlocked},
		{name: "blocked beats working", panes: []agentPaneStatus{{state: agentactivity.Working}, {state: agentactivity.Blocked}}, want: agentactivity.IndicatorBlocked},
		{name: "working beats idle", panes: []agentPaneStatus{{state: agentactivity.Idle}, {state: agentactivity.Working}}, want: agentactivity.IndicatorWorking},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 1)
			tb := r.sess.tabs[0]
			for _, status := range tt.panes {
				seedAgentStatus(r.d, r.sess, tb, &pane{}, status)
			}
			require.Equal(t, tt.want, tb.agentIndicator())
		})
	}
}

func TestAgentActivityDoneOnlyWhenUnseen(t *testing.T) {
	tests := []struct {
		name     string
		shownTab int
		focus    domain.TerminalFocus
		wantDone bool
	}{
		{name: "background tab", shownTab: 0, focus: domain.TerminalFocusUnknown, wantDone: true},
		{name: "shown focused tab", shownTab: 1, focus: domain.TerminalFocusUnknown, wantDone: false},
		{name: "shown tab in unfocused window", shownTab: 1, focus: domain.TerminalFocusUnfocused, wantDone: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			selectTestAttachmentTab(r.sess, tt.shownTab)
			r.ac.setTerminalFocus(tt.focus)
			r.runAgent(1, 777, "claude")
			r.output(1, claudeWorkingRows...)
			r.settle()
			r.advance(3 * time.Second)
			r.output(1, claudeIdleRows...)
			r.settle()
			r.advance(agentactivity.DefaultIdleHoldCap)
			got := r.sess.tabs[1].agentIndicator() == agentactivity.IndicatorDone
			require.Equal(t, tt.wantDone, got)
		})
	}
}

func TestAgentActivityDoneClearedByAck(t *testing.T) {
	r := newAgentTestRig(t, 2)
	tb := r.sess.tabs[1]
	p := r.pane(1)
	seedAgentStatus(r.d, r.sess, tb, p, agentPaneStatus{state: agentactivity.Idle, done: true})
	gen := tb.agentState.Load().gen
	selectTestAttachmentTab(r.sess, 0)
	seq := paneVisibilitySeq(tb)
	require.False(t, r.sess.ackAgentDone(r.d, tb, r.ac, gen, seq), "another tab does not acknowledge")
	selectTestAttachmentTab(r.sess, 1)
	r.ac.setTerminalFocus(domain.TerminalFocusUnfocused)
	require.False(t, r.sess.ackAgentDone(r.d, tb, r.ac, gen, seq), "an unfocused window does not acknowledge")
	r.advance(0)
	require.Equal(t, agentactivity.IndicatorDone, tb.agentIndicator())
	r.ac.setTerminalFocus(domain.TerminalFocusFocused)
	require.True(t, r.sess.ackAgentDone(r.d, tb, r.ac, gen, seq))
	require.Equal(t, agentactivity.IndicatorDone, tb.agentIndicator(), "paint does not mutate the published state")
	r.advance(0)
	require.Equal(t, agentactivity.IndicatorIdle, tb.agentIndicator())
}

func TestAgentActivityAckKeepsNewerDone(t *testing.T) {
	tests := []struct {
		name     string
		overflow bool
	}{
		{name: "queued ack"},
		{name: "overflowed ack sweeps"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 3)
			selectTestAttachmentTab(r.sess, 0)
			a, b := r.sess.tabs[1], r.sess.tabs[2]
			seedAgentStatus(r.d, r.sess, a, r.pane(1), agentPaneStatus{state: agentactivity.Idle, done: true})
			rendered := a.agentState.Load().gen
			// A second completion published after the paint rendered gen.
			seedAgentStatus(r.d, r.sess, a, &pane{}, agentPaneStatus{state: agentactivity.Idle, done: true})
			seedAgentStatus(r.d, r.sess, b, r.pane(2), agentPaneStatus{state: agentactivity.Idle, done: true})
			if tt.overflow {
				for len(r.sched.acks) < cap(r.sched.acks) {
					r.sched.acks <- b
				}
			}
			selectTestAttachmentTab(r.sess, 1)
			require.True(t, r.sess.ackAgentDone(r.d, a, r.ac, rendered, paneVisibilitySeq(a)))
			r.advance(0)
			require.Equal(t, agentactivity.IndicatorDone, a.agentIndicator(), "a completion newer than the rendered generation survives")
			r.sched.requestAck(a, a.agentState.Load().gen, paneVisibilitySeq(a))
			r.advance(0)
			require.Equal(t, agentactivity.IndicatorIdle, a.agentIndicator())
			require.Equal(t, agentactivity.IndicatorDone, b.agentIndicator(), "another tab's completion is not acknowledged")
		})
	}
}

func TestAgentActivityInboxOverflowRescans(t *testing.T) {
	r := newAgentTestRig(t, 1)
	r.runAgent(0, 777, "claude")
	stranger := &pane{}
	for len(r.sched.dirty) < cap(r.sched.dirty) {
		stranger.activityDirty.Store(false)
		r.sched.dirty <- stranger
	}
	p := r.pane(0)
	p.mu.Lock()
	p.screen.Write([]byte("\x1b[2J\x1b[" + fmt.Sprint(p.screen.Rows()-2) + ";1H" + strings.Join(claudeWorkingRows, "\r\n")))
	p.mu.Unlock()
	r.sched.markDirty(p)
	require.True(t, r.sched.rescanAll.Load(), "a full inbox falls back to a sweep")
	r.advance(0)
	r.settle()
	require.Equal(t, agentactivity.IndicatorWorking, r.sess.tabs[0].agentIndicator())
}

func TestAgentActivityCleanup(t *testing.T) {
	tests := []struct {
		name    string
		cleanup func(r *agentTestRig)
	}{
		{name: "agent exits", cleanup: func(r *agentTestRig) {
			r.ptys[1].pgid.Store(agentTestShellPid)
			r.output(1, "$")
			r.settle()
		}},
		{name: "pane closes", cleanup: func(r *agentTestRig) {
			// The pane-gone notification runs on its own goroutine and
			// signals the scheduler's wake channel.
			select {
			case <-r.sched.wake:
			default:
			}
			r.pane(1).cancel()
			<-r.sched.wake
			r.advance(0)
		}},
		{name: "activity turned off", cleanup: func(r *agentTestRig) {
			cfg := domain.Defaults()
			cfg.Agents.Activity = false
			r.d.ApplyConfig(cfg)
			r.advance(0)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			selectTestAttachmentTab(r.sess, 0)
			r.runAgent(1, 777, "claude")
			r.output(1, claudeWorkingRows...)
			r.settle()
			require.Equal(t, agentactivity.IndicatorWorking, r.sess.tabs[1].agentIndicator())
			tt.cleanup(r)
			require.Equal(t, agentactivity.IndicatorNone, r.sess.tabs[1].agentIndicator())
		})
	}
}

func TestAgentActivityOffIgnoresOutput(t *testing.T) {
	r := newAgentTestRig(t, 1)
	cfg := domain.Defaults()
	cfg.Agents.Activity = false
	r.d.ApplyConfig(cfg)
	r.advance(0)
	r.d.noteAgentOutput(r.pane(0))
	r.settle()
	require.Empty(t, r.sched.records)
	require.True(t, r.sched.nextDeadline().IsZero())
}

// TestAgentActivitySchedulerRunArmsOneTimerAndExits drives the goroutine with
// a fake clock: a dirty pane arms the timer, its firing scans, and cancel
// returns.
func TestAgentActivitySchedulerRunArmsOneTimerAndExits(t *testing.T) {
	r := newAgentTestRig(t, 1)
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().Return(r.now).Maybe()
	timerC := make(chan time.Time, 1)
	timer := portsmocks.NewMockTimer(t)
	timer.EXPECT().C().Return((<-chan time.Time)(timerC)).Maybe()
	timer.EXPECT().Stop().Return(true).Maybe()
	timer.EXPECT().Reset(mock.Anything).Return(true).Maybe()
	armed := make(chan time.Duration, 8)
	clock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(d time.Duration) ports.Timer {
		armed <- d
		return timer
	}).Maybe()
	r.d.clock = clock
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.sched.run(ctx)
		close(done)
	}()
	r.sched.markDirty(r.pane(0))
	require.Equal(t, agentScanQuiet, <-armed)
	cancel()
	<-done
	require.Empty(t, armed, "one timer serves every pane")
}

func TestAgentActivityScansStaticScreensWhenEnabled(t *testing.T) {
	tests := []struct {
		name   string
		enable func(r *agentTestRig)
	}{
		{name: "activity turned on", enable: func(r *agentTestRig) {
			cfg := domain.Defaults()
			cfg.Agents.Activity = false
			r.d.ApplyConfig(cfg)
			r.advance(0)
			r.screen(1, claudeBlockedRows...)
			cfg.Agents.Activity = true
			r.d.ApplyConfig(cfg)
		}},
		{name: "daemon startup with restored panes", enable: func(r *agentTestRig) {
			r.screen(1, claudeBlockedRows...)
			r.sched = newAgentActivityScheduler(r.d)
			r.d.agents = r.sched
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			selectTestAttachmentTab(r.sess, 0)
			r.runAgent(1, 777, "claude")
			tt.enable(r)
			r.advance(0)
			r.settle()
			require.Equal(t, agentactivity.IndicatorBlocked, r.sess.tabs[1].agentIndicator())
		})
	}
}

func TestAgentActivityRepublishesOnPaneMove(t *testing.T) {
	r := newAgentTestRig(t, 3)
	selectTestAttachmentTab(r.sess, 0)
	source, destination := r.sess.tabs[1], r.sess.tabs[2]
	moved := r.pane(1)
	// The pane outlives its source tab.
	moved.ctx, moved.cancel = context.WithCancel(r.d.paneProcessCtx)
	t.Cleanup(moved.cancel)
	r.runAgent(1, 777, "claude")
	r.output(1, claudeWorkingRows...)
	r.settle()
	require.Equal(t, agentactivity.IndicatorWorking, source.agentIndicator())

	require.NoError(t, r.d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: r.sess.id, Incarnation: r.sess.incarnation},
		SourceTabID:      domain.TabStableID(source.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: r.sess.id, Incarnation: r.sess.incarnation},
		DestinationTabID: domain.TabStableID(destination.stableID),
	}))
	require.Equal(t, moved, destination.panes[moved.id])
	r.advance(0)
	r.settle()
	require.Equal(t, agentactivity.IndicatorWorking, destination.agentIndicator())
	require.Equal(t, agentactivity.IndicatorNone, source.agentIndicator())
}

// zut rows as its redraw paints them: status bar, cwd, blank, prompt.
var (
	zutIdleRows    = []string{"  answer 0 with some words", "", "  [1] gpt-5.4  ctx0%/272k", "  ~/dev/projects/demo", "", "▌"}
	zutWorkingRows = []string{"  answer 0 with some words", "", "  ⠋ Running tool: bash - 3s  [1] gpt-5.4  ctx0%/272k", "  ~/dev/projects/demo", "", "▌"}
)

// outputTop writes rows from the top of pane i's screen, leaving the rows
// below blank as zut does under a short transcript, and records the output.
func (r *agentTestRig) outputTop(i int, rows ...string) {
	p := r.pane(i)
	p.mu.Lock()
	p.screen.Write([]byte("\x1b[2J\x1b[1;1H" + strings.Join(rows, "\r\n")))
	p.mu.Unlock()
	r.sched.markDirty(p)
	r.advance(0)
}

func TestAgentActivityZutTurn(t *testing.T) {
	tests := []struct {
		name string
		top  bool
	}{
		{name: "content at the grid bottom"},
		{name: "short transcript at the grid top", top: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			selectTestAttachmentTab(r.sess, 0)
			r.runAgent(1, 777, "zut")
			tb := r.sess.tabs[1]
			write := r.output
			if tt.top {
				write = r.outputTop
			}

			write(1, zutIdleRows...)
			r.settle()
			r.advance(agentactivity.DefaultStartupGrace)
			require.Equal(t, agentactivity.IndicatorIdle, tb.agentIndicator(), "settled zut is idle")
			require.True(t, r.sched.nextDeadline().IsZero(), "an idle pane parks the scheduler")

			// Output long after the idle publish starts a new scan.
			r.advance(10 * time.Second)
			write(1, zutWorkingRows...)
			r.settle()
			require.Equal(t, agentactivity.IndicatorWorking, tb.agentIndicator())

			r.advance(agentactivity.DefaultMinDoneWorking)
			write(1, zutIdleRows...)
			r.settle()
			r.advance(agentactivity.DefaultIdleHoldCap)
			require.Equal(t, agentactivity.IndicatorDone, tb.agentIndicator())
		})
	}
}

// bell feeds a terminal BEL through pane i's PTY parsing path, as the reader
// does.
func (r *agentTestRig) bell(i int) {
	p := r.pane(i)
	p.mu.Lock()
	p.installScreenEffectsLocked()
	p.mu.Unlock()
	r.d.processPTYData(r.sess, r.sess.tabs[i], p, []byte("\a"), false)
	r.advance(0)
}

func (r *agentTestRig) tabAttention(i int) bool {
	r.sess.mu.Lock()
	defer r.sess.mu.Unlock()
	return r.sess.tabs[i].attention
}

func (r *agentTestRig) clearAttention(i int) {
	r.sess.mu.Lock()
	r.sess.tabs[i].attention = false
	r.sess.mu.Unlock()
}

// zutTurn runs one zut turn in background tab 1 that ends with a BEL, as zut
// rings at the end of every turn.
func (r *agentTestRig) zutTurn() {
	r.runAgent(1, 777, "zut")
	r.output(1, zutIdleRows...)
	r.settle()
	r.advance(agentactivity.DefaultStartupGrace)
	r.clearAttention(1)
	r.output(1, zutWorkingRows...)
	r.settle()
	r.advance(agentactivity.DefaultMinDoneWorking)
	r.output(1, zutIdleRows...)
	r.bell(1)
	r.settle()
	r.advance(agentactivity.DefaultIdleHoldCap)
}

func TestAgentActivityTerminalBell(t *testing.T) {
	tests := []struct {
		name          string
		run           func(r *agentTestRig)
		wantAttention bool
		wantTab       statusActivity
		wantEntry     statusActivity
	}{
		{
			name:      "zut turn ending with a bell shows done",
			run:       func(r *agentTestRig) { r.zutTurn() },
			wantTab:   activityDone,
			wantEntry: activityDone,
		},
		{
			name: "plain shell bell rings",
			run: func(r *agentTestRig) {
				r.output(1, "$ ")
				r.settle()
				r.bell(1)
			},
			wantAttention: true,
			wantTab:       activityBell,
			wantEntry:     activityBell,
		},
		{
			name: "activity turned off rings again",
			run: func(r *agentTestRig) {
				r.zutTurn()
				cfg := domain.Defaults()
				cfg.Agents.Activity = false
				// The scheduler has not cleared the pane yet: the config
				// gate alone restores the bell.
				r.d.ApplyConfig(cfg)
				r.bell(1)
				require.False(t, r.pane(1).agentOwned.Load(), "disabling forgets the agent")
			},
			wantAttention: true,
			wantTab:       activityBell,
			wantEntry:     activityBell,
		},
		{
			name: "agent exit rings again",
			run: func(r *agentTestRig) {
				r.zutTurn()
				r.ptys[1].pgid.Store(agentTestShellPid)
				r.output(1, "$ ")
				r.settle()
				r.bell(1)
			},
			wantAttention: true,
			wantTab:       activityBell,
			wantEntry:     activityBell,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			selectTestAttachmentTab(r.sess, 0)
			tt.run(r)

			require.Equal(t, tt.wantAttention, r.tabAttention(1))
			agents := r.d.currentRuntimeConfig().agents.Activity
			snap := r.sess.statusSegmentsWithAgents(r.ac, false, agents)
			require.Equal(t, tt.wantTab, snap.tabs[1].activity, "tab cell")

			// The bottom bar of another session draws this session's entry
			// from its route attention and the published session index.
			view := r.sess.snapshotView(viewOptions{})
			indicator := agentactivity.IndicatorNone
			if lookup := r.d.agentSessionLookup(); lookup != nil {
				indicator = lookup(r.sess.incarnation)
			}
			require.Equal(t, tt.wantEntry, entryCell(view.hasAttention, sessionEntryActivity(indicator, true)), "session entry")
		})
	}
}

func TestAgentActivityBlockedBellRaisesAttentionOnce(t *testing.T) {
	r := newAgentTestRig(t, 2)
	selectTestAttachmentTab(r.sess, 0)
	r.runAgent(1, 777, "claude")
	r.output(1, claudeWorkingRows...)
	r.settle()
	r.advance(agentactivity.DefaultStartupGrace)
	r.clearAttention(1)

	r.output(1, claudeBlockedRows...)
	r.bell(1)
	r.settle()
	require.True(t, r.tabAttention(1), "Blocked raises attention")
	require.Equal(t, activityBell, r.sess.statusSegmentsFor(r.ac, false).tabs[1].activity)
	r.clearAttention(1)

	r.bell(1)
	r.output(1, append(claudeBlockedRows, "")...)
	r.settle()
	require.False(t, r.tabAttention(1), "neither the bell nor the same Blocked episode raises again")
	require.Equal(t, activityBell, r.sess.statusSegmentsFor(r.ac, false).tabs[1].activity, "Blocked keeps the bell cell")
}
