package daemon

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/agentactivity"
)

// startClaudeTurn identifies an idle claude in background tab 1, lets the
// startup grace pass, and starts a Working turn with no attention pending.
func (r *agentTestRig) startClaudeTurn() {
	r.runAgent(1, 777, "claude")
	r.output(1, claudeIdleRows...)
	r.settle()
	r.advance(agentactivity.DefaultStartupGrace)
	r.clearAttention(1)
	r.output(1, claudeWorkingRows...)
	r.settle()
}

// endTurn shows the idle prompt after working and resolves the idle hold.
func (r *agentTestRig) endTurn(working time.Duration, bell bool) {
	r.advance(working)
	r.output(1, claudeIdleRows...)
	if bell {
		r.bell(1)
	}
	r.settle()
	r.advance(agentactivity.DefaultIdleHoldCap)
}

func TestAgentActivityDeferredBell(t *testing.T) {
	short := time.Second
	long := agentactivity.DefaultMinDoneWorking + time.Second
	tests := []struct {
		name          string
		run           func(r *agentTestRig)
		wantAttention bool
		wantIndicator agentactivity.Indicator
	}{
		{
			name:          "short turn ending with a bell rings",
			run:           func(r *agentTestRig) { r.endTurn(short, true) },
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorIdle,
		},
		{
			name: "bell during a short turn rings once it settles",
			run: func(r *agentTestRig) {
				r.bell(1)
				r.settle()
				require.False(t, r.tabAttention(1), "a Working turn keeps the bell pending")
				require.True(t, r.pane(1).pendingBell.Load())
				r.endTurn(short, false)
			},
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorIdle,
		},
		{
			name:          "long turn ending with a bell shows Done only",
			run:           func(r *agentTestRig) { r.endTurn(long, true) },
			wantIndicator: agentactivity.IndicatorDone,
		},
		{
			name: "bell during a long turn shows Done only",
			run: func(r *agentTestRig) {
				r.bell(1)
				r.endTurn(long, false)
			},
			wantIndicator: agentactivity.IndicatorDone,
		},
		{
			name: "Blocked turn with a bell raises attention once",
			run: func(r *agentTestRig) {
				r.output(1, claudeBlockedRows...)
				r.bell(1)
				r.settle()
				require.True(t, r.tabAttention(1), "Blocked raises attention")
				r.clearAttention(1)
				r.endTurn(short, false)
			},
			wantIndicator: agentactivity.IndicatorIdle,
		},
		{
			name: "Blocked then idle without Done rings its bell",
			run: func(r *agentTestRig) {
				r.output(1, claudeBlockedRows...)
				r.settle()
				r.clearAttention(1)
				r.endTurn(short, true)
			},
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorIdle,
		},
		{
			name: "short turn with a bell after a Done turn rings",
			run: func(r *agentTestRig) {
				r.endTurn(long, false)
				require.Equal(t, agentactivity.IndicatorDone, r.sess.tabs[1].agentIndicator())
				r.output(1, claudeWorkingRows...)
				r.settle()
				r.endTurn(short, true)
			},
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorIdle,
		},
		{
			name: "bell scanned after the Done publish is absorbed",
			run: func(r *agentTestRig) {
				r.endTurn(long, false)
				r.bell(1)
				r.settle()
			},
			wantIndicator: agentactivity.IndicatorDone,
		},
		{
			name: "bell after the Done turn settled rings",
			run: func(r *agentTestRig) {
				r.endTurn(long, false)
				r.bell(1)
				r.settle()
				r.bell(1)
				r.settle()
			},
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorDone,
		},
		{
			name: "bell after the absorb window rings",
			run: func(r *agentTestRig) {
				r.endTurn(long, false)
				r.advance(agentBellAbsorbWindow)
				r.bell(1)
				r.settle()
			},
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorDone,
		},
		{
			name: "agent change after a Done turn rings a later bell",
			run: func(r *agentTestRig) {
				r.endTurn(long, false)
				r.runAgent(1, 778, "codex")
				r.bell(1)
				r.settle()
			},
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorDone,
		},
		{
			name: "agent change while a bell is pending rings it",
			run: func(r *agentTestRig) {
				r.bell(1)
				r.settle()
				require.True(t, r.pane(1).pendingBell.Load())
				r.runAgent(1, 778, "codex")
				r.output(1, claudeWorkingRows...)
				r.settle()
			},
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorNone,
		},
		{
			name: "agent exit while a bell is pending rings it",
			run: func(r *agentTestRig) {
				r.bell(1)
				r.settle()
				require.True(t, r.pane(1).pendingBell.Load())
				r.ptys[1].pgid.Store(agentTestShellPid)
				r.output(1, "$ ")
				r.settle()
			},
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorNone,
		},
		{
			name: "activity off rings at once",
			run: func(r *agentTestRig) {
				cfg := domain.Defaults()
				cfg.Agents.Activity = false
				r.d.ApplyConfig(cfg)
				p := r.pane(1)
				p.mu.Lock()
				p.installScreenEffectsLocked()
				p.mu.Unlock()
				// No scheduler step: the bell rings on the PTY path.
				r.d.processPTYData(r.sess, r.sess.tabs[1], p, []byte("\a"), false)
			},
			wantAttention: true,
			wantIndicator: agentactivity.IndicatorWorking,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			selectTestAttachmentTab(r.sess, 0)
			r.startClaudeTurn()
			tt.run(r)

			require.Equal(t, tt.wantAttention, r.tabAttention(1), "tab attention")
			require.Equal(t, tt.wantIndicator, r.sess.tabs[1].agentIndicator())
			require.False(t, r.pane(1).pendingBell.Load(), "the bell is resolved")
		})
	}
}

func TestAgentActivityPendingBellRingsWhenActivityStops(t *testing.T) {
	tests := []struct {
		name string
		stop func(r *agentTestRig)
	}{
		{
			name: "activity turned off",
			stop: func(r *agentTestRig) {
				cfg := domain.Defaults()
				cfg.Agents.Activity = false
				r.d.ApplyConfig(cfg)
				r.advance(0)
			},
		},
		{
			name: "bell deferred while activity turns off",
			stop: func(r *agentTestRig) {
				cfg := domain.Defaults()
				cfg.Agents.Activity = false
				r.d.ApplyConfig(cfg)
				r.advance(0)
				r.clearAttention(1)
				// A BEL deferred just before the PTY path saw the reload.
				p := r.pane(1)
				p.pendingBell.Store(true)
				r.sched.markDirty(p)
				r.advance(0)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			selectTestAttachmentTab(r.sess, 0)
			r.startClaudeTurn()
			r.bell(1)
			require.True(t, r.pane(1).pendingBell.Load())

			tt.stop(r)

			require.False(t, r.pane(1).pendingBell.Load(), "the bell is resolved")
			require.True(t, r.tabAttention(1), "the pending bell rings")
		})
	}
}

func TestAgentActivityPendingBellOfGonePaneIsDropped(t *testing.T) {
	r := newAgentTestRig(t, 2)
	selectTestAttachmentTab(r.sess, 0)
	r.startClaudeTurn()
	p := r.pane(1)
	r.bell(1)
	require.True(t, p.pendingBell.Load())

	p.cancel()
	r.sched.markGone(p)
	r.advance(0)

	require.False(t, p.pendingBell.Load())
	require.False(t, r.tabAttention(1), "a gone pane drops its bell")
}

func TestAgentActivityPendingBellFollowsMovedPane(t *testing.T) {
	r := newAgentTestRig(t, 3)
	selectTestAttachmentTab(r.sess, 0)
	source, destination := r.sess.tabs[1], r.sess.tabs[2]
	moved := r.pane(1)
	// The pane outlives its source tab, as a process lifetime does; track it
	// again under that lifetime.
	moved.ctx, moved.cancel = context.WithCancel(r.d.paneProcessCtx)
	t.Cleanup(moved.cancel)
	r.sched.forget(moved)
	r.startClaudeTurn()
	r.bell(1)
	r.settle()
	require.True(t, moved.pendingBell.Load())

	require.NoError(t, r.d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: r.sess.id, Incarnation: r.sess.incarnation},
		SourceTabID:      domain.TabStableID(source.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: r.sess.id, Incarnation: r.sess.incarnation},
		DestinationTabID: domain.TabStableID(destination.stableID),
	}))
	r.advance(0)
	r.settle()
	require.True(t, moved.pendingBell.Load(), "the moved Working turn keeps the bell pending")
	require.Equal(t, agentactivity.IndicatorWorking, destination.agentIndicator())

	// End the short turn on the destination tab.
	moved.mu.Lock()
	top := moved.screen.Rows() - len(claudeIdleRows) + 1
	moved.screen.Write([]byte(fmt.Sprintf("\x1b[2J\x1b[%d;1H", top) + strings.Join(claudeIdleRows, "\r\n")))
	moved.mu.Unlock()
	r.sched.markDirty(moved)
	r.advance(0)
	r.settle()
	r.advance(agentactivity.DefaultIdleHoldCap)

	require.False(t, moved.pendingBell.Load(), "the bell is resolved")
	require.Equal(t, agentactivity.IndicatorIdle, destination.agentIndicator(), "a short turn shows no Done")
	r.sess.mu.Lock()
	rang := destination.attention
	r.sess.mu.Unlock()
	require.True(t, rang, "the bell rings on the tab that owns the pane now")
}

func TestAgentActivityPendingBellsRingWhenDisabled(t *testing.T) {
	r := newAgentTestRig(t, 3)
	selectTestAttachmentTab(r.sess, 0)
	for i := 1; i <= 2; i++ {
		r.runAgent(i, 776+i, "claude")
		r.output(i, claudeIdleRows...)
	}
	r.settle()
	r.advance(agentactivity.DefaultStartupGrace)
	for i := 1; i <= 2; i++ {
		r.output(i, claudeWorkingRows...)
	}
	r.settle()
	for i := 1; i <= 2; i++ {
		r.clearAttention(i)
		r.bell(i)
		require.True(t, r.pane(i).pendingBell.Load())
		require.False(t, r.tabAttention(i))
	}

	cfg := domain.Defaults()
	cfg.Agents.Activity = false
	r.d.ApplyConfig(cfg)
	r.advance(0)

	for i := 1; i <= 2; i++ {
		require.False(t, r.pane(i).pendingBell.Load(), "pane %d bell resolved", i)
		require.True(t, r.tabAttention(i), "pane %d bell rings", i)
		r.clearAttention(i)
	}
	r.advance(0)
	for i := 1; i <= 2; i++ {
		require.False(t, r.tabAttention(i), "pane %d bell rings once", i)
	}
}

func TestAgentActivityResolvePendingBellOnAgentChange(t *testing.T) {
	now := time.Unix(1_000, 0)
	tests := []struct {
		name    string
		update  agentactivity.Update
		state   agentactivity.State
		pending bool
		want    bool
	}{
		{name: "agent change to a Blocked agent rings the old bell", update: agentactivity.Update{AgentChanged: true}, state: agentactivity.Blocked, pending: true, want: true},
		{name: "agent change to a Working agent rings the old bell", update: agentactivity.Update{AgentChanged: true}, state: agentactivity.Working, pending: true, want: true},
		{name: "agent change during startup grace rings the old bell", update: agentactivity.Update{AgentChanged: true, Deadline: now.Add(time.Second)}, state: agentactivity.Unknown, pending: true, want: true},
		{name: "agent change without a bell rings nothing", update: agentactivity.Update{AgentChanged: true}, state: agentactivity.Blocked},
		{name: "same agent's Blocked prompt owns the bell", state: agentactivity.Blocked, pending: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &agentPaneRecord{pane: &pane{}}
			rec.pane.pendingBell.Store(tt.pending)
			require.Equal(t, tt.want, rec.resolvePendingBell(tt.update, tt.state, now))
			require.False(t, rec.pane.pendingBell.Load(), "the bell is resolved")
		})
	}
}

func TestAgentActivityAgentChangeRingsOldBellOnce(t *testing.T) {
	r := newAgentTestRig(t, 2)
	selectTestAttachmentTab(r.sess, 0)
	r.startClaudeTurn()
	r.bell(1)
	require.True(t, r.pane(1).pendingBell.Load())

	// The foreground job becomes codex, waiting at an approval prompt.
	r.runAgent(1, 779, "codex")
	codexBlockedRows := []string{
		"  Would you like to run the following command?",
		"",
		"  $ go test ./cmd/...",
		"",
		"› 1. Yes, proceed (y)",
		"  2. Yes, and don't ask again for this command in this session (a)",
		"  3. No, and tell Codex what to do differently (esc)",
		"",
		"  Press enter to confirm or esc to cancel",
	}
	r.output(1, codexBlockedRows...)
	r.settle()

	require.Equal(t, agentactivity.IndicatorBlocked, r.sess.tabs[1].agentIndicator())
	require.False(t, r.pane(1).pendingBell.Load(), "the old agent's bell is resolved")
	require.True(t, r.tabAttention(1), "one attention event covers the bell and the prompt")
}
