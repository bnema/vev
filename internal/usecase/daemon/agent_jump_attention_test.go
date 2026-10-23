package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/agentactivity"
)

// newJumpTestSession registers another local session of d with tabs tabs.
func newJumpTestSession(t *testing.T, d *Daemon, id domain.SessionID, tabs int) *session {
	t.Helper()
	sctx, cancel := context.WithCancel(d.serveCtx)
	t.Cleanup(cancel)
	sess := &session{sessionCore: sessionCore{id: id, name: string(id), incarnation: newTestLifecycle(t)}, ctx: sctx, cancel: cancel}
	for range tabs {
		sess.tabs = append(sess.tabs, newTestTabWithContext(newAgentTestPTY(t).mock, sctx, cancel))
	}
	d.mu.Lock()
	d.sessions[sess.id] = sess
	d.mu.Unlock()
	return sess
}

// seedDone publishes an unseen completion on tb. Each call finishes later
// than the previous one.
func seedDone(r *agentTestRig, sess *session, tb *tab) {
	seedAgentStatus(r.d, sess, tb, &pane{}, agentPaneStatus{state: agentactivity.Idle, done: true})
}

func setBell(sess *session, tb *tab, at time.Time) {
	sess.mu.Lock()
	tb.attention = true
	tb.attentionAt = at
	sess.mu.Unlock()
}

func TestJumpAttentionReachesDoneAgentTabs(t *testing.T) {
	tests := []struct {
		name string
		// setup seeds targets; current has 3 tabs and the client shows tab 0.
		// others are registered as sessions "b" and "c" with 2 tabs each.
		setup       func(r *agentTestRig, b, c *session)
		wantSession string // "", "b", or "c"; "" is the current session
		wantTab     int
	}{
		{
			name: "bell beats Done",
			setup: func(r *agentTestRig, b, c *session) {
				seedDone(r, r.sess, r.sess.tabs[1])
				setBell(r.sess, r.sess.tabs[2], time.Unix(50, 0))
			},
			wantTab: 2,
		},
		{
			name: "bell in another session beats local Done",
			setup: func(r *agentTestRig, b, c *session) {
				seedDone(r, r.sess, r.sess.tabs[1])
				setBell(b, b.tabs[1], time.Unix(50, 0))
			},
			wantSession: "b",
			wantTab:     1,
		},
		{
			name: "Done in current session before other session",
			setup: func(r *agentTestRig, b, c *session) {
				seedDone(r, b, b.tabs[0])
				seedDone(r, r.sess, r.sess.tabs[2])
			},
			wantTab: 2,
		},
		{
			name: "oldest Done first in current session",
			setup: func(r *agentTestRig, b, c *session) {
				seedDone(r, r.sess, r.sess.tabs[2])
				seedDone(r, r.sess, r.sess.tabs[1])
			},
			wantTab: 2,
		},
		{
			name: "oldest Done first across sessions",
			setup: func(r *agentTestRig, b, c *session) {
				seedDone(r, c, c.tabs[1])
				seedDone(r, b, b.tabs[0])
			},
			wantSession: "c",
			wantTab:     1,
		},
		{
			name: "currently viewed tab skipped",
			setup: func(r *agentTestRig, b, c *session) {
				seedDone(r, r.sess, r.sess.tabs[0])
				seedDone(r, r.sess, r.sess.tabs[1])
			},
			wantTab: 1,
		},
		{
			name: "Done beside a working agent shows Done and is a target",
			setup: func(r *agentTestRig, b, c *session) {
				tb := r.sess.tabs[1]
				seedDone(r, r.sess, tb)
				seedAgentStatus(r.d, r.sess, tb, &pane{}, agentPaneStatus{state: agentactivity.Working})
			},
			wantTab: 1,
		},
		{
			name: "Done hidden by a blocked agent is not a Done target",
			setup: func(r *agentTestRig, b, c *session) {
				tb := r.sess.tabs[1]
				seedDone(r, r.sess, tb)
				seedAgentStatus(r.d, r.sess, tb, &pane{}, agentPaneStatus{state: agentactivity.Blocked})
				// Acknowledge the Blocked bell: only the Done could target it.
				r.sess.mu.Lock()
				tb.attention = false
				r.sess.mu.Unlock()
			},
			wantTab: 0,
		},
		{
			name:    "no target is a no-op",
			setup:   func(*agentTestRig, *session, *session) {},
			wantTab: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 3)
			// A cross-session switch refreshes the bar, which reads the cwd.
			r.proc.EXPECT().Cwd(mock.Anything).Return("", errors.New("no cwd")).Maybe()
			b := newJumpTestSession(t, r.d, "b", 2)
			c := newJumpTestSession(t, r.d, "c", 2)
			selectTestAttachmentTab(r.sess, 0)
			tt.setup(r, b, c)

			require.NoError(t, r.d.jumpAttention(r.sess, r.ac))

			want := map[string]*session{"": r.sess, "b": b, "c": c}[tt.wantSession]
			require.Same(t, want, r.ac.currentSession())
			require.Equal(t, tt.wantTab, testAttachmentTabIndex(want))
		})
	}
}

// TestJumpAttentionIgnoresDoneWhenActivityOff covers the window between a
// reload that turns activity off and the scheduler clearing published Done.
func TestJumpAttentionIgnoresDoneWhenActivityOff(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(r *agentTestRig, b *session)
		wantSession string
		wantTab     int
	}{
		{name: "stale Done in the current session", setup: func(r *agentTestRig, _ *session) { seedDone(r, r.sess, r.sess.tabs[1]) }},
		{name: "stale Done in another session", setup: func(r *agentTestRig, b *session) { seedDone(r, b, b.tabs[1]) }},
		{
			name: "a bell is still a target",
			setup: func(r *agentTestRig, b *session) {
				seedDone(r, r.sess, r.sess.tabs[1])
				setBell(b, b.tabs[1], time.Unix(50, 0))
			},
			wantSession: "b",
			wantTab:     1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAgentTestRig(t, 2)
			r.proc.EXPECT().Cwd(mock.Anything).Return("", errors.New("no cwd")).Maybe()
			b := newJumpTestSession(t, r.d, "b", 2)
			selectTestAttachmentTab(r.sess, 0)
			tt.setup(r, b)
			// The reload is applied, but the scheduler has not run yet.
			cfg := domain.Defaults()
			cfg.Agents.Activity = false
			r.d.ApplyConfig(cfg)

			require.NoError(t, r.d.jumpAttention(r.sess, r.ac))

			want := map[string]*session{"": r.sess, "b": b}[tt.wantSession]
			require.Same(t, want, r.ac.currentSession())
			require.Equal(t, tt.wantTab, testAttachmentTabIndex(want))
		})
	}
}

func TestJumpAttentionBlockedAgentBeatsDone(t *testing.T) {
	r := newAgentTestRig(t, 3)
	selectTestAttachmentTab(r.sess, 0)
	seedDone(r, r.sess, r.sess.tabs[2])
	r.runAgent(1, 777, "claude")
	r.output(1, claudeBlockedRows...)
	r.settle()
	require.True(t, r.tabAttention(1), "a Blocked agent raises attention")

	require.NoError(t, r.d.jumpAttention(r.sess, r.ac))

	require.Equal(t, 1, testAttachmentTabIndex(r.sess))
}

// TestJumpAttentionRepeatedSkipsUnackedDone covers a second jump issued
// before the scheduler processed the paint ack of the first target.
func TestJumpAttentionRepeatedSkipsUnackedDone(t *testing.T) {
	r := newAgentTestRig(t, 3)
	selectTestAttachmentTab(r.sess, 0)
	seedDone(r, r.sess, r.sess.tabs[1])
	seedDone(r, r.sess, r.sess.tabs[2])

	require.NoError(t, r.d.jumpAttention(r.sess, r.ac))
	require.Equal(t, 1, testAttachmentTabIndex(r.sess))
	require.Equal(t, agentactivity.IndicatorDone, r.sess.tabs[1].agentIndicator(), "the ack is not processed yet")

	require.NoError(t, r.d.jumpAttention(r.sess, r.ac))
	require.Equal(t, 2, testAttachmentTabIndex(r.sess), "the viewed Done tab is skipped")

	// The tab left behind keeps its check until a paint acknowledges it.
	require.NoError(t, r.d.jumpAttention(r.sess, r.ac))
	require.Equal(t, 1, testAttachmentTabIndex(r.sess))
}

func TestAggregateAgentPanesDoneSeqIsOldestCompletion(t *testing.T) {
	tests := []struct {
		name    string
		panes   []agentPaneStatus
		wantSeq uint64
		done    bool
	}{
		{name: "no completion", panes: []agentPaneStatus{{state: agentactivity.Idle}}},
		{name: "one", panes: []agentPaneStatus{{state: agentactivity.Idle, done: true, doneGen: 7}}, wantSeq: 7, done: true},
		{name: "oldest of two", panes: []agentPaneStatus{
			{state: agentactivity.Idle, done: true, doneGen: 9},
			{state: agentactivity.Idle, done: true, doneGen: 4},
			{state: agentactivity.Idle},
		}, wantSeq: 4, done: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			panes := make(map[*pane]agentPaneStatus)
			for _, status := range tt.panes {
				panes[&pane{}] = status
			}
			got := aggregateAgentPanes(panes)
			require.Equal(t, tt.done, got.done)
			require.Equal(t, tt.wantSeq, got.doneSeq)
		})
	}
}
