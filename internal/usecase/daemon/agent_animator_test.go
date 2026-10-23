package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/usecase/agentactivity"
)

// openClientPickerForTest marks ac's picker as client-owned and open, as an
// accepted offer does, so refreshClientPickers republishes it.
func openClientPickerForTest(ac *attachedClient) {
	ac.overlays.pickerMu.Lock()
	ac.overlays.pickerOpen = true
	ac.overlays.pickerInteraction = 1
	ac.overlays.pickerRevisions = make(map[string]uint64)
	ac.overlays.pickerMu.Unlock()
}

// awaitTickRefresh fires timer and reports whether that tick refreshed a
// picker before its repaint invalidation.
func awaitTickRefresh(t *testing.T, timer *manualAttentionTimer, invs chan renderInvalidation) bool {
	t.Helper()
	timer.fire()
	refreshed := false
	for {
		inv := awaitInvalidation(t, invs)
		if inv.producer == "picker.go:refresh" {
			refreshed = true
			continue
		}
		if inv.producer == "attention.go" {
			return refreshed
		}
	}
}

func TestAttentionAnimatorRefreshesPickersOnTickClearingLastBell(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()
	clk := newManualAttentionClock()
	d.clock = clk
	selectTestAttachmentTab(sess, 0)
	invs := installPaletteInvalidationObserver(sess)
	openClientPickerForTest(ac)
	// A Working agent keeps the animator ticking once the bell clears.
	tb := sess.tabs[1]
	seedAgentStatus(d, sess, tb, tb.focusedPane(), agentPaneStatus{state: agentactivity.Working})
	sess.mu.Lock()
	tb.attention = true
	tb.attentionAt = time.Unix(1, 0)
	sess.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.attentionAnimator(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	timer := clk.nextTimer(t)
	for len(invs) > 0 {
		<-invs
	}
	require.True(t, awaitTickRefresh(t, timer, invs), "a pending bell refreshes the picker")

	// Another client acknowledges the bell between ticks.
	sess.mu.Lock()
	tb.attention = false
	tb.attentionAt = time.Time{}
	sess.mu.Unlock()
	require.True(t, awaitTickRefresh(t, timer, invs), "the tick that finds the bell cleared refreshes the picker")
	require.False(t, awaitTickRefresh(t, timer, invs), "later ticks without a bell do not")
}

func TestAttentionAnimatorRefreshesPickersOnlyForBells(t *testing.T) {
	tests := []struct {
		name        string
		seed        func(d *Daemon, sess *session)
		wantRefresh bool
	}{
		{
			name: "agent spinner only",
			seed: func(d *Daemon, sess *session) {
				tb := sess.tabs[1]
				seedAgentStatus(d, sess, tb, tb.focusedPane(), agentPaneStatus{state: agentactivity.Working})
			},
		},
		{
			name: "pending bell",
			seed: func(_ *Daemon, sess *session) {
				sess.mu.Lock()
				sess.tabs[1].attention = true
				sess.tabs[1].attentionAt = time.Unix(1, 0)
				sess.mu.Unlock()
			},
			wantRefresh: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 2)
			defer releases[0]()
			defer releases[1]()
			clk := newManualAttentionClock()
			d.clock = clk
			selectTestAttachmentTab(sess, 0)
			invs := installPaletteInvalidationObserver(sess)
			openClientPickerForTest(ac)
			tt.seed(d, sess)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				d.attentionAnimator(ctx)
				close(done)
			}()
			defer func() {
				cancel()
				<-done
			}()

			timer := clk.nextTimer(t)
			// The seed may change a session label, which refreshes pickers
			// once before the timer is armed: only ticks are counted.
			for len(invs) > 0 {
				<-invs
			}
			refreshed := false
			for range 3 {
				// A picker refresh runs before the tick's repaint, so it is
				// observed before the repaint invalidation of the same tick.
				refreshed = awaitTickRefresh(t, timer, invs) || refreshed
			}
			require.Equal(t, tt.wantRefresh, refreshed)
		})
	}
}
