package client

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

func TestTransitionMessageDescribesSwitchAndStoppedRestore(t *testing.T) {
	live := protocol.AttachTarget{Endpoint: "remote", Session: "work"}
	stoppedTarget := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: domain.SessionLifecycleID{1},
		SessionName: "work", Stopped: true, StoppedTab: domain.NewStableTabSelector("tab-1"),
	}
	stopped := protocol.AttachTarget{Endpoint: "remote", Session: "work", RemoteTarget: &stoppedTarget}

	require.Equal(t, "Switching to work@remote…", transitionMessage(live))
	require.Equal(t, "Starting work@remote…", transitionMessage(stopped))
}

func TestDrawTransitionToastUsesAnimatedFrames(t *testing.T) {
	var first, second bytes.Buffer
	_, err := drawTransitionToast(&first, domain.Size{Cols: 80, Rows: 24}, 0, "Switching to work@remote…")
	require.NoError(t, err)
	_, err = drawTransitionToast(&second, domain.Size{Cols: 80, Rows: 24}, 1, "Switching to work@remote…")
	require.NoError(t, err)

	require.Contains(t, first.String(), string(transitionSpinnerFrames[0]))
	require.Contains(t, second.String(), string(transitionSpinnerFrames[1]))
	require.Contains(t, first.String(), "Switching to work@remote…")
	require.NotEqual(t, first.String(), second.String())
}

type transitionTestTimer struct {
	ch      chan time.Time
	resets  []time.Duration
	stopped bool
}

func (t *transitionTestTimer) C() <-chan time.Time { return t.ch }
func (t *transitionTestTimer) Reset(delay time.Duration) bool {
	t.resets = append(t.resets, delay)
	return true
}
func (t *transitionTestTimer) Stop() bool {
	t.stopped = true
	return true
}

type transitionTestClock struct {
	timer  *transitionTestTimer
	delays []time.Duration
}

func (c *transitionTestClock) Now() time.Time { return time.Time{} }
func (c *transitionTestClock) NewTimer(delay time.Duration) ports.Timer {
	c.delays = append(c.delays, delay)
	return c.timer
}

type transitionTestTerminal struct {
	out bytes.Buffer
}

func (*transitionTestTerminal) EnterRaw() (func() error, error) {
	return func() error { return nil }, nil
}
func (*transitionTestTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil
}
func (*transitionTestTerminal) ResizeEvents() <-chan domain.Geometry { return nil }
func (*transitionTestTerminal) In() io.Reader                        { return strings.NewReader("") }
func (t *transitionTestTerminal) Out() io.Writer                     { return &t.out }
func (*transitionTestTerminal) Flush() error                         { return nil }

func TestTransitionUIStartsAfterDelayTicksResizesAndStops(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  bool
	}{
		{name: "raw", raw: true},
		{name: "not raw", raw: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			timer := &transitionTestTimer{ch: make(chan time.Time, 1)}
			clock := &transitionTestClock{timer: timer}
			term := &transitionTestTerminal{}
			raw := tt.raw
			ui := newTransitionUI(term, clock, &raw)
			ui.start(protocol.AttachTarget{Endpoint: "remote", Session: "work"})

			require.Equal(t, []time.Duration{transitionSpinnerDelay}, clock.delays)
			require.NotNil(t, ui.tickC())
			require.Empty(t, term.out.String(), "the delay prevents flashes on fast switches")
			require.NoError(t, ui.advance())
			require.Equal(t, []time.Duration{transitionSpinnerInterval}, timer.resets)
			if !tt.raw {
				require.Empty(t, term.out.String())
				require.False(t, ui.showing)
				ui.stop()
				require.True(t, timer.stopped)
				require.Nil(t, ui.tickC())
				return
			}

			require.Contains(t, term.out.String(), "Switching to work@remote…")
			require.True(t, ui.showing)
			beforeResize := term.out.Len()
			require.NoError(t, ui.redraw(domain.Size{Cols: 100, Rows: 30}))
			require.Greater(t, term.out.Len(), beforeResize)
			require.True(t, strings.Contains(term.out.String(), "Switching to work@remote…"))

			ui.stop()
			require.True(t, timer.stopped)
			require.Nil(t, ui.tickC())
		})
	}
}
