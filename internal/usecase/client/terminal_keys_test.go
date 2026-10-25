package client

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func TestTerminalKeysNormalizeOnlyOnceEnabled(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		focus    domain.TerminalFocus
		in       string
		want     string
		wantHeld bool
	}{
		{name: "disabled passes through", in: "\x1b[57442;5u", want: "\x1b[57442;5u"},
		{name: "enabled drops ctrl press", enabled: true, in: "\x1b[57442;5u", want: "", wantHeld: true},
		{name: "enabled rewrites text", enabled: true, in: "\x1b[97;;97u", want: "a"},
		{name: "lost focus forgets ctrl", enabled: true, focus: domain.TerminalFocusUnfocused, in: "\x1b[57442;5u", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				keys := newTerminalKeys()
				if tt.enabled {
					keys.enable()
				}
				require.Equal(t, tt.want, string(keys.normalize([]byte(tt.in), tt.focus)))
				held, _ := keys.ctrl.Watch()
				require.Equal(t, tt.wantHeld, held)
			})
		})
	}
}

func TestCtrlReporterReportsHoldsAfterDelayAndReleasesAtOnce(t *testing.T) {
	type step struct {
		held bool // set the state, then handle the change
		fire bool // instead, fire the hold timer
		want *protocol.ModifierState
	}
	held, released := &protocol.ModifierState{Ctrl: true}, &protocol.ModifierState{}
	tests := []struct {
		name  string
		steps []step
	}{
		{name: "hold then release", steps: []step{{held: true}, {fire: true, want: held}, {held: false, want: released}}},
		{name: "quick tap sends nothing", steps: []step{{held: true}, {held: false}, {fire: true}}},
		{name: "hold reported once", steps: []step{{held: true}, {fire: true, want: held}, {fire: true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				state := newTerminalCtrlState()
				clock := newSupervisorTestClock(t)
				r := newCtrlReporter(state, clock.port)
				var timer *supervisorTestTimer
				for i, s := range tt.steps {
					var message protocol.ModifierState
					var ok bool
					if s.fire {
						if r.timerC() == nil {
							require.Nil(t, s.want, "step %d: no timer to fire", i)
							continue
						}
						timer.ch <- time.Time{}
						<-r.timerC()
						message, ok = r.due()
					} else {
						state.set(s.held)
						<-r.changed
						message, ok = r.next()
						if s.held {
							timer = clock.awaitTimer(t)
							require.Equal(t, ctrlHoldDelay, timer.delay)
						}
					}
					require.Equal(t, s.want != nil, ok, "step %d", i)
					if s.want != nil {
						require.Equal(t, *s.want, message, "step %d", i)
					}
				}
				r.stop()
				require.True(t, clock.allStopped())
			})
		})
	}
}

// TestAttachmentReportsHeldCtrl drives a real pump: once kitty key events are
// enabled, a held Ctrl reaches the daemon as ModifierState and its release
// follows, while the dropped modifier events never reach the session as input.
func TestAttachmentReportsHeldCtrl(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		defer pw.Close()
		pump := newTerminalInputPump(pr)
		pump.keys.enable()
		pump.start()
		defer pump.stop()
		stream := newSessionStream(t)
		host := newWorkerTestHost(t, newWorkerTestTerminal(), pump, nil)
		cfg := sessionTestWorkerConfig(sessionTestRequest(true))
		clock := newSupervisorTestClock(t)
		cfg.Clock = clock.port
		worker, err := newSessionAttachmentWorker(cfg)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _, _ = host.Run(ctx, AttachmentToken{Generation: 4, Attempt: 1}, worker, stream.conn) }()

		awaitHello(t, stream)
		stream.deliver(protocol.Welcome{SessionName: "alpha"})
		stream.deliver(sessionTestOutput(1, "\x1b[Hready"))

		sent := func(want protocol.ModifierState) func() bool {
			return func() bool {
				for _, message := range stream.messages() {
					if message == want {
						return true
					}
				}
				return false
			}
		}
		_, err = pw.Write([]byte("\x1b[57442;5u"))
		require.NoError(t, err)
		var timer *supervisorTestTimer
		synctest.Wait()
		require.True(t, func() bool {
			select {
			case created := <-clock.created:
				if created.delay == ctrlHoldDelay {
					timer = created
				}
			default:
			}
			return timer != nil
		}(), "a Ctrl press arms the hold delay")
		timer.ch <- time.Time{}
		synctest.Wait()
		require.True(t, sent(protocol.ModifierState{Ctrl: true})())

		_, err = pw.Write([]byte("\x1b[57442;1:3u"))
		require.NoError(t, err)
		synctest.Wait()
		require.True(t, sent(protocol.ModifierState{})())
		for _, message := range stream.messages() {
			_, isInput := message.(protocol.Input)
			require.False(t, isInput, "modifier events are never session input")
		}
	})
}
