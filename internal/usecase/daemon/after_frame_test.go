package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAfterFrameQueueReleasesOnlyAfterALaterCapture(t *testing.T) {
	tests := []struct {
		name    string
		steps   []string // "add:<id>", "capture", "emit:<n>"
		wantRan []string
	}{
		{name: "nothing emitted", steps: []string{"add:a", "capture"}},
		{name: "capture started before registration", steps: []string{"capture", "add:a", "capture", "emit:1"}},
		{name: "capture started after registration", steps: []string{"capture", "add:a", "capture", "emit:2"}, wantRan: []string{"a"}},
		{name: "unnumbered emission", steps: []string{"add:a", "capture", "emit:0"}},
		{name: "partial release keeps later work", steps: []string{"add:a", "capture", "add:b", "capture", "emit:1"}, wantRan: []string{"a"}},
		{name: "later emission releases the rest", steps: []string{"add:a", "capture", "add:b", "capture", "emit:1", "emit:2"}, wantRan: []string{"a", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var q afterFrameQueue
			ran := make(chan string, len(tt.steps))
			for _, step := range tt.steps {
				switch {
				case step == "capture":
					q.beginCapture()
				case step[:4] == "add:":
					id := step[4:]
					q.add(func() { ran <- id })
				case step[:5] == "emit:":
					q.emitted(uint64(step[5] - '0'))
				}
			}
			q.runReady()
			q.runReady()
			var got []string
			for range tt.wantRan {
				got = append(got, awaitTestValue(t, ran, "released work did not run"))
			}
			require.ElementsMatch(t, tt.wantRan, got)
			select {
			case extra := <-ran:
				t.Fatalf("unexpected work %q ran", extra)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

// Work registered through afterNextFrame runs once a later capture is emitted
// on the same attachment, and is dropped once the attachment reconnected.
func TestAfterNextFrameRunsOrDropsByCapability(t *testing.T) {
	tests := []struct {
		name      string
		reconnect bool
		freeze    bool
		wantRun   bool
	}{
		{name: "same attachment runs", wantRun: true},
		{name: "reconnected attachment drops", reconnect: true},
		{name: "transient freeze delays until thaw", freeze: true, wantRun: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, source, ac, _, _, releases, _ := setupMovePickerSessionsCore(t, stubClock{}, 0)
			defer releaseAll(releases)
			effect, admitted := ac.beginAttachmentEffect(captureAttachmentCapability(source, ac, ac.transport()))
			require.True(t, admitted)
			ran := make(chan struct{}, 1)
			d.queueAfterNextFrame(ac, effect, func(*attachmentEffect) { ran <- struct{}{} })
			effect.End()
			if tt.reconnect {
				ac.replaceTransport(ac.transport())
				ac.installTestAttachmentCapability(captureAttachmentCapability(source, ac, ac.transport()))
			}
			var frozen attachmentTransitionGuard
			if tt.freeze {
				frozen = freezeAttachmentEffectGates(ac)
				require.True(t, frozen.acquired)
			}
			ac.afterFrame.emitted(ac.afterFrame.beginCapture())
			ac.afterFrame.runReady()
			if tt.freeze {
				// Released while frozen: it waits, with no further paint needed.
				select {
				case <-ran:
					t.Fatal("work ran while the attachment was frozen")
				case <-time.After(50 * time.Millisecond):
				}
				frozen.unfreeze()
			}
			if tt.wantRun {
				awaitTestValue(t, ran, "queued work did not run after the frame")
				return
			}
			select {
			case <-ran:
				t.Fatal("work ran for a reconnected attachment")
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}
